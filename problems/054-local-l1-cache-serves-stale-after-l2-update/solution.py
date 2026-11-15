"""Keep every pod's in-process (L1) cache in step with the shared store.

Each app pod caches prices in its own memory for 5 minutes in front of the
shared store. An update deleted the entry only from the L1 of the pod that
handled it, so every other pod kept serving the old price until its TTL ran
out. This module keeps that pod and adds one that subscribes to a shared
invalidation bus: every write publishes (key, version), every pod drops older
copies, and a pod that notices it missed messages (a gap in the bus sequence
numbers) clears its whole L1 instead of trusting it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

L1_TTL_SECONDS = 300.0      # backstop: no L1 entry lives longer than this


@dataclass
class VersionedValue:
    value: int
    version: int


@dataclass
class Invalidation:
    sequence: int           # position on the bus, 1, 2, 3, ... with no gaps
    key: str
    version: int            # version of the key after the write


class SharedStore:
    """The L2 cache / database every pod reads through to."""

    def __init__(self) -> None:
        self.rows: dict[str, VersionedValue] = {}

    def read(self, key: str) -> VersionedValue:
        return self.rows[key]

    def write(self, key: str, value: int) -> int:
        """Store a new value and return its version (each write bumps it)."""
        previous = self.rows.get(key)
        version = 1 if previous is None else previous.version + 1
        self.rows[key] = VersionedValue(value, version)
        return version


class InvalidationBus:
    """In-memory stand-in for Redis pub/sub, Kafka or NATS."""

    def __init__(self) -> None:
        self.subscribers: list[Callable[[Invalidation], None]] = []
        self.last_sequence = 0

    def subscribe(self, handler: Callable[[Invalidation], None]) -> None:
        self.subscribers.append(handler)

    def publish(self, key: str, version: int) -> None:
        self.last_sequence += 1
        event = Invalidation(self.last_sequence, key, version)
        for handler in self.subscribers:
            handler(event)


@dataclass
class L1Entry:
    value: int
    version: int
    expires_at: float


class NaivePod:
    """Original pod: an update evicts only this pod's own L1 entry."""

    def __init__(self, store: SharedStore, clock: Callable[[], float]) -> None:
        self.store = store
        self.clock = clock
        self.l1: dict[str, L1Entry] = {}

    def get(self, key: str) -> int:
        """Serve from L1 while fresh, otherwise read through to the store."""
        entry = self.l1.get(key)
        if entry is not None and entry.expires_at > self.clock():
            return entry.value
        row = self.store.read(key)
        self.l1[key] = L1Entry(row.value, row.version, self.clock() + L1_TTL_SECONDS)
        return row.value

    def update(self, key: str, value: int) -> None:
        self.store.write(key, value)
        # Bug: the other pods' L1 caches never hear about this write.
        self.l1.pop(key, None)


class Pod(NaivePod):
    """Fixed pod: writes are broadcast, and every pod applies them to its L1."""

    def __init__(self, store: SharedStore, clock: Callable[[], float],
                 bus: InvalidationBus) -> None:
        super().__init__(store, clock)
        self.bus = bus
        self.connected = True                   # tests flip this to simulate a dropped link
        self.last_seen_sequence = bus.last_sequence
        bus.subscribe(self.receive)

    def update(self, key: str, value: int) -> None:
        version = self.store.write(key, value)
        self.l1.pop(key, None)
        self.bus.publish(key, version)

    def receive(self, event: Invalidation) -> None:
        """Bus handler: evict stale entries, or everything if messages were lost."""
        if not self.connected:
            return                              # message lost while disconnected
        if event.sequence != self.last_seen_sequence + 1:
            # We missed at least one invalidation and cannot know which keys it
            # covered, so no L1 entry can be trusted any more.
            self.l1.clear()
        self.last_seen_sequence = event.sequence
        self._evict_if_older(event)

    def _evict_if_older(self, event: Invalidation) -> None:
        """Drop the entry unless it already holds this version or a newer one.

        A delayed event for an old write must not evict a newer cached value,
        or every late message would cause a needless store read.
        """
        entry = self.l1.get(event.key)
        if entry is not None and entry.version < event.version:
            del self.l1[event.key]
