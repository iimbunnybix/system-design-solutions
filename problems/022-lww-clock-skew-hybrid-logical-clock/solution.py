"""Stop last-write-wins replication from dropping newer writes on skewed clocks.

Each replica stamped writes with its own wall clock and kept the value with
the highest stamp. A node whose clock ran a few seconds behind produced stamps
that lost to *older* writes from other nodes, so a user's edit was silently
discarded. This module keeps that wall-clock version and adds a hybrid logical
clock (HLC, Kulkarni et al. 2014): stamps stay close to wall time but never go
below any stamp the node has already seen, so a write made after seeing
another write always sorts after it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

MAX_CLOCK_DRIFT_MS = 60_000   # refuse remote stamps this far ahead of our clock

WallClock = Callable[[], int]  # returns milliseconds since the epoch


class ClockDriftError(Exception):
    """A remote stamp is implausibly far in the future (its clock is broken)."""


@dataclass(frozen=True, order=True)
class Timestamp:
    """Ordered by wall time, then logical counter, then node id (tie-break)."""
    wall_ms: int
    logical: int
    node_id: str


@dataclass
class Write:
    key: str
    value: str
    stamp: Timestamp


@dataclass
class LwwStore:
    """Per-key register that keeps the write with the highest stamp."""
    entries: dict[str, Write] = field(default_factory=dict)

    def apply(self, write: Write) -> bool:
        """Store ``write`` if it is newer than what we have; return True if kept."""
        current = self.entries.get(write.key)
        if current is not None and current.stamp >= write.stamp:
            return False
        self.entries[write.key] = write
        return True

    def read(self, key: str) -> str | None:
        entry = self.entries.get(key)
        return entry.value if entry else None


class WallClockNode:
    """Original replica: stamps every write with the local wall clock."""

    def __init__(self, node_id: str, wall_clock: WallClock) -> None:
        self.node_id = node_id
        self.wall_clock = wall_clock
        self.store = LwwStore()

    def write(self, key: str, value: str) -> Write:
        write = Write(key, value, Timestamp(self.wall_clock(), 0, self.node_id))
        self.store.apply(write)
        return write

    def receive(self, write: Write) -> None:
        self.store.apply(write)


class HybridLogicalClock:
    """Wall-clock-like timestamps that never go backwards and respect causality."""

    def __init__(self, node_id: str, wall_clock: WallClock) -> None:
        self.node_id = node_id
        self.wall_clock = wall_clock
        self.last_wall_ms = 0
        self.logical = 0

    def now(self) -> Timestamp:
        """Stamp for a local event (a write originating on this node)."""
        physical_ms = self.wall_clock()
        if physical_ms > self.last_wall_ms:
            # Real time moved past everything we have seen: use it directly.
            self.last_wall_ms = physical_ms
            self.logical = 0
        else:
            # Our clock is behind (or equal to) a stamp we already issued or
            # received; stay on that wall time and count up instead.
            self.logical += 1
        return Timestamp(self.last_wall_ms, self.logical, self.node_id)

    def observe(self, remote: Timestamp) -> None:
        """Merge a stamp received from another node so later stamps exceed it."""
        physical_ms = self.wall_clock()
        if remote.wall_ms - physical_ms > MAX_CLOCK_DRIFT_MS:
            # Without this guard one node with a clock set to next year would
            # drag every node's stamps into next year permanently.
            raise ClockDriftError(f"remote stamp {remote.wall_ms} is too far ahead")
        new_wall_ms = max(self.last_wall_ms, remote.wall_ms, physical_ms)
        self.logical = self._merged_logical(new_wall_ms, remote)
        self.last_wall_ms = new_wall_ms

    def _merged_logical(self, new_wall_ms: int, remote: Timestamp) -> int:
        """Logical counter after a merge: bump whichever sources share the max."""
        local_matches = new_wall_ms == self.last_wall_ms
        remote_matches = new_wall_ms == remote.wall_ms
        if local_matches and remote_matches:
            return max(self.logical, remote.logical) + 1
        if local_matches:
            return self.logical + 1
        if remote_matches:
            return remote.logical + 1
        return 0   # physical time is ahead of both: start a fresh counter


class HlcNode:
    """Fixed replica: stamps writes with a hybrid logical clock."""

    def __init__(self, node_id: str, wall_clock: WallClock) -> None:
        self.clock = HybridLogicalClock(node_id, wall_clock)
        self.store = LwwStore()

    def write(self, key: str, value: str) -> Write:
        write = Write(key, value, self.clock.now())
        self.store.apply(write)
        return write

    def receive(self, write: Write) -> None:
        # Advance the clock first so anything written after this sorts later.
        self.clock.observe(write.stamp)
        self.store.apply(write)
