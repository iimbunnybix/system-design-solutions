"""Deleted data comes back when tombstones are purged before every replica saw them.

In an eventually consistent key-value store a delete is written as a
tombstone ("this key was deleted at version v") so that anti-entropy repair
can spread the delete to replicas that missed it. The original store purged
tombstones after one hour and let a replica that had been offline for days
rejoin as if nothing happened. Repair then found the old value on that
replica, no record of the delete anywhere else, and copied the old value
back to every replica: the deleted messages rose from the dead ("zombies").

The fix keeps tombstones for ``gc_grace`` (10 days, longer than any allowed
downtime and the repair interval), refuses to let a replica that was offline
longer than ``gc_grace`` rejoin without a full resync, and merges by version
so a tombstone beats every older write.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

HOUR_S = 3_600
DAY_S = 24 * HOUR_S
ORIGINAL_GC_GRACE_S = HOUR_S      # lowered to cut tombstone scan overhead
SAFE_GC_GRACE_S = 10 * DAY_S      # > longest allowed downtime and the repair interval
GC_INTERVAL_S = HOUR_S            # how often compaction purges old tombstones


class FakeClock:
    """Deterministic time in seconds."""

    def __init__(self) -> None:
        self.now_s = 0

    def advance(self, seconds: int) -> None:
        """Move time forward by ``seconds``."""
        self.now_s += seconds


@dataclass(frozen=True)
class Cell:
    """One stored version of a key. ``value is None`` means a tombstone."""

    value: Optional[str]
    version: int                  # write order; real stores use timestamps/HLC (problem 022)
    written_at_s: int

    @property
    def is_tombstone(self) -> bool:
        return self.value is None


Merge = Callable[[Optional[Cell], Optional[Cell]], Optional[Cell]]


def naive_merge(local: Optional[Cell], incoming: Optional[Cell]) -> Optional[Cell]:
    """Original repair rule: "fill in replicas that have no data".

    A live value beats a tombstone or a missing key, whatever its version,
    because the code treated a tombstone like "this replica has nothing".
    """
    if local is None or incoming is None:
        return local or incoming
    if local.is_tombstone != incoming.is_tombstone:
        return incoming if local.is_tombstone else local
    return incoming if incoming.version > local.version else local


def newest_wins(local: Optional[Cell], incoming: Optional[Cell]) -> Optional[Cell]:
    """Fixed repair rule: the higher version wins, tombstone or not.

    On a tie the tombstone wins, so a delete is never undone by the very
    write it deleted.
    """
    if local is None or incoming is None:
        return local or incoming
    if incoming.version != local.version:
        return incoming if incoming.version > local.version else local
    return incoming if incoming.is_tombstone else local


class RejoinRefused(Exception):
    """The replica was away longer than gc_grace and must be fully resynced."""


class Replica:
    """One node's copy of the data."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.cells: dict[str, Cell] = {}
        self.online = True
        self.offline_since_s = 0


class ReplicatedStore:
    """N replicas, writes to every online replica, repair by merging all copies."""

    def __init__(self, clock: FakeClock, gc_grace_s: int, merge: Merge,
                 check_rejoin: bool, replica_names: tuple[str, ...] = ("a", "b", "c")) -> None:
        self.clock = clock
        self.gc_grace_s = gc_grace_s
        self.merge = merge
        self.check_rejoin = check_rejoin
        self.replicas = {name: Replica(name) for name in replica_names}
        self._last_version = 0

    # ------------------------------------------------------------ writes
    def put(self, key: str, value: str) -> None:
        """Write ``value`` to every online replica (no hinted handoff)."""
        self._write(key, value)

    def delete(self, key: str) -> None:
        """Write a tombstone; offline replicas keep their old value until repaired."""
        self._write(key, None)

    def _write(self, key: str, value: Optional[str]) -> None:
        self._last_version += 1
        cell = Cell(value, self._last_version, self.clock.now_s)
        for replica in self._online():
            replica.cells[key] = cell

    # ------------------------------------------------------------- reads
    def get(self, key: str) -> Optional[str]:
        """Read from all online replicas and read-repair them to the merged cell."""
        winner = self._repair_key(key)
        if winner is None or winner.is_tombstone:
            return None
        return winner.value

    # ---------------------------------------------------- anti-entropy
    def repair(self) -> None:
        """Anti-entropy: make every online replica hold the merged cell for every key."""
        keys: set[str] = set()
        for replica in self._online():
            keys |= replica.cells.keys()
        for key in keys:
            self._repair_key(key)

    def _repair_key(self, key: str) -> Optional[Cell]:
        winner: Optional[Cell] = None
        for replica in self._online():
            winner = self.merge(winner, replica.cells.get(key))
        if winner is not None:
            for replica in self._online():
                replica.cells[key] = winner
        return winner

    def purge_tombstones(self) -> None:
        """Compaction: drop tombstones older than gc_grace on online replicas."""
        cutoff = self.clock.now_s - self.gc_grace_s
        for replica in self._online():
            expired = [k for k, c in replica.cells.items()
                       if c.is_tombstone and c.written_at_s < cutoff]
            for key in expired:
                del replica.cells[key]

    # ------------------------------------------------------ membership
    def take_offline(self, name: str) -> None:
        self.replicas[name].online = False
        self.replicas[name].offline_since_s = self.clock.now_s

    def rejoin(self, name: str) -> None:
        """Bring a replica back with its old data, if it was not away too long.

        A tombstone written after the replica left can be purged gc_grace
        later, so after a longer absence the replica may hold values whose
        deletes no longer exist anywhere. Merging them would resurrect them.
        """
        replica = self.replicas[name]
        downtime = self.clock.now_s - replica.offline_since_s
        if self.check_rejoin and downtime > self.gc_grace_s:
            raise RejoinRefused(f"{name} was offline {downtime} s > gc_grace {self.gc_grace_s} s")
        replica.online = True

    def full_resync(self, name: str) -> None:
        """Throw away the replica's data and copy the merged view of its peers."""
        replica = self.replicas[name]
        replica.cells = {}
        self.repair()                          # peers agree with each other first
        for peer in self._online():
            replica.cells = dict(peer.cells)
            break
        replica.online = True

    def _online(self) -> list[Replica]:
        return [r for r in self.replicas.values() if r.online]


def original_store(clock: FakeClock) -> ReplicatedStore:
    """1-hour tombstones, "live data fills holes" repair, no rejoin check."""
    return ReplicatedStore(clock, ORIGINAL_GC_GRACE_S, naive_merge, check_rejoin=False)


def safe_store(clock: FakeClock) -> ReplicatedStore:
    """10-day tombstones, newest-version repair, rejoin refused after gc_grace."""
    return ReplicatedStore(clock, SAFE_GC_GRACE_S, newest_wins, check_rejoin=True)


def run_until(store: ReplicatedStore, clock: FakeClock, end_s: int) -> None:
    """Advance time to ``end_s``, running compaction every GC_INTERVAL_S."""
    while clock.now_s + GC_INTERVAL_S <= end_s:
        clock.advance(GC_INTERVAL_S)
        store.purge_tombstones()
    clock.advance(end_s - clock.now_s)


def simulate(make_store: Callable[[FakeClock], ReplicatedStore], offline_days: int,
             messages: int = 1_000, deleted: int = 200) -> int:
    """Replica c goes offline, users delete messages, c comes back, repair runs.

    Returns how many deleted messages are readable again afterwards.
    """
    clock = FakeClock()
    store = make_store(clock)
    for i in range(messages):
        store.put(f"msg-{i}", f"body-{i}")
    store.take_offline("c")
    clock.advance(HOUR_S)
    for i in range(deleted):
        store.delete(f"msg-{i}")
    run_until(store, clock, offline_days * DAY_S)
    try:
        store.rejoin("c")
    except RejoinRefused:
        store.full_resync("c")
    store.repair()
    return sum(1 for i in range(deleted) if store.get(f"msg-{i}") is not None)
