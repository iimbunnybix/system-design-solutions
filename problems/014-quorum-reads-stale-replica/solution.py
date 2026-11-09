"""Leaderless replication: quorum reads/writes with read repair.

A key is stored on ``n`` replicas. The coordinator sends each write to all
replicas and acknowledges the client after ``w`` of them have applied it; a
read asks ``r`` replicas and returns the value with the highest version.

With ``w = 1, r = 1`` (the original configuration, chosen for latency) a read
can land on a replica that has not received a write yet, or on a replica
that was down while writes happened and has just come back. If ``r + w > n``
every read set overlaps every acknowledged write set in at least one replica,
so a read always sees the latest acknowledged write. Read repair then pushes
the newest value to the stale replicas it contacted, and ``anti_entropy``
fixes keys nobody reads.

``simulate`` is a deterministic discrete-event model: replication delays,
replica outages (writes to a down replica are lost for it), and a
stream of reads and writes. A read is *stale* if it returns a version older
than the newest write acknowledged before the read started. A read *went
back* if it returns a version older than one an earlier read of the same key
already returned (the user saw the new value, then the old one again).
``r + w > n`` prevents stale reads; preventing went-back reads also needs
blocking read repair with ``r >= 2``: a write that is still in flight can be
returned by one read and missed by the next unless the reader first copies it
to enough replicas.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field

Version = int


@dataclass
class Replica:
    data: dict[str, tuple[Version, str]] = field(default_factory=dict)

    def apply(self, key: str, version: Version, value: str) -> bool:
        cur = self.data.get(key)
        if cur is None or version > cur[0]:
            self.data[key] = (version, value)
            return True
        return False

    def get(self, key: str) -> tuple[Version, str | None]:
        return self.data.get(key, (0, None))


class Outages:
    """``{replica: [(start, end), ...]}``: replica unreachable in [start, end)."""

    def __init__(self, windows: dict[int, list[tuple[int, int]]] | None = None):
        self.windows = windows or {}

    def up(self, replica: int, t: int) -> bool:
        return not any(s <= t < e for s, e in self.windows.get(replica, []))


@dataclass
class Stats:
    reads: int = 0
    stale_reads: int = 0
    unavailable_reads: int = 0
    writes: int = 0
    failed_writes: int = 0      # fewer than w replicas applied it
    repairs: int = 0
    went_back_reads: int = 0    # older than a version an earlier read returned

    @property
    def stale_rate(self) -> float:
        ok = self.reads - self.unavailable_reads
        return self.stale_reads / ok if ok else 0.0


class Cluster:
    def __init__(self, n: int, w: int, r: int, outages: Outages, seed: int = 0,
                 max_delay: int = 20, read_repair: bool = True):
        if not (1 <= w <= n and 1 <= r <= n):
            raise ValueError("need 1 <= w, r <= n")
        self.n, self.w, self.r = n, w, r
        self.replicas = [Replica() for _ in range(n)]
        self.outages = outages
        self.rng = random.Random(seed)
        self.max_delay = max_delay
        self.read_repair = read_repair
        self._pending: list[tuple[int, int, int, str, Version, str]] = []
        self._seq = 0
        self._version = 0
        self.acked: dict[str, list[tuple[int, Version]]] = {}   # key -> (ack_time, v)
        # Highest version any read has returned per key, to detect flip-flops.
        self.max_returned: dict[str, Version] = {}
        self.stats = Stats()

    @property
    def strict_quorum(self) -> bool:
        return self.r + self.w > self.n

    def advance(self, t: int) -> None:
        """Deliver replication messages due by ``t``; down replicas miss them."""
        while self._pending and self._pending[0][0] <= t:
            at, _, rid, key, version, value = heapq.heappop(self._pending)
            if self.outages.up(rid, at):
                self.replicas[rid].apply(key, version, value)

    def write(self, key: str, value: str, t: int) -> bool:
        self.advance(t)
        self._version += 1
        version = self._version
        applied_at = []
        for rid in range(self.n):
            at = t + self.rng.randint(1, self.max_delay)
            self._seq += 1
            heapq.heappush(self._pending, (at, self._seq, rid, key, version, value))
            if self.outages.up(rid, at):
                applied_at.append(at)
        self.stats.writes += 1
        if len(applied_at) < self.w:
            self.stats.failed_writes += 1      # client gets an error
            return False
        ack = sorted(applied_at)[self.w - 1]
        self.acked.setdefault(key, []).append((ack, version))
        return True

    def latest_acked(self, key: str, t: int) -> Version:
        return max((v for at, v in self.acked.get(key, []) if at < t), default=0)

    def read(self, key: str, t: int) -> str | None:
        self.advance(t)
        self.stats.reads += 1
        up = [i for i in range(self.n) if self.outages.up(i, t)]
        if len(up) < self.r:
            self.stats.unavailable_reads += 1
            return None
        chosen = self.rng.sample(up, self.r)
        answers = {i: self.replicas[i].get(key) for i in chosen}
        version, value = max(answers.values(), key=lambda a: a[0])
        if version < self.latest_acked(key, t):
            self.stats.stale_reads += 1
        # A read can return an in-flight (not yet acknowledged) write; if a
        # later read misses it, the user sees the value go back in time.
        if version < self.max_returned.get(key, 0):
            self.stats.went_back_reads += 1
        self.max_returned[key] = max(self.max_returned.get(key, 0), version)
        if self.read_repair and version:
            for i, (v, _) in answers.items():
                if v < version:
                    self.replicas[i].apply(key, version, value)
                    self.stats.repairs += 1
        return value

    def anti_entropy(self, t: int) -> int:
        """Background sync of all up replicas to the newest version per key
        (a real system compares Merkle trees instead of every key)."""
        self.advance(t)
        up = [self.replicas[i] for i in range(self.n) if self.outages.up(i, t)]
        fixed = 0
        keys = {k for rep in up for k in rep.data}
        for k in keys:
            best = max((rep.get(k) for rep in up), key=lambda a: a[0])
            for rep in up:
                fixed += rep.apply(k, best[0], best[1])
        return fixed


def simulate(n: int, w: int, r: int, outages: Outages, *, ops: int = 20_000,
             keys: int = 20, write_share: float = 0.3, seed: int = 0,
             read_repair: bool = True, max_delay: int = 20) -> Stats:
    """One operation per tick on a small, hot key space."""
    cluster = Cluster(n, w, r, outages, seed=seed, read_repair=read_repair,
                      max_delay=max_delay)
    rng = random.Random(seed + 1)
    for t in range(ops):
        key = f"k{rng.randrange(keys)}"
        if rng.random() < write_share:
            cluster.write(key, f"v{t}", t)
        else:
            cluster.read(key, t)
    return cluster.stats
