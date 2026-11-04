"""Sharded counters for a hot row (the "viral post like count" problem).

The original bug: every like ran ``UPDATE posts SET likes = likes + 1 WHERE id = ?``.
For normal posts that is fine. When one post went viral, all writes queued for
the *same row lock*, so a single row could absorb only ``1 / lock_hold_time``
updates per second no matter how big the database was. The queue grew,
like-requests timed out, connection pools filled, and unrelated pages slowed
down with them.

Fix: split the counter into N sub-counters (rows ``(post_id, shard)``). A write
increments one randomly chosen shard, so N rows absorb the load in parallel; a
read sums the N shards. N can be small for cold keys and raised for hot ones.
"""
from __future__ import annotations

import heapq
import random
import threading
from collections import defaultdict


class ShardedCounter:
    """In-memory model of a sharded counter table with one lock per row."""

    def __init__(self, default_shards: int = 1, seed: int | None = None):
        if default_shards < 1:
            raise ValueError("need at least one shard")
        self.default_shards = default_shards
        self._shards: dict[str, int] = {}
        self._rows: dict[tuple[str, int], int] = defaultdict(int)
        self._locks: dict[tuple[str, int], threading.Lock] = defaultdict(threading.Lock)
        self._meta = threading.Lock()
        self._rng = random.Random(seed)

    def shards_for(self, key: str) -> int:
        return self._shards.get(key, self.default_shards)

    def set_shards(self, key: str, n: int) -> None:
        """Grow the shard count for a hot key. Shrinking is not allowed because rows
        above the new count would be silently ignored by reads."""
        if n < self.shards_for(key):
            raise ValueError("shard count can only grow (merge rows offline to shrink)")
        with self._meta:
            self._shards[key] = n

    def increment(self, key: str, amount: int = 1) -> None:
        with self._meta:
            shard = self._rng.randrange(self.shards_for(key))
            lock = self._locks[(key, shard)]
        with lock:                                         # row lock: only this shard
            self._rows[(key, shard)] += amount

    def value(self, key: str) -> int:
        """SELECT SUM(count) FROM post_like_shards WHERE post_id = ?"""
        with self._meta:
            n = self.shards_for(key)
        return sum(self._rows.get((key, s), 0) for s in range(n))

    def rows(self, key: str) -> list[int]:
        return [self._rows.get((key, s), 0) for s in range(self.shards_for(key))]


def simulate_row_contention(rate: float, duration: float, shards: int, service_time: float,
                            seed: int = 0) -> dict:
    """Discrete-event model of row-lock queueing.

    Writes arrive as a Poisson process at ``rate`` per second for ``duration``
    seconds; each picks a random shard; each row serves one write at a time,
    holding its lock for ``service_time``. Returns wait-time statistics.
    """
    if shards < 1 or rate <= 0 or service_time <= 0:
        raise ValueError("invalid simulation parameters")
    rng = random.Random(seed)
    free_at = [0.0] * shards
    waits = []
    t = 0.0
    while True:
        t += rng.expovariate(rate)
        if t > duration:
            break
        s = rng.randrange(shards)
        start = max(t, free_at[s])
        waits.append(start - t)
        free_at[s] = start + service_time
    waits.sort()
    n = len(waits)
    return {
        "writes": n,
        "utilization_per_row": rate * service_time / shards,
        "p50_wait": waits[n // 2],
        "p99_wait": waits[min(n - 1, int(n * 0.99))],
        "max_wait": waits[-1],
    }


def recommended_shards(peak_writes_per_s: float, service_time: float, max_utilization: float = 0.5) -> int:
    """Smallest shard count keeping each row's lock utilization at or below ``max_utilization``."""
    import math
    return max(1, math.ceil(peak_writes_per_s * service_time / max_utilization))
