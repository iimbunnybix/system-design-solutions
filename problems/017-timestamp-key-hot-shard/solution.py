"""Spread time-ordered writes across range shards with a hash-bucket key prefix.

An event store is range-partitioned on its row key. The key started with the
event timestamp, so every new event sorted after every old one and all live
writes landed on the last shard while the others sat idle. This module keeps
the naive key for comparison and adds a "salted" key: a stable hash bucket of
the event id goes first, so new writes spread over all buckets (and shards),
and time-range reads become one scan per bucket merged back into time order.
"""
from __future__ import annotations

import bisect
import hashlib
import heapq
from dataclasses import dataclass
from typing import Iterator

TIMESTAMP_DIGITS = 13       # epoch milliseconds fit in 13 digits until year 2286
BUCKET_DIGITS = 2           # enough for up to 100 buckets
MAX_BUCKET_COUNT = 10 ** BUCKET_DIGITS
KEY_SEPARATOR = "|"


@dataclass(frozen=True)
class Event:
    event_id: str
    timestamp_ms: int
    payload: str


class RangeShardedStore:
    """A key-value store split into shards by sorted key ranges.

    `split_points` are the first keys of shards 1..N-1; shard 0 holds every key
    below the first split point. This is how Bigtable/HBase-style stores route keys.
    """

    def __init__(self, split_points: list[str]) -> None:
        self.split_points = sorted(split_points)
        shard_count = len(self.split_points) + 1
        # Each shard keeps its rows sorted by key so range scans are a slice.
        self.shards: list[list[tuple[str, Event]]] = [[] for _ in range(shard_count)]
        self.writes_per_shard: list[int] = [0] * shard_count

    def shard_for(self, key: str) -> int:
        """Index of the shard whose key range contains `key`."""
        return bisect.bisect_right(self.split_points, key)

    def put(self, key: str, event: Event) -> None:
        """Store one row and count the write against its shard."""
        shard_index = self.shard_for(key)
        bisect.insort(self.shards[shard_index], (key, event))
        self.writes_per_shard[shard_index] += 1

    def scan(self, start_key: str, end_key: str) -> list[Event]:
        """Rows with start_key <= key < end_key, in key order."""
        results: list[Event] = []
        for rows in self.shards:
            # A 1-tuple (key,) sorts before every (key, event) row with that key,
            # so bisect_left finds the first row whose key is >= the bound.
            low = bisect.bisect_left(rows, (start_key,))
            high = bisect.bisect_left(rows, (end_key,))
            for _key, event in rows[low:high]:
                results.append(event)
        return results


def naive_row_key(event: Event) -> str:
    """Original key: timestamp first, so keys grow monotonically over time."""
    return f"{event.timestamp_ms:0{TIMESTAMP_DIGITS}d}{KEY_SEPARATOR}{event.event_id}"


def check_bucket_count(bucket_count: int) -> None:
    """Reject bucket counts whose prefixes would not fit in BUCKET_DIGITS.

    A 3-digit prefix such as "100|" sorts between "10|" and "11|" as a string,
    so buckets of 100 or more would land in the wrong shard ranges.
    """
    if not 1 <= bucket_count <= MAX_BUCKET_COUNT:
        raise ValueError(f"bucket_count must be between 1 and {MAX_BUCKET_COUNT}")


def bucket_for(event_id: str, bucket_count: int) -> int:
    """Stable bucket for an id. hashlib, not hash(): Python randomises str hashes per process."""
    check_bucket_count(bucket_count)
    digest = hashlib.sha256(event_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % bucket_count


def bucket_prefix(bucket: int) -> str:
    """Fixed-width prefix so buckets sort as 00|, 01|, ... 15|."""
    return f"{bucket:0{BUCKET_DIGITS}d}{KEY_SEPARATOR}"


def salted_row_key(event: Event, bucket_count: int) -> str:
    """Fixed key: bucket prefix first, then the time-ordered part inside each bucket."""
    prefix = bucket_prefix(bucket_for(event.event_id, bucket_count))
    return prefix + naive_row_key(event)


def bucket_split_points(bucket_count: int, shard_count: int) -> list[str]:
    """Shard boundaries on bucket prefixes so each shard owns an equal run of buckets."""
    check_bucket_count(bucket_count)
    if bucket_count % shard_count != 0:
        raise ValueError("bucket_count must be a multiple of shard_count")
    buckets_per_shard = bucket_count // shard_count
    points = []
    for shard_index in range(1, shard_count):
        points.append(bucket_prefix(shard_index * buckets_per_shard))
    return points


def naive_time_range(store: RangeShardedStore, start_ms: int, end_ms: int) -> list[Event]:
    """Time-range read on naive keys: one contiguous scan."""
    start_key = f"{start_ms:0{TIMESTAMP_DIGITS}d}"
    end_key = f"{end_ms:0{TIMESTAMP_DIGITS}d}"
    return store.scan(start_key, end_key)


def salted_time_range(store: RangeShardedStore, start_ms: int, end_ms: int,
                      bucket_count: int) -> list[Event]:
    """Scatter one scan per bucket, then merge the sorted streams back into time order."""
    per_bucket_results: list[list[Event]] = []
    for bucket in range(bucket_count):
        prefix = bucket_prefix(bucket)
        start_key = f"{prefix}{start_ms:0{TIMESTAMP_DIGITS}d}"
        end_key = f"{prefix}{end_ms:0{TIMESTAMP_DIGITS}d}"
        per_bucket_results.append(store.scan(start_key, end_key))
    # Each bucket is already time-sorted, so a k-way merge is enough (no full re-sort).
    merged: Iterator[Event] = heapq.merge(*per_bucket_results, key=_time_order)
    return list(merged)


def _time_order(event: Event) -> tuple[int, str]:
    """Sort by time, then id, matching the order inside one bucket."""
    return (event.timestamp_ms, event.event_id)


def hottest_shard_share(store: RangeShardedStore) -> float:
    """Fraction of all writes that went to the busiest shard (1.0 = one hot shard)."""
    total = sum(store.writes_per_shard)
    if total == 0:
        return 0.0
    return max(store.writes_per_shard) / total
