"""Count weekly active users across shards and days without double counting.

The analytics service stored one "distinct users" number per shard per day
and built the weekly-active-users (WAU) figure by adding those numbers up.
Distinct counts are not additive: a user who was active on three days, or
whose sessions landed on two shards, was counted once for every bucket.

This module keeps a mergeable HyperLogLog sketch per (shard, day) instead.
Merging sketches (register-wise maximum) gives the sketch of the union, so
any range of days and any set of shards can be counted with a fixed 16 KB of
memory per bucket and an error of about 1%.
"""
from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable

DEFAULT_PRECISION = 14          # 2**14 = 16,384 registers, ~0.81% std error
HASH_BITS = 64                  # 64-bit hashes make large-range correction unnecessary
MIN_PRECISION = 4
MAX_PRECISION = 18


def naive_weekly_active(daily_shard_counts: Iterable[int]) -> int:
    """Original WAU: add up per-(shard, day) distinct counts.

    Wrong whenever one user appears in more than one bucket.
    """
    total = 0
    for count in daily_shard_counts:
        total += count
    return total


def hash_user(user_id: str) -> int:
    """Return a stable 64-bit hash of a user id.

    Python's built-in hash() is randomized per process, so sketches built on
    different servers would not merge; blake2b is the same everywhere.
    """
    digest = hashlib.blake2b(user_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def alpha(register_count: int) -> float:
    """Bias-correction constant from the HyperLogLog paper (Flajolet et al.)."""
    if register_count == 16:
        return 0.673
    if register_count == 32:
        return 0.697
    if register_count == 64:
        return 0.709
    return 0.7213 / (1 + 1.079 / register_count)


class HyperLogLog:
    """Fixed-size sketch that estimates how many distinct ids were added."""

    def __init__(self, precision: int = DEFAULT_PRECISION) -> None:
        if not MIN_PRECISION <= precision <= MAX_PRECISION:
            raise ValueError(f"precision must be {MIN_PRECISION}..{MAX_PRECISION}")
        self.precision = precision
        self.register_count = 1 << precision
        # One byte per register: a register stores a rank of at most 65.
        self.registers = bytearray(self.register_count)

    def add(self, user_id: str) -> None:
        """Record one occurrence of user_id; repeats do not change the sketch."""
        hashed = hash_user(user_id)
        remaining_bits = HASH_BITS - self.precision
        # The top `precision` bits pick the register...
        index = hashed >> remaining_bits
        # ...and the rest gives the rank: position of the first 1-bit.
        # A long run of leading zeros is rare, so it signals many distinct ids.
        rest = hashed & ((1 << remaining_bits) - 1)
        rank = remaining_bits - rest.bit_length() + 1
        if rank > self.registers[index]:
            self.registers[index] = rank

    def merge(self, other: "HyperLogLog") -> None:
        """Fold another sketch into this one, giving the sketch of the union."""
        if other.precision != self.precision:
            raise ValueError("cannot merge sketches with different precision")
        for index in range(self.register_count):
            if other.registers[index] > self.registers[index]:
                self.registers[index] = other.registers[index]

    def estimate(self) -> int:
        """Return the estimated number of distinct ids added so far."""
        inverse_sum = 0.0
        empty_registers = 0
        for rank in self.registers:
            inverse_sum += 2.0 ** -rank
            if rank == 0:
                empty_registers += 1
        raw = alpha(self.register_count) * self.register_count ** 2 / inverse_sum
        # For small sets the raw formula is biased; counting empty registers
        # (linear counting) is far more accurate there.
        if raw <= 2.5 * self.register_count and empty_registers > 0:
            raw = self.register_count * math.log(self.register_count / empty_registers)
        return round(raw)

    def standard_error(self) -> float:
        """Typical relative error of estimate(): 1.04 / sqrt(registers)."""
        return 1.04 / math.sqrt(self.register_count)


class ActiveUserStore:
    """Keeps one sketch per (shard, day) and answers distinct counts over ranges."""

    def __init__(self, precision: int = DEFAULT_PRECISION) -> None:
        self.precision = precision
        self.sketches: dict[tuple[str, int], HyperLogLog] = {}
        # Kept only to reproduce the original report next to the fix.
        self.exact_bucket_users: dict[tuple[str, int], set[str]] = {}

    def record(self, shard: str, day: int, user_id: str) -> None:
        """Record that user_id was active on `day`, served by `shard`."""
        bucket = (shard, day)
        if bucket not in self.sketches:
            self.sketches[bucket] = HyperLogLog(self.precision)
            self.exact_bucket_users[bucket] = set()
        self.sketches[bucket].add(user_id)
        self.exact_bucket_users[bucket].add(user_id)

    def _buckets_in_range(self, first_day: int, last_day: int) -> list[tuple[str, int]]:
        """Return every stored bucket whose day is within the inclusive range."""
        selected = []
        for bucket in self.sketches:
            day = bucket[1]
            if first_day <= day <= last_day:
                selected.append(bucket)
        return selected

    def naive_active_users(self, first_day: int, last_day: int) -> int:
        """Original report: sum of the per-bucket distinct counts."""
        counts = []
        for bucket in self._buckets_in_range(first_day, last_day):
            counts.append(len(self.exact_bucket_users[bucket]))
        return naive_weekly_active(counts)

    def active_users(self, first_day: int, last_day: int) -> int:
        """Fixed report: merge the sketches, then estimate the union once."""
        combined = HyperLogLog(self.precision)
        for bucket in self._buckets_in_range(first_day, last_day):
            combined.merge(self.sketches[bucket])
        return combined.estimate()
