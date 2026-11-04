"""Bloom filter in front of the database for "is this username taken?" checks.

The original bug: the sign-up form called ``GET /usernames/{name}/available``
on every keystroke (debounced to ~300 ms), and each call ran a uniqueness query
on the users database. During a marketing campaign the endpoint became the
single largest source of database queries, and almost all of them answered
"available": people type many prefixes and variations that nobody has taken.

Fix: keep a Bloom filter of all taken usernames in each API instance. A Bloom
filter can say "definitely not taken" with certainty (no false negatives) and
"maybe taken" with a small, tunable false-positive rate. Only the "maybe" case
goes to the database, which stays the source of truth (and keeps its unique
index for the actual sign-up).
"""
from __future__ import annotations

import hashlib
import math


class BloomFilter:
    def __init__(self, capacity: int, fp_rate: float):
        if capacity <= 0 or not 0 < fp_rate < 1:
            raise ValueError("capacity must be > 0 and 0 < fp_rate < 1")
        self.capacity = capacity
        self.fp_rate = fp_rate
        # Standard optimal sizing: m = -n ln p / (ln 2)^2, k = (m / n) ln 2
        self.m = math.ceil(-capacity * math.log(fp_rate) / (math.log(2) ** 2))
        self.k = max(1, round(self.m / capacity * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.count = 0

    def _positions(self, item: str):
        """Kirsch-Mitzenmacher double hashing: k positions from one SHA-256."""
        d = hashlib.sha256(item.encode("utf-8")).digest()
        h1 = int.from_bytes(d[:8], "big")
        h2 = int.from_bytes(d[8:16], "big") | 1
        return ((h1 + i * h2) % self.m for i in range(self.k))

    def add(self, item: str) -> None:
        for p in self._positions(item):
            self.bits[p >> 3] |= 1 << (p & 7)
        self.count += 1

    def __contains__(self, item: str) -> bool:
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(item))

    def expected_fp_rate(self) -> float:
        """(1 - e^(-k n / m))^k for the current number of inserted items."""
        return (1 - math.exp(-self.k * self.count / self.m)) ** self.k

    def needs_rebuild(self) -> bool:
        """True once over capacity: the FP rate climbs quickly past the design point."""
        return self.count > self.capacity

    def to_bytes(self) -> bytes:
        header = f"{self.capacity},{self.fp_rate!r},{self.count}\n".encode()
        return header + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        header, _, body = data.partition(b"\n")
        cap, rate, count = header.decode().split(",")
        bf = cls(int(cap), float(rate))
        if len(body) != len(bf.bits):
            raise ValueError("bit array length does not match header")
        bf.bits[:] = body
        bf.count = int(count)
        return bf


def normalize(username: str) -> str:
    """Usernames are case-insensitive; normalize before *every* lookup and insert."""
    return username.strip().lower()


class UsernameService:
    """Availability checks with an optional Bloom filter in front of the DB."""

    def __init__(self, taken: set[str], bloom: BloomFilter | None):
        self.db = {normalize(u) for u in taken}       # stands in for the users table
        self.bloom = bloom
        self.db_queries = 0
        if bloom is not None:
            for u in self.db:
                bloom.add(u)

    def _db_exists(self, name: str) -> bool:
        self.db_queries += 1
        return name in self.db

    def is_available(self, username: str) -> bool:
        name = normalize(username)
        if self.bloom is not None and name not in self.bloom:
            return True                                # definitely not taken
        return not self._db_exists(name)               # maybe taken: ask the DB

    def register(self, username: str) -> bool:
        """The DB unique constraint is the real guard; the filter is only a cache."""
        name = normalize(username)
        if name in self.db:
            return False
        self.db.add(name)
        if self.bloom is not None:
            self.bloom.add(name)                       # add right after commit
        return True
