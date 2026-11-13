"""An in-process LRU cache bounded by bytes, not by number of entries.

The product-API pods cached rendered responses in an LRU limited to 10,000
entries, sized for ~2 KB product cards (about 20 MB). A new endpoint started
caching full category pages of several hundred KB through the same cache.
The entry count never exceeded 10,000, but memory did, and pods were
OOM-killed. This cache tracks the size of every value, evicts until the
total fits a byte budget, and refuses to cache single values too large to
be worth it.
"""
from __future__ import annotations

import random
from collections import OrderedDict

KB = 1024
MB = 1024 * KB


class NaiveCountLRU:
    """Original cache: evicts only when the number of entries exceeds the limit."""

    def __init__(self, max_entries: int) -> None:
        self.max_entries = max_entries
        self.entries: OrderedDict[str, bytes] = OrderedDict()
        self.total_bytes = 0

    def put(self, key: str, value: bytes) -> None:
        old_value = self.entries.pop(key, None)
        if old_value is not None:
            self.total_bytes -= len(old_value)
        self.entries[key] = value
        self.total_bytes += len(value)
        while len(self.entries) > self.max_entries:
            _key, evicted = self.entries.popitem(last=False)
            self.total_bytes -= len(evicted)

    def get(self, key: str) -> bytes | None:
        if key not in self.entries:
            return None
        self.entries.move_to_end(key)
        return self.entries[key]

    def bytes_used(self) -> int:
        return self.total_bytes


class ByteBoundedLRU:
    """LRU cache whose total value size never exceeds `max_bytes`."""

    def __init__(self, max_bytes: int, max_item_bytes: int) -> None:
        if max_item_bytes > max_bytes:
            raise ValueError("max_item_bytes cannot exceed max_bytes")
        self.max_bytes = max_bytes
        self.max_item_bytes = max_item_bytes
        self.entries: OrderedDict[str, bytes] = OrderedDict()
        # Kept as a running total so put() does not re-sum every value.
        self.total_bytes = 0
        self.rejected_too_large = 0

    def put(self, key: str, value: bytes) -> bool:
        """Cache the value if it is small enough; return whether it was stored."""
        # Remove any old version first so its size is not counted twice.
        self._remove(key)
        if len(value) > self.max_item_bytes:
            # One huge value would evict hundreds of small hot ones; serving it
            # uncached is cheaper than wiping the cache for it.
            self.rejected_too_large += 1
            return False
        self.entries[key] = value
        self.total_bytes += len(value)
        self._evict_until_within_budget()
        return True

    def get(self, key: str) -> bytes | None:
        """Return the value and mark it most recently used, or None on a miss."""
        if key not in self.entries:
            return None
        self.entries.move_to_end(key)
        return self.entries[key]

    def bytes_used(self) -> int:
        return self.total_bytes

    def _remove(self, key: str) -> None:
        old_value = self.entries.pop(key, None)
        if old_value is not None:
            self.total_bytes -= len(old_value)

    def _evict_until_within_budget(self) -> None:
        """Drop least-recently-used entries until the byte total fits the budget."""
        while self.total_bytes > self.max_bytes:
            _key, evicted = self.entries.popitem(last=False)
            self.total_bytes -= len(evicted)


def simulated_requests(count: int, seed: int) -> list[tuple[str, int]]:
    """(key, response size) pairs: mostly 2 KB product cards, some ~300 KB category pages."""
    rng = random.Random(seed)
    requests = []
    for _ in range(count):
        if rng.random() < 0.2:
            requests.append((f"category:{rng.randint(1, 5_000)}", rng.randint(200 * KB, 400 * KB)))
        else:
            requests.append((f"product:{rng.randint(1, 50_000)}", 2 * KB))
    return requests


def peak_bytes(cache, requests: list[tuple[str, int]]) -> int:
    """Replay requests (miss -> put) and return the highest memory use seen."""
    peak = 0
    for key, size in requests:
        if cache.get(key) is None:
            cache.put(key, b"x" * size)
        peak = max(peak, cache.bytes_used())
    return peak
