import unittest

from solution import KB, MB, ByteBoundedLRU, NaiveCountLRU, peak_bytes, simulated_requests

MAX_ENTRIES = 10_000          # sized for 2 KB product cards: ~20 MB
MEMORY_BUDGET = 64 * MB
MAX_ITEM = 1 * MB
REQUEST_COUNT = 20_000
SEED = 5


def small_cache() -> ByteBoundedLRU:
    """10 KB budget so eviction is easy to follow in a test."""
    return ByteBoundedLRU(max_bytes=10 * KB, max_item_bytes=4 * KB)


def value(size_kb: int) -> bytes:
    return b"v" * (size_kb * KB)


class ReproduceProblem(unittest.TestCase):
    def test_entry_limit_is_respected_while_memory_explodes(self):
        cache = NaiveCountLRU(MAX_ENTRIES)
        peak = peak_bytes(cache, simulated_requests(REQUEST_COUNT, SEED))
        self.assertLessEqual(len(cache.entries), MAX_ENTRIES)
        self.assertGreater(peak, 500 * MB)


class FixByteBudget(unittest.TestCase):
    def test_peak_memory_never_exceeds_budget(self):
        cache = ByteBoundedLRU(MEMORY_BUDGET, MAX_ITEM)
        peak = peak_bytes(cache, simulated_requests(REQUEST_COUNT, SEED))
        self.assertLessEqual(peak, MEMORY_BUDGET)

    def test_least_recently_used_is_evicted_first(self):
        cache = small_cache()
        cache.put("a", value(4))
        cache.put("b", value(4))
        cache.get("a")                      # "b" is now the oldest
        cache.put("c", value(4))            # 12 KB > 10 KB: evict one
        self.assertIsNone(cache.get("b"))
        self.assertIsNotNone(cache.get("a"))
        self.assertEqual(cache.bytes_used(), 8 * KB)

    def test_one_large_value_evicts_several_small_ones(self):
        cache = small_cache()
        for name in ["a", "b", "c", "d", "e"]:
            cache.put(name, value(2))
        cache.put("big", value(4))
        self.assertEqual(list(cache.entries), ["c", "d", "e", "big"])

    def test_value_larger_than_item_limit_is_not_cached(self):
        cache = small_cache()
        cache.put("a", value(2))
        self.assertFalse(cache.put("huge", value(5)))
        self.assertIsNone(cache.get("huge"))
        self.assertIsNotNone(cache.get("a"))
        self.assertEqual(cache.rejected_too_large, 1)

    def test_overwriting_a_key_counts_only_the_new_size(self):
        cache = small_cache()
        cache.put("a", value(4))
        cache.put("a", value(1))
        self.assertEqual(cache.bytes_used(), 1 * KB)

    def test_oversized_overwrite_removes_the_stale_value(self):
        cache = small_cache()
        cache.put("a", value(1))
        cache.put("a", value(5))
        self.assertIsNone(cache.get("a"))
        self.assertEqual(cache.bytes_used(), 0)

    def test_item_limit_above_budget_is_rejected(self):
        with self.assertRaises(ValueError):
            ByteBoundedLRU(max_bytes=1 * KB, max_item_bytes=2 * KB)


if __name__ == "__main__":
    unittest.main()
