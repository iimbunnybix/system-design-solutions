import unittest

from solution import (CACHE_CAPACITY, FakeDatabase, LRUCache, SegmentedLRUCache,
                      read_through, simulate_night)


def fill_once(cache, keys) -> None:
    """Read each key once, like the nightly scan does."""
    database = FakeDatabase()
    for key in keys:
        read_through(cache, database, key)


def make_hot(cache, keys) -> None:
    """Read each key twice so it counts as reused."""
    fill_once(cache, keys)
    fill_once(cache, keys)


class ReproduceProblem(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.report = simulate_night(LRUCache(CACHE_CAPACITY))

    def test_lru_hit_rate_is_high_during_the_day(self):
        self.assertAlmostEqual(self.report.day_hit_rate, 0.99944)

    def test_scan_drops_lru_user_hit_rate_to_under_ten_percent(self):
        self.assertAlmostEqual(self.report.scan_hit_rate, 0.0892)
        self.assertEqual(self.report.scan_database_queries, 9_108)

    def test_lru_is_still_cold_the_morning_after(self):
        self.assertAlmostEqual(self.report.morning_hit_rate, 0.493)

    def test_one_pass_of_new_keys_evicts_every_hot_key(self):
        cache = LRUCache(4)
        make_hot(cache, ["a", "b"])
        fill_once(cache, range(4))
        self.assertIsNone(cache.get("a"))
        self.assertIsNone(cache.get("b"))


class FixWithSegmentedLRU(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.report = simulate_night(SegmentedLRUCache(CACHE_CAPACITY))

    def test_user_hit_rate_stays_high_during_the_scan(self):
        self.assertAlmostEqual(self.report.scan_hit_rate, 0.9997)
        self.assertEqual(self.report.scan_database_queries, 3)

    def test_daytime_hit_rate_matches_lru(self):
        self.assertAlmostEqual(self.report.day_hit_rate, 0.99944)

    def test_morning_hit_rate_is_unaffected(self):
        self.assertAlmostEqual(self.report.morning_hit_rate, 0.9998)

    def test_keys_hit_twice_survive_a_scan(self):
        cache = SegmentedLRUCache(10)
        make_hot(cache, ["a", "b"])
        fill_once(cache, range(1_000))
        self.assertEqual(cache.get("a"), "product-a")
        self.assertEqual(cache.get("b"), "product-b")

    def test_new_key_starts_in_probation(self):
        cache = SegmentedLRUCache(10)
        cache.put("a", 1)
        self.assertIn("a", cache.probation)
        self.assertNotIn("a", cache.protected)

    def test_second_hit_promotes_to_protected(self):
        cache = SegmentedLRUCache(10)
        cache.put("a", 1)
        cache.get("a")
        self.assertIn("a", cache.protected)

    def test_full_protected_segment_demotes_instead_of_dropping(self):
        cache = SegmentedLRUCache(5, protected_share=0.4)     # 2 protected slots
        make_hot(cache, ["a", "b", "c"])
        self.assertEqual(list(cache.protected), ["b", "c"])
        self.assertIn("a", cache.probation)

    def test_size_never_exceeds_capacity(self):
        cache = SegmentedLRUCache(10)
        make_hot(cache, range(8))
        fill_once(cache, range(100, 200))
        self.assertEqual(len(cache), 10)

    def test_updating_a_protected_key_keeps_it_protected(self):
        cache = SegmentedLRUCache(10)
        make_hot(cache, ["a"])
        cache.put("a", "new")
        self.assertEqual(cache.protected["a"], "new")
        self.assertNotIn("a", cache.probation)

    def test_capacity_of_one_still_caches(self):
        cache = SegmentedLRUCache(1)
        cache.put("a", 1)
        self.assertEqual(cache.get("a"), 1)
        cache.put("b", 2)
        self.assertIsNone(cache.get("a"))
        self.assertEqual(cache.get("b"), 2)

    def test_zero_capacity_is_rejected(self):
        with self.assertRaises(ValueError):
            SegmentedLRUCache(0)


class TradeOffs(unittest.TestCase):
    def test_new_key_seen_once_is_lost_to_a_scan_before_its_second_hit(self):
        cache = SegmentedLRUCache(10)
        cache.put("new-hot-product", 1)
        fill_once(cache, range(100))
        self.assertIsNone(cache.get("new-hot-product"))


if __name__ == "__main__":
    unittest.main()
