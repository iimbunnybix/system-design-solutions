import unittest

from solution import (MAX_AGE_SECONDS, BoundedAgeCache, FakeClock, NaiveSlidingCache,
                      PriceDatabase, seconds_stale)

PRODUCT = "sku-123"
OLD_PRICE = 1999
NEW_PRICE = 1499
HOT_READ_INTERVAL = 2.0          # a popular product page is viewed every 2 seconds
ONE_DAY = 24 * 60 * 60.0


def make_setup(cache_class):
    clock = FakeClock()
    database = PriceDatabase({PRODUCT: OLD_PRICE})
    return clock, database, cache_class(clock)


class ReproduceProblem(unittest.TestCase):
    def test_hot_key_serves_old_price_for_a_whole_day(self):
        clock, database, cache = make_setup(NaiveSlidingCache)
        stale = seconds_stale(cache, database, clock, PRODUCT, NEW_PRICE,
                              HOT_READ_INTERVAL, ONE_DAY)
        self.assertEqual(stale, ONE_DAY)

    def test_hot_key_is_loaded_from_the_database_only_once(self):
        clock, database, cache = make_setup(NaiveSlidingCache)
        seconds_stale(cache, database, clock, PRODUCT, NEW_PRICE, HOT_READ_INTERVAL, ONE_DAY)
        self.assertEqual(cache.loads, 1)

    def test_cold_key_does_refresh_after_the_ttl(self):
        clock, database, cache = make_setup(NaiveSlidingCache)
        stale = seconds_stale(cache, database, clock, PRODUCT, NEW_PRICE, 400.0, ONE_DAY)
        self.assertEqual(stale, 0.0)


class FixMaximumAge(unittest.TestCase):
    def test_hot_key_shows_new_price_within_max_age(self):
        clock, database, cache = make_setup(BoundedAgeCache)
        stale = seconds_stale(cache, database, clock, PRODUCT, NEW_PRICE,
                              HOT_READ_INTERVAL, ONE_DAY)
        self.assertLessEqual(stale, MAX_AGE_SECONDS)

    def test_hot_key_still_served_from_cache_between_reloads(self):
        clock, database, cache = make_setup(BoundedAgeCache)
        for _ in range(150):                         # 300 seconds of reads
            cache.get(PRODUCT, database.load)
            clock.advance(HOT_READ_INTERVAL)
        self.assertEqual(cache.loads, 1)

    def test_entry_reloads_exactly_at_max_age(self):
        clock, database, cache = make_setup(BoundedAgeCache)
        cache.get(PRODUCT, database.load)
        clock.advance(MAX_AGE_SECONDS)
        cache.get(PRODUCT, database.load)
        self.assertEqual(cache.loads, 2)

    def test_reload_resets_the_age(self):
        clock, database, cache = make_setup(BoundedAgeCache)
        cache.get(PRODUCT, database.load)
        clock.advance(MAX_AGE_SECONDS)
        cache.get(PRODUCT, database.load)            # reload
        clock.advance(MAX_AGE_SECONDS - 1)
        cache.get(PRODUCT, database.load)
        self.assertEqual(cache.loads, 2)

    def test_idle_entry_still_expires_on_idle_timeout(self):
        clock = FakeClock()
        database = PriceDatabase({PRODUCT: OLD_PRICE})
        cache = BoundedAgeCache(clock, idle_ttl_seconds=60.0, max_age_seconds=600.0)
        cache.get(PRODUCT, database.load)
        clock.advance(61.0)
        cache.get(PRODUCT, database.load)
        self.assertEqual(cache.loads, 2)

    def test_hot_key_reloads_once_per_max_age_over_a_day(self):
        clock, database, cache = make_setup(BoundedAgeCache)
        reads = int(ONE_DAY / HOT_READ_INTERVAL)
        for _ in range(reads):
            cache.get(PRODUCT, database.load)
            clock.advance(HOT_READ_INTERVAL)
        # 43,200 page views cost 288 database loads (one per 5 minutes).
        self.assertEqual(cache.loads, int(ONE_DAY / MAX_AGE_SECONDS))

if __name__ == "__main__":
    unittest.main()
