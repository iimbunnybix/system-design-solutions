import unittest

from solution import (CACHE_TTL_MS, MARKER_TTL_MS, NEW_PRICE, OLD_PRICE, Cache, CacheEntry,
                      Database, FakeClock, NaiveProductService, VersionedProductService, simulate)

LAG_MS = 250


def setup_service(service_class: type, lag_ms: int = LAG_MS):
    """A service whose replica already holds the old price and whose cache is warm."""
    clock = FakeClock()
    db = Database(clock, lag_ms)
    service = service_class(db, Cache(clock))
    db.write("sku-1", OLD_PRICE)
    clock.advance(lag_ms + 10)
    service.get_price("sku-1")
    return clock, db, service


class ReproduceProblem(unittest.TestCase):
    def test_read_right_after_update_caches_the_old_price(self):
        clock, _, service = setup_service(NaiveProductService)
        service.update_price("sku-1", NEW_PRICE)
        clock.advance(10)
        self.assertEqual(service.get_price("sku-1"), OLD_PRICE)

    def test_old_price_stays_after_the_replica_caught_up(self):
        clock, db, service = setup_service(NaiveProductService)
        service.update_price("sku-1", NEW_PRICE)
        clock.advance(10)
        service.get_price("sku-1")
        clock.advance(60_000)                                 # replica caught up long ago
        self.assertEqual(db.read_replica("sku-1")[0], NEW_PRICE)
        self.assertEqual(service.get_price("sku-1"), OLD_PRICE)

    def test_hot_product_shows_old_price_for_the_whole_ttl(self):
        result = simulate(NaiveProductService)
        self.assertEqual(result["stale_reads"], 60_000)       # 10 minutes at 100 reads/s
        self.assertEqual(result["last_stale_ms"], 600_990)

    def test_even_a_5_ms_lag_pins_the_old_price(self):
        self.assertEqual(simulate(NaiveProductService, lag_ms=5)["stale_reads"], 60_000)


class FixVersionedFill(unittest.TestCase):
    def test_no_read_after_the_update_shows_the_old_price(self):
        result = simulate(VersionedProductService)
        self.assertEqual(result["stale_reads"], 0)

    def test_only_one_primary_read_per_invalidation(self):
        self.assertEqual(simulate(VersionedProductService)["primary_reads"], 1)

    def test_miss_inside_lag_window_reads_the_primary(self):
        clock, db, service = setup_service(VersionedProductService)
        service.update_price("sku-1", NEW_PRICE)
        clock.advance(10)
        self.assertEqual(service.get_price("sku-1"), NEW_PRICE)
        self.assertEqual(db.primary_reads, 1)

    def test_miss_after_replica_caught_up_uses_the_replica(self):
        clock, db, service = setup_service(VersionedProductService)
        service.update_price("sku-1", NEW_PRICE)
        clock.advance(LAG_MS + 10)
        self.assertEqual(service.get_price("sku-1"), NEW_PRICE)
        self.assertEqual(db.primary_reads, 0)

    def test_misses_without_a_marker_never_touch_the_primary(self):
        clock, db, service = setup_service(VersionedProductService)
        clock.advance(CACHE_TTL_MS)                           # plain TTL expiry
        self.assertEqual(service.get_price("sku-1"), OLD_PRICE)
        self.assertEqual(db.primary_reads, 0)

    def test_late_fill_with_an_older_version_is_rejected(self):
        clock, db, service = setup_service(VersionedProductService)
        service.cache.delete("sku-1")
        old_value, old_lsn = db.read_replica("sku-1")         # a slow request reads first...
        service.update_price("sku-1", NEW_PRICE)              # ...the update lands...
        late_fill = CacheEntry(old_value, old_lsn, clock.now_ms + CACHE_TTL_MS)
        self.assertFalse(service.cache.set_if_newer("sku-1", late_fill))   # ...and its SET loses
        clock.advance(10)
        self.assertEqual(service.get_price("sku-1"), NEW_PRICE)

    def test_two_quick_updates_end_with_the_latest_price(self):
        clock, _, service = setup_service(VersionedProductService)
        service.update_price("sku-1", NEW_PRICE)
        clock.advance(10)
        service.get_price("sku-1")
        service.update_price("sku-1", 2_999)
        clock.advance(10)
        self.assertEqual(service.get_price("sku-1"), 2_999)

    def test_lag_longer_than_the_marker_ttl_can_still_cache_old_data(self):
        # Documented limit: the marker must outlive the worst replication lag.
        clock, _, service = setup_service(VersionedProductService, lag_ms=MARKER_TTL_MS + 10_000)
        service.update_price("sku-1", NEW_PRICE)
        clock.advance(MARKER_TTL_MS + 1)                      # nobody read while the marker lived
        self.assertEqual(service.get_price("sku-1"), OLD_PRICE)


if __name__ == "__main__":
    unittest.main()
