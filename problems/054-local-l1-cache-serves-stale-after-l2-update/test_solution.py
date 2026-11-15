import unittest

from solution import (L1_TTL_SECONDS, Invalidation, InvalidationBus, NaivePod, Pod,
                      SharedStore)

KEY = "price:sku-42"
OLD_PRICE = 1999
NEW_PRICE = 1499


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def store_with_price() -> SharedStore:
    store = SharedStore()
    store.write(KEY, OLD_PRICE)
    return store


class ReproduceProblem(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        store = store_with_price()
        self.writer = NaivePod(store, self.clock)
        self.reader = NaivePod(store, self.clock)
        self.reader.get(KEY)                    # reader's L1 now holds the old price
        self.writer.update(KEY, NEW_PRICE)

    def test_other_pod_serves_old_price_after_update(self):
        self.assertEqual(self.reader.get(KEY), OLD_PRICE)

    def test_stale_price_lasts_until_just_before_the_ttl(self):
        self.clock.now = L1_TTL_SECONDS - 1
        self.assertEqual(self.reader.get(KEY), OLD_PRICE)
        self.clock.now = L1_TTL_SECONDS
        self.assertEqual(self.reader.get(KEY), NEW_PRICE)

    def test_writing_pod_itself_sees_the_new_price(self):
        self.assertEqual(self.writer.get(KEY), NEW_PRICE)


class FixBroadcastInvalidation(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.store = store_with_price()
        self.bus = InvalidationBus()
        self.pods = [Pod(self.store, self.clock, self.bus) for _ in range(3)]
        for pod in self.pods:
            pod.get(KEY)

    def test_every_pod_serves_the_new_price_immediately(self):
        self.pods[0].update(KEY, NEW_PRICE)
        for pod in self.pods:
            self.assertEqual(pod.get(KEY), NEW_PRICE)

    def test_unrelated_keys_stay_cached(self):
        self.store.write("price:other", 500)
        reader = self.pods[1]
        reader.get("price:other")
        self.pods[0].update(KEY, NEW_PRICE)
        self.assertIn("price:other", reader.l1)

    def test_delayed_event_does_not_evict_a_newer_cached_value(self):
        reader = self.pods[1]
        self.pods[0].update(KEY, NEW_PRICE)     # version 2
        reader.get(KEY)                         # reader caches version 2
        old_event = Invalidation(self.bus.last_sequence + 1, KEY, version=1)
        reader.receive(old_event)
        self.assertIn(KEY, reader.l1)
        self.assertEqual(reader.l1[KEY].version, 2)


class FixMissedMessages(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.store = store_with_price()
        self.bus = InvalidationBus()
        self.writer = Pod(self.store, self.clock, self.bus)
        self.reader = Pod(self.store, self.clock, self.bus)
        self.store.write("price:other", 500)
        self.reader.get(KEY)
        self.reader.get("price:other")

    def test_lost_invalidation_alone_leaves_the_pod_stale(self):
        self.reader.connected = False
        self.writer.update(KEY, NEW_PRICE)
        self.reader.connected = True
        self.assertEqual(self.reader.get(KEY), OLD_PRICE)

    def test_gap_in_sequence_numbers_clears_the_whole_l1(self):
        self.reader.connected = False
        self.writer.update(KEY, NEW_PRICE)
        self.reader.connected = True
        self.writer.update("price:other", 600)  # next event reveals the gap
        self.assertEqual(self.reader.l1, {})
        self.assertEqual(self.reader.get(KEY), NEW_PRICE)


if __name__ == "__main__":
    unittest.main()
