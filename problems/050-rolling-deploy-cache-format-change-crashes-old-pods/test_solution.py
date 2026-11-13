import json
import unittest

from solution import V1_FORMAT, V2_FORMAT, PricingPod, SharedCache, versioned_cache_key

PRODUCT_ID = 42
PRICE_PAISE = 1999


class FakeDatabase:
    """Returns the product in the shape each release expects, and counts loads."""

    def __init__(self) -> None:
        self.loads = 0

    def __call__(self, product_id: int, schema_version: int) -> dict:
        self.loads += 1
        if schema_version == 1:
            return {"id": product_id, "price": PRICE_PAISE}
        return {"id": product_id, "price": {"amount": PRICE_PAISE, "currency": "INR"}}


def fleet(versioned_keys: bool) -> tuple[PricingPod, PricingPod, SharedCache, FakeDatabase]:
    """One old (v1) and one new (v2) pod sharing a cache, as mid-rollout."""
    cache, database = SharedCache(), FakeDatabase()
    old_pod = PricingPod(V1_FORMAT, database, cache, versioned_keys)
    new_pod = PricingPod(V2_FORMAT, database, cache, versioned_keys)
    return old_pod, new_pod, cache, database


class ReproduceProblem(unittest.TestCase):
    def test_old_pod_crashes_on_entry_written_by_new_pod(self):
        old_pod, new_pod, _cache, _db = fleet(versioned_keys=False)
        new_pod.price_label(PRODUCT_ID)
        with self.assertRaises(TypeError):
            old_pod.price_label(PRODUCT_ID)

    def test_new_pod_crashes_on_entry_written_by_old_pod(self):
        old_pod, new_pod, _cache, _db = fleet(versioned_keys=False)
        old_pod.price_label(PRODUCT_ID)
        with self.assertRaises(TypeError):
            new_pod.price_label(PRODUCT_ID)


class FixVersionedKeys(unittest.TestCase):
    def test_both_versions_serve_correct_prices_side_by_side(self):
        old_pod, new_pod, _cache, _db = fleet(versioned_keys=True)
        for _ in range(3):
            self.assertEqual(old_pod.price_label(PRODUCT_ID), "₹19.99")
            self.assertEqual(new_pod.price_label(PRODUCT_ID), "INR 19.99")

    def test_each_version_writes_its_own_key(self):
        old_pod, new_pod, cache, _db = fleet(versioned_keys=True)
        old_pod.price_label(PRODUCT_ID)
        new_pod.price_label(PRODUCT_ID)
        self.assertEqual(set(cache.values), {"product:v1:42", "product:v2:42"})

    def test_each_version_hits_its_own_cache_after_first_load(self):
        old_pod, new_pod, _cache, database = fleet(versioned_keys=True)
        for _ in range(5):
            old_pod.price_label(PRODUCT_ID)
            new_pod.price_label(PRODUCT_ID)
        self.assertEqual(database.loads, 2)

    def test_corrupt_entry_is_a_miss_not_a_crash(self):
        _old_pod, new_pod, cache, _db = fleet(versioned_keys=True)
        cache.set(versioned_cache_key(PRODUCT_ID, 2), "{not json")
        self.assertEqual(new_pod.price_label(PRODUCT_ID), "INR 19.99")
        self.assertEqual(new_pod.decode_failures, 1)

    def test_wrong_shape_under_versioned_key_is_a_miss(self):
        _old_pod, new_pod, cache, _db = fleet(versioned_keys=True)
        cache.set(versioned_cache_key(PRODUCT_ID, 2), json.dumps({"price": PRICE_PAISE}))
        self.assertEqual(new_pod.price_label(PRODUCT_ID), "INR 19.99")
        self.assertEqual(json.loads(cache.get(versioned_cache_key(PRODUCT_ID, 2)))["price"]["currency"], "INR")


if __name__ == "__main__":
    unittest.main()
