import random
import unittest

from solution import ActiveUserStore, HyperLogLog, naive_weekly_active

USERS = 40_000
DAYS = 7
SHARDS = 8
SEED = 7
# Three standard errors (3 x 0.81%) at the default precision.
TOLERANCE = 0.025


def simulate_week(users: int = USERS, seed: int = SEED) -> ActiveUserStore:
    """Each user is active on 1-7 random days, with 1-3 sessions per day,
    and every session is served by a random shard."""
    rng = random.Random(seed)
    store = ActiveUserStore()
    for number in range(users):
        user_id = f"user-{number}"
        for day in rng.sample(range(DAYS), rng.randint(1, DAYS)):
            for _ in range(rng.randint(1, 3)):
                store.record(f"shard-{rng.randrange(SHARDS)}", day, user_id)
    return store


def exact_users_on_day(store: ActiveUserStore, day: int) -> int:
    users = set()
    for (_, bucket_day), bucket_users in store.exact_bucket_users.items():
        if bucket_day == day:
            users |= bucket_users
    return len(users)


class ReproduceProblem(unittest.TestCase):
    def test_summing_bucket_counts_reports_seven_times_the_real_wau(self):
        store = simulate_week()
        self.assertEqual(store.naive_active_users(0, DAYS - 1), 294_569)

    def test_summing_counts_users_once_per_shard_they_touched(self):
        store = simulate_week()
        self.assertEqual(store.naive_active_users(3, 3), 42_178)
        self.assertEqual(exact_users_on_day(store, 3), 22_827)

    def test_one_user_on_two_shards_counts_twice(self):
        self.assertEqual(naive_weekly_active([1, 1]), 2)


class FixWithMergedHyperLogLog(unittest.TestCase):
    def test_weekly_estimate_is_within_tolerance_of_the_truth(self):
        store = simulate_week()
        self.assertEqual(store.active_users(0, DAYS - 1), 39_469)
        self.assertAlmostEqual(store.active_users(0, DAYS - 1) / USERS, 1, delta=TOLERANCE)

    def test_single_day_estimate_merges_all_shards(self):
        store = simulate_week()
        truth = exact_users_on_day(store, 3)
        self.assertAlmostEqual(store.active_users(3, 3) / truth, 1, delta=TOLERANCE)

    def test_repeated_ids_do_not_change_the_estimate(self):
        sketch = HyperLogLog()
        for _ in range(1_000):
            sketch.add("same-user")
        self.assertEqual(sketch.estimate(), 1)

    def test_small_sets_are_counted_exactly(self):
        sketch = HyperLogLog()
        for number in range(10):
            sketch.add(f"user-{number}")
        self.assertEqual(sketch.estimate(), 10)

    def test_empty_sketch_estimates_zero(self):
        self.assertEqual(HyperLogLog().estimate(), 0)

    def test_empty_range_reports_zero_users(self):
        self.assertEqual(simulate_week(users=100).active_users(30, 40), 0)

    def test_merge_equals_sketch_of_the_union(self):
        left, right, union = HyperLogLog(), HyperLogLog(), HyperLogLog()
        for number in range(5_000):
            user_id = f"user-{number}"
            (left if number % 2 else right).add(user_id)
            union.add(user_id)
        left.merge(right)
        self.assertEqual(left.registers, union.registers)

    def test_merging_twice_is_harmless(self):
        sketch, other = HyperLogLog(), HyperLogLog()
        for number in range(2_000):
            other.add(f"user-{number}")
        sketch.merge(other)
        once = sketch.estimate()
        sketch.merge(other)
        self.assertEqual(sketch.estimate(), once)

    def test_different_precisions_cannot_be_merged(self):
        with self.assertRaises(ValueError):
            HyperLogLog(12).merge(HyperLogLog(14))

    def test_invalid_precision_is_rejected(self):
        with self.assertRaises(ValueError):
            HyperLogLog(3)

    def test_sketch_memory_is_fixed_at_16_kib(self):
        self.assertEqual(len(HyperLogLog().registers), 16_384)

    def test_standard_error_matches_the_paper(self):
        self.assertAlmostEqual(HyperLogLog().standard_error(), 0.0081, places=4)


if __name__ == "__main__":
    unittest.main()
