import unittest

from solution import (CUTOVER_SECOND, DB_BUDGET_READS_PER_SECOND, DECISION_SECONDS,
                      RAMP_PERCENTS, LruCache, RampController, SecondStats, first_second_fully_on_new,
                      naive_cutover, normal_db_load, routes_to_new, warmed_cutover)

SEED = 1


def warm_window_stats(db_reads=50):
    """One second where the new cluster is as warm as the old one."""
    return SecondStats(db_reads=db_reads, old_lookups=1000, old_hits=950,
                       new_lookups=1000, new_hits=950)


def cold_window_stats():
    """One second where the new cluster misses far more than the old one."""
    return SecondStats(db_reads=50, old_lookups=1000, old_hits=950,
                       new_lookups=1000, new_hits=300)


class ReproduceProblem(unittest.TestCase):
    def test_old_cluster_answers_about_95_percent_of_reads(self):
        self.assertAlmostEqual(normal_db_load(naive_cutover(SEED)), 47.3, places=1)

    def test_flipping_to_the_empty_cluster_sends_every_read_to_the_database(self):
        result = naive_cutover(SEED)
        self.assertEqual(result.db_reads_per_second[CUTOVER_SECOND], 1000)
        self.assertGreater(result.peak_db_load(), 20 * normal_db_load(result))

    def test_reads_beyond_database_capacity_fail(self):
        self.assertEqual(naive_cutover(SEED).failed_reads, 705)

    def test_database_stays_over_budget_while_the_new_cluster_fills(self):
        self.assertEqual(naive_cutover(SEED).seconds_over_budget(), 4)


class FixShadowFillAndGuardedRamp(unittest.TestCase):
    def test_database_load_never_exceeds_budget(self):
        result = warmed_cutover(SEED)
        self.assertEqual(result.peak_db_load(), 67)
        self.assertEqual(result.seconds_over_budget(), 0)

    def test_no_read_fails(self):
        for seed in range(5):
            with self.subTest(seed=seed):
                self.assertEqual(warmed_cutover(seed).failed_reads, 0)

    def test_all_traffic_reaches_the_new_cluster(self):
        self.assertEqual(first_second_fully_on_new(warmed_cutover(SEED)), 100)

    def test_shadow_phase_keeps_database_load_unchanged(self):
        naive = naive_cutover(SEED)
        fixed = warmed_cutover(SEED)
        self.assertEqual(fixed.db_reads_per_second[:CUTOVER_SECOND],
                         naive.db_reads_per_second[:CUTOVER_SECOND])

    def test_ramp_without_shadow_fill_still_overloads_and_steps_back(self):
        result = warmed_cutover(SEED, shadow_fill=False)
        self.assertEqual(result.failed_reads, 218)
        self.assertEqual(result.peak_db_load(), 483)
        # The first 5% step overloads the DB, and the next second is back on 0%.
        self.assertEqual(result.percent_on_new[10:12], [5, 0])


class RampControllerDecisions(unittest.TestCase):
    def test_advances_one_step_after_a_warm_window(self):
        controller = RampController()
        for _ in range(DECISION_SECONDS):
            controller.observe(warm_window_stats())
        self.assertEqual(controller.percent(), RAMP_PERCENTS[1])

    def test_does_not_advance_while_new_cluster_is_cold(self):
        controller = RampController()
        for _ in range(DECISION_SECONDS * 3):
            controller.observe(cold_window_stats())
        self.assertEqual(controller.percent(), 0)

    def test_steps_back_at_once_when_database_is_over_budget(self):
        controller = RampController(step=3)
        controller.observe(warm_window_stats(db_reads=DB_BUDGET_READS_PER_SECOND + 1))
        self.assertEqual(controller.percent(), RAMP_PERCENTS[2])

    def test_never_steps_below_shadow_only(self):
        controller = RampController()
        controller.observe(warm_window_stats(db_reads=DB_BUDGET_READS_PER_SECOND + 1))
        self.assertEqual(controller.percent(), 0)

    def test_never_goes_past_full_cutover(self):
        controller = RampController(step=len(RAMP_PERCENTS) - 1)
        for _ in range(DECISION_SECONDS * 2):
            controller.observe(warm_window_stats())
        self.assertEqual(controller.percent(), 100)


class RoutingAndCache(unittest.TestCase):
    def test_zero_percent_routes_nothing_and_full_routes_everything(self):
        keys = range(1000)
        self.assertFalse(any(routes_to_new(k, 0) for k in keys))
        self.assertTrue(all(routes_to_new(k, 100) for k in keys))

    def test_a_key_moved_at_a_small_step_stays_moved_at_larger_steps(self):
        moved = [k for k in range(1000) if routes_to_new(k, 5)]
        self.assertTrue(all(routes_to_new(k, 25) for k in moved))

    def test_lru_evicts_the_least_recently_used_key(self):
        cache = LruCache(capacity=2)
        cache.put(1)
        cache.put(2)
        cache.contains(1)
        cache.put(3)
        self.assertEqual((cache.contains(1), cache.contains(2)), (True, False))


if __name__ == "__main__":
    unittest.main()
