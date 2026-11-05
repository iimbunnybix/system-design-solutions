import random
import unittest

from solution import (HEDGE_BUDGET_BURST, HEDGE_BUDGET_FRACTION, INITIAL_HEDGE_DELAY_MS,
                      MIN_SAMPLES_FOR_DELAY, HedgeBudget, HedgeDelay, HedgedCaller,
                      NaiveCaller, ReplicaLatency, page_latency_ms, percentile, simulate)

SEED = 7
PAGES = 5000
NORMAL_P99_LIMIT_MS = 30.0       # fast calls are 8-12 ms; a hedged slow call adds ~12 ms


class ScriptedLatency:
    """Returns the given latencies in order, so a test controls every call."""

    def __init__(self, latencies_ms: list[float]) -> None:
        self.remaining = list(latencies_ms)

    def sample_ms(self) -> float:
        return self.remaining.pop(0)


def seeded_latency() -> ReplicaLatency:
    return ReplicaLatency(random.Random(SEED))


class ReproduceProblem(unittest.TestCase):
    def test_about_a_third_of_pages_wait_for_a_slow_replica(self):
        caller = NaiveCaller(seeded_latency())
        slow_pages = 0
        for _ in range(PAGES):
            if page_latency_ms(caller) >= 200.0:
                slow_pages += 1
        self.assertAlmostEqual(slow_pages / PAGES, 0.33, delta=0.02)

    def test_page_p99_is_set_by_slow_replicas(self):
        result = simulate(NaiveCaller(seeded_latency()), PAGES)
        self.assertLess(result.p50_ms, 13.0)
        self.assertGreater(result.p99_ms, 400.0)


class FixHedgedRequests(unittest.TestCase):
    def test_hedging_brings_p99_close_to_normal_latency(self):
        result = simulate(HedgedCaller(seeded_latency(), HedgeBudget()), PAGES)
        self.assertLess(result.p99_ms, NORMAL_P99_LIMIT_MS)

    def test_hedging_adds_only_a_few_percent_of_calls(self):
        result = simulate(HedgedCaller(seeded_latency(), HedgeBudget()), PAGES)
        self.assertLess(result.calls_per_shard_request, 1.07)

    def test_fast_first_call_is_not_hedged(self):
        caller = HedgedCaller(ScriptedLatency([10.0]))
        self.assertEqual(caller.call_shard(), 10.0)
        self.assertEqual(caller.hedges_sent, 0)

    def test_backup_answer_is_used_when_it_arrives_first(self):
        caller = HedgedCaller(ScriptedLatency([300.0, 10.0]))
        latency = caller.call_shard()
        self.assertEqual(latency, INITIAL_HEDGE_DELAY_MS + 10.0)
        self.assertEqual(caller.calls_sent, 2)

    def test_first_answer_is_kept_when_backup_is_slower(self):
        caller = HedgedCaller(ScriptedLatency([60.0, 300.0]))
        self.assertEqual(caller.call_shard(), 60.0)


class FixHedgeDelay(unittest.TestCase):
    def test_initial_delay_is_used_until_enough_samples(self):
        delay = HedgeDelay()
        for _ in range(MIN_SAMPLES_FOR_DELAY - 1):
            delay.record(10.0)
        self.assertEqual(delay.current_ms(), INITIAL_HEDGE_DELAY_MS)

    def test_delay_follows_recent_p95(self):
        delay = HedgeDelay()
        for value in range(1, 201):
            delay.record(float(value))
        self.assertEqual(delay.current_ms(), 190.0)


class FixHedgeBudget(unittest.TestCase):
    def test_budget_allows_a_burst_then_refuses(self):
        budget = HedgeBudget(fraction=0.1, burst=2.0)
        self.assertTrue(budget.try_spend())
        self.assertTrue(budget.try_spend())
        self.assertFalse(budget.try_spend())

    def test_budget_refills_with_first_calls(self):
        budget = HedgeBudget(fraction=0.25, burst=2.0)     # exact in binary floating point
        budget.tokens = 0.0
        for _ in range(4):
            budget.earn()
        self.assertTrue(budget.try_spend())

    def test_budget_never_exceeds_burst(self):
        budget = HedgeBudget(fraction=0.5, burst=2.0)
        for _ in range(100):
            budget.earn()
        self.assertEqual(budget.tokens, 2.0)

    def test_budget_limits_extra_load_during_fleet_wide_slowdown(self):
        hedges = {}
        for use_budget in (False, True):
            latency = seeded_latency()
            caller = HedgedCaller(latency, HedgeBudget() if use_budget else None)
            simulate(caller, 500)                  # warm up on healthy traffic
            latency.slow_probability = 1.0         # every replica becomes slow
            caller.hedges_sent = 0
            simulate(caller, 25)                   # 500 shard calls
            hedges[use_budget] = caller.hedges_sent
        self.assertLessEqual(hedges[True], HEDGE_BUDGET_BURST + HEDGE_BUDGET_FRACTION * 500)
        self.assertGreater(hedges[False], 2 * hedges[True])


class Percentile(unittest.TestCase):
    def test_nearest_rank(self):
        self.assertEqual(percentile([5.0, 1.0, 3.0, 2.0, 4.0], 50), 3.0)
        self.assertEqual(percentile([1.0, 2.0], 99), 2.0)

    def test_empty_list_is_rejected(self):
        with self.assertRaises(ValueError):
            percentile([], 50)


if __name__ == "__main__":
    unittest.main()
