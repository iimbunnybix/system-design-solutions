import unittest

from solution import (BLIP_TICKS, REQUESTS_PER_TICK, Database, Outcome, RequestStack,
                      RetryBudget, budgeted_stack, call_with_retries, naive_stack,
                      simulate, single_layer_stack)

FIRST_TICK_AFTER_BLIP = BLIP_TICKS[-1] + 1


class ScriptedDatabase:
    """Fake database that returns the scripted results in order, then fails."""

    def __init__(self, results: list[bool]) -> None:
        self.remaining = list(results)
        self.calls_received = 0

    def execute(self) -> Outcome:
        self.calls_received += 1
        if self.remaining:
            return Outcome(ok=self.remaining.pop(0))
        return Outcome(ok=False)


def warmed_budget(first_attempts: int = 100) -> RetryBudget:
    """A budget that has seen normal traffic, so it has retries to spend."""
    budget = RetryBudget()
    for _ in range(first_attempts):
        budget.record_first_attempt()
    return budget


class ReproduceProblem(unittest.TestCase):
    def test_one_failing_request_becomes_64_database_calls(self):
        database = ScriptedDatabase([])
        naive_stack(database).handle_user_request()
        self.assertEqual(database.calls_received, 64)

    def test_short_blip_becomes_a_permanent_outage(self):
        result = simulate(naive_stack)
        self.assertEqual(result.last_tick().succeeded, 0)
        self.assertEqual(result.last_tick().db_calls, 64 * REQUESTS_PER_TICK)

    def test_backlog_keeps_growing_after_capacity_returns(self):
        result = simulate(naive_stack)
        after_blip = [tick.backlog for tick in result.ticks[FIRST_TICK_AFTER_BLIP:]]
        self.assertEqual(after_blip, sorted(after_blip))
        self.assertGreater(after_blip[-1], after_blip[0])

    def test_retrying_at_one_layer_without_a_budget_still_does_not_recover(self):
        result = simulate(single_layer_stack)
        self.assertEqual(result.last_tick().succeeded, 0)
        self.assertEqual(result.last_tick().db_calls, 4 * REQUESTS_PER_TICK)


class FixRetryBudget(unittest.TestCase):
    def test_service_recovers_a_few_seconds_after_the_blip(self):
        result = simulate(budgeted_stack)
        self.assertEqual(result.ticks[17].succeeded, REQUESTS_PER_TICK)
        self.assertEqual(result.last_tick().backlog, 0)

    def test_database_load_stays_near_normal_during_the_blip(self):
        result = simulate(budgeted_stack)
        worst_tick_calls = max(tick.db_calls for tick in result.ticks)
        self.assertLessEqual(worst_tick_calls, 2 * REQUESTS_PER_TICK)

    def test_isolated_failure_is_still_retried_and_succeeds(self):
        database = ScriptedDatabase([False, True])
        stack = RequestStack(database, 0, 0, 3, warmed_budget())
        self.assertTrue(stack.handle_user_request().ok)
        self.assertEqual(database.calls_received, 2)

    def test_no_retries_before_any_first_attempt(self):
        self.assertFalse(RetryBudget().try_spend_retry())

    def test_retries_stop_when_budget_is_spent(self):
        budget = RetryBudget()
        for _ in range(20):
            budget.record_first_attempt()
        self.assertTrue(budget.try_spend_retry())
        self.assertTrue(budget.try_spend_retry())    # 2 retries = 10% of 20
        self.assertFalse(budget.try_spend_retry())   # a 3rd would exceed 10%

    def test_give_up_is_marked_do_not_retry(self):
        outcome = call_with_retries(ScriptedDatabase([]).execute, 3, warmed_budget())
        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.do_not_retry)

    def test_upper_layers_respect_do_not_retry_even_if_misconfigured(self):
        database = ScriptedDatabase([])
        empty_budget = RetryBudget()
        stack = RequestStack(database, 3, 3, 3, empty_budget)
        stack.handle_user_request()
        self.assertEqual(database.calls_received, 1)

    def test_fixed_stack_sends_at_most_four_calls_per_request(self):
        database = ScriptedDatabase([])
        stack = RequestStack(database, 0, 0, 3, warmed_budget(1000))
        stack.handle_user_request()
        self.assertEqual(database.calls_received, 4)


class DatabaseModel(unittest.TestCase):
    def test_abandoned_calls_use_capacity_in_the_next_tick(self):
        database = Database()
        database.start_tick(capacity=1)
        database.execute()
        self.assertFalse(database.execute().ok)       # queued, client timed out
        database.start_tick(capacity=1)
        self.assertFalse(database.execute().ok)       # capacity went to old work


if __name__ == "__main__":
    unittest.main()
