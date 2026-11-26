import unittest
from collections import Counter

from solution import (CLAIM_TTL_SECONDS, CUSTOMERS, RUN_KEY, PaymentProvider, RunRegistry,
                      naive_run_on_every_replica, run_all_replicas, run_billing)


def charges_per_customer(provider: PaymentProvider) -> set[int]:
    """The distinct number of times customers were charged (ideally {1})."""
    return set(Counter(provider.charges).values())


class ReproduceProblem(unittest.TestCase):
    def test_three_replicas_charge_every_customer_three_times(self):
        provider = PaymentProvider()
        naive_run_on_every_replica(provider)
        self.assertEqual(len(provider.charges), 3 * CUSTOMERS)
        self.assertEqual(charges_per_customer(provider), {3})


class FixClaimTheRun(unittest.TestCase):
    def test_only_one_replica_runs_the_job(self):
        finished = run_all_replicas(RunRegistry(), PaymentProvider(), now=0)
        self.assertEqual(finished, ["billing-1"])

    def test_every_customer_is_charged_exactly_once(self):
        provider = PaymentProvider()
        run_all_replicas(RunRegistry(), provider, now=0)
        self.assertEqual(len(provider.charges), CUSTOMERS)
        self.assertEqual(charges_per_customer(provider), {1})

    def test_finished_run_is_never_claimed_again(self):
        registry = RunRegistry()
        run_all_replicas(registry, PaymentProvider(), now=0)
        self.assertFalse(registry.try_claim(RUN_KEY, "billing-2", now=10 * CLAIM_TTL_SECONDS))

    def test_live_claim_blocks_other_replicas(self):
        registry = RunRegistry()
        registry.try_claim(RUN_KEY, "billing-1", now=0)
        self.assertFalse(registry.try_claim(RUN_KEY, "billing-2", now=CLAIM_TTL_SECONDS - 1))

    def test_only_the_owner_can_mark_a_run_done(self):
        registry = RunRegistry()
        registry.try_claim(RUN_KEY, "billing-1", now=0)
        with self.assertRaises(RuntimeError):
            registry.mark_done(RUN_KEY, "billing-2")


class FixTakeoverAfterCrash(unittest.TestCase):
    def setUp(self):
        self.registry = RunRegistry()
        self.provider = PaymentProvider()
        run_billing("billing-1", self.registry, self.provider, now=0, crash_after=80)

    def test_crash_leaves_the_run_unfinished(self):
        self.assertEqual(len(self.provider.charges), 80)
        self.assertFalse(self.registry.claims[RUN_KEY].done)

    def test_nobody_takes_over_before_the_claim_expires(self):
        self.assertEqual(run_all_replicas(self.registry, self.provider, now=60), [])

    def test_takeover_finishes_without_charging_anyone_twice(self):
        finished = run_all_replicas(self.registry, self.provider, now=CLAIM_TTL_SECONDS)
        self.assertEqual(len(finished), 1)
        self.assertEqual(len(self.provider.charges), CUSTOMERS)
        self.assertEqual(charges_per_customer(self.provider), {1})


class FixIdempotentCharges(unittest.TestCase):
    def test_same_key_twice_charges_once(self):
        provider = PaymentProvider()
        provider.charge("cust-0001", idempotency_key="k")
        provider.charge("cust-0001", idempotency_key="k")
        self.assertEqual(provider.charges, ["cust-0001"])


if __name__ == "__main__":
    unittest.main()
