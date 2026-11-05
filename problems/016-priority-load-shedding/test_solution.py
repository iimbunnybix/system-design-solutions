import unittest

from solution import NaiveGateway, Priority, PriorityGateway, simulate

CAPACITY = 100
TICKS = 200
SEED = 7

# Flash sale: 200 requests per tick against a gateway that can serve 100.
OVERLOAD_MIX = {
    Priority.CRITICAL: 30,
    Priority.DEFAULT: 70,
    Priority.BACKGROUND: 100,
}

# Normal day: 60 requests per tick, below every ceiling.
NORMAL_MIX = {
    Priority.CRITICAL: 10,
    Priority.DEFAULT: 30,
    Priority.BACKGROUND: 20,
}


def run(gateway, mix):
    """Simulate the given traffic mix with a fixed seed."""
    return simulate(gateway, mix, ticks=TICKS, seed=SEED)


class ReproduceProblem(unittest.TestCase):
    def test_overload_sheds_checkout_as_often_as_analytics(self):
        rates = run(NaiveGateway(CAPACITY), OVERLOAD_MIX)
        # Every class gets roughly the same ~50% success rate.
        self.assertLess(rates[Priority.CRITICAL], 0.6)
        self.assertAlmostEqual(rates[Priority.CRITICAL], rates[Priority.BACKGROUND], delta=0.05)


class FixWithPriorityAdmission(unittest.TestCase):
    def test_checkout_is_never_shed_while_within_reserved_capacity(self):
        rates = run(PriorityGateway(CAPACITY), OVERLOAD_MIX)
        self.assertEqual(rates[Priority.CRITICAL], 1.0)

    def test_background_is_shed_before_default(self):
        rates = run(PriorityGateway(CAPACITY), OVERLOAD_MIX)
        self.assertLess(rates[Priority.BACKGROUND], rates[Priority.DEFAULT])

    def test_gateway_still_does_useful_work_for_lower_priorities(self):
        rates = run(PriorityGateway(CAPACITY), OVERLOAD_MIX)
        self.assertGreater(rates[Priority.DEFAULT], 0.0)
        self.assertGreater(rates[Priority.BACKGROUND], 0.0)

    def test_nothing_is_shed_under_normal_load(self):
        rates = run(PriorityGateway(CAPACITY), NORMAL_MIX)
        self.assertEqual(set(rates.values()), {1.0})

    def test_background_is_capped_even_with_spare_capacity(self):
        # 80 requests fit in 100 slots, but background may only fill 60.
        gateway = PriorityGateway(CAPACITY)
        admitted = 0
        for _ in range(80):
            if gateway.try_admit(Priority.BACKGROUND):
                admitted += 1
        self.assertEqual(admitted, 60)

    def test_critical_can_use_the_full_capacity(self):
        gateway = PriorityGateway(CAPACITY)
        for _ in range(CAPACITY):
            self.assertTrue(gateway.try_admit(Priority.CRITICAL))
        self.assertFalse(gateway.try_admit(Priority.CRITICAL))

    def test_released_slot_can_be_reused(self):
        gateway = PriorityGateway(1)
        self.assertTrue(gateway.try_admit(Priority.CRITICAL))
        self.assertFalse(gateway.try_admit(Priority.CRITICAL))
        gateway.release()
        self.assertTrue(gateway.try_admit(Priority.CRITICAL))

    def test_release_without_admit_is_an_error(self):
        with self.assertRaises(RuntimeError):
            PriorityGateway(CAPACITY).release()

    def test_capacity_must_be_positive(self):
        with self.assertRaises(ValueError):
            PriorityGateway(0)


if __name__ == "__main__":
    unittest.main()
