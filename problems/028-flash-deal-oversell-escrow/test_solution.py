import random
import threading
import unittest

from solution import (DEAL_UNITS, REGIONS, EscrowCoordinator, RegionAllocation,
                      simulate_escrow, simulate_replicated_check)


def drain(coordinator: EscrowCoordinator, region_name: str, attempts: int) -> int:
    """Try to sell ``attempts`` units in one region; return how many sold."""
    sold = 0
    for _ in range(attempts):
        if coordinator.sell(region_name):
            sold += 1
    return sold


class ReproduceProblem(unittest.TestCase):
    def test_half_second_replication_lag_oversells_by_250(self):
        sold = simulate_replicated_check(lag_ticks=5)
        self.assertEqual(sum(sold.values()), DEAL_UNITS + 250)

    def test_more_lag_means_more_oversold(self):
        oversold = [sum(simulate_replicated_check(lag).values()) - DEAL_UNITS
                    for lag in (2, 3, 5, 10)]
        self.assertEqual(oversold, [80, 140, 250, 540])

    def test_zero_lag_is_rejected_by_the_model(self):
        with self.assertRaises(ValueError):
            simulate_replicated_check(lag_ticks=0)


class FixEscrowAllocation(unittest.TestCase):
    def test_sells_exactly_the_deal_size(self):
        coordinator = simulate_escrow()
        self.assertEqual(coordinator.total_sold(), DEAL_UNITS)

    def test_units_are_sold_where_the_demand_is(self):
        coordinator = simulate_escrow()
        sold = {name: region.sold for name, region in coordinator.regions.items()}
        self.assertEqual(sold, {"eu": 300, "us": 600, "ap": 100})

    def test_coordinator_is_called_rarely(self):
        coordinator = simulate_escrow()
        self.assertEqual(coordinator.refill_requests, 14)

    def test_static_split_without_rebalancing_strands_units(self):
        coordinator = simulate_escrow(rebalance=False)
        self.assertEqual(coordinator.total_sold(), 967)
        self.assertEqual(coordinator.regions["ap"].unsold(), 33)

    def test_initial_split_keeps_every_unit(self):
        coordinator = EscrowCoordinator(1000, REGIONS)
        shares = [region.unsold() for region in coordinator.regions.values()]
        self.assertEqual(shares, [334, 333, 333])

    def test_one_region_can_sell_the_whole_deal(self):
        coordinator = EscrowCoordinator(DEAL_UNITS, REGIONS)
        self.assertEqual(drain(coordinator, "us", 5000), DEAL_UNITS)

    def test_after_sell_out_coordinator_is_not_called_again(self):
        coordinator = EscrowCoordinator(10, REGIONS)
        drain(coordinator, "eu", 10)
        requests_at_sell_out = coordinator.refill_requests
        self.assertEqual(drain(coordinator, "eu", 100), 0)
        self.assertTrue(coordinator.sold_out)
        self.assertLessEqual(coordinator.refill_requests, requests_at_sell_out + 1)

    def test_last_unit_in_another_region_is_reclaimed(self):
        coordinator = EscrowCoordinator(3, REGIONS)     # one unit per region
        self.assertEqual(drain(coordinator, "ap", 10), 3)

    def test_random_interleaving_never_oversells(self):
        rng = random.Random(28)
        for _ in range(50):
            deal_units = rng.randint(1, 200)
            coordinator = EscrowCoordinator(deal_units, REGIONS)
            for _ in range(400):
                coordinator.sell(rng.choice(REGIONS))
            self.assertEqual(coordinator.total_sold(), deal_units)
            self.assertEqual(coordinator.units_accounted_for(), deal_units)

    def test_concurrent_buyers_in_all_regions_sell_exactly_the_total(self):
        coordinator = EscrowCoordinator(DEAL_UNITS, REGIONS)
        start = threading.Barrier(6)

        def buyer(region_name: str) -> None:
            start.wait()
            drain(coordinator, region_name, 600)

        threads = [threading.Thread(target=buyer, args=(name,)) for name in REGIONS * 2]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(coordinator.total_sold(), DEAL_UNITS)
        self.assertEqual(coordinator.units_accounted_for(), DEAL_UNITS)


class RegionAllocationBehaviour(unittest.TestCase):
    def test_release_half_rounds_up_so_the_last_unit_can_move(self):
        region = RegionAllocation("eu", 1)
        self.assertEqual(region.release_half(), 1)
        self.assertEqual(region.unsold(), 0)

    def test_empty_allocation_refuses_to_sell(self):
        self.assertFalse(RegionAllocation("eu", 0).try_take_local())


if __name__ == "__main__":
    unittest.main()
