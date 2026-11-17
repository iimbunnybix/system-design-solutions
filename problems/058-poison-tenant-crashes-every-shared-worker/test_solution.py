import unittest

from solution import (Fleet, poison_attack_naive, poison_attack_sharded, possible_shards,
                      share_of_tenants_down, shuffle_shard, tenant_is_served, worker_names)

POISON_TENANT = "tenant-poison"
CLIENT_RETRIES = 20


def other_tenants() -> list[str]:
    return [f"tenant-{number}" for number in range(1000)]


class ReproduceProblem(unittest.TestCase):
    def test_retries_of_one_poison_request_crash_every_worker(self):
        workers = worker_names()
        fleet = Fleet(workers)
        poison_attack_naive(fleet, workers, CLIENT_RETRIES)
        self.assertEqual(fleet.alive, set())

    def test_every_tenant_loses_service(self):
        workers = worker_names()
        fleet = Fleet(workers)
        poison_attack_naive(fleet, workers, CLIENT_RETRIES)
        self.assertEqual(share_of_tenants_down(fleet, other_tenants(), workers, use_shards=False), 1.0)


class FixShuffleSharding(unittest.TestCase):
    def test_poison_tenant_only_crashes_its_own_two_workers(self):
        workers = worker_names()
        fleet = Fleet(workers)
        poison_attack_sharded(fleet, POISON_TENANT, workers, CLIENT_RETRIES)
        self.assertEqual(len(fleet.alive), 6)
        self.assertEqual(fleet.alive, set(workers) - set(shuffle_shard(POISON_TENANT, workers)))

    def test_only_tenants_with_the_exact_same_shard_go_down(self):
        workers = worker_names()
        fleet = Fleet(workers)
        poison_attack_sharded(fleet, POISON_TENANT, workers, CLIENT_RETRIES)
        share_down = share_of_tenants_down(fleet, other_tenants(), workers, use_shards=True)
        self.assertEqual(share_down, 0.028)

    def test_expected_share_down_is_one_in_possible_shards(self):
        self.assertEqual(possible_shards(), 28)

    def test_tenant_sharing_one_worker_is_still_served_by_the_other(self):
        workers = worker_names()
        fleet = Fleet(workers)
        poison_attack_sharded(fleet, POISON_TENANT, workers, CLIENT_RETRIES)
        poison_shard = set(shuffle_shard(POISON_TENANT, workers))
        for tenant in other_tenants():
            if len(set(shuffle_shard(tenant, workers)) & poison_shard) == 1:
                self.assertTrue(tenant_is_served(fleet, tenant, workers), tenant)

    def test_shard_is_stable_for_a_tenant(self):
        workers = worker_names()
        self.assertEqual(shuffle_shard("tenant-42", workers), shuffle_shard("tenant-42", workers))

    def test_shard_has_distinct_workers(self):
        shard = shuffle_shard("tenant-7", worker_names())
        self.assertEqual(len(set(shard)), 2)

    def test_tenants_are_spread_over_many_different_shards(self):
        workers = worker_names()
        shards = {tuple(shuffle_shard(tenant, workers)) for tenant in other_tenants()}
        self.assertEqual(len(shards), 28)

    def test_invalid_shard_size_is_rejected(self):
        for size in (0, 9):
            with self.subTest(size=size), self.assertRaises(ValueError):
                shuffle_shard("tenant-1", worker_names(), workers_per_tenant=size)


if __name__ == "__main__":
    unittest.main()
