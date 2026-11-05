import math
import unittest

from solution import (HOT_TENANT, LOAD_FACTOR, WORKER_SLOTS, WORKERS, BoundedLoadRouter, HashRing,
                      NaiveRouter, simulate)


def bounded_router(workers=WORKERS, load_factor=LOAD_FACTOR):
    return BoundedLoadRouter(HashRing(workers), load_factor)


class ReproduceProblem(unittest.TestCase):
    def test_hot_tenant_overloads_its_one_home_worker(self):
        result = simulate(NaiveRouter)
        self.assertEqual(result["hot_tenant_workers"], 1)
        self.assertEqual(result["max_load"], 464)           # 150 slots per worker
        self.assertEqual(result["over_capacity"], 314)

    def test_other_workers_sit_far_below_capacity(self):
        self.assertLess(simulate(NaiveRouter)["min_load"], WORKER_SLOTS // 2)

    def test_adding_workers_does_not_help_the_hot_tenant(self):
        router = NaiveRouter(HashRing(WORKERS + [f"worker-{i}" for i in range(10, 20)]))
        for _ in range(400):
            router.assign(HOT_TENANT)
        self.assertEqual(max(router.load.values()), 400)


class FixBoundedLoads(unittest.TestCase):
    def test_no_worker_exceeds_c_times_the_average(self):
        result = simulate(BoundedLoadRouter)
        self.assertEqual(result["max_load"], 125)           # ceil(1.25 x 1,000 / 10)
        self.assertEqual(result["over_capacity"], 0)

    def test_hot_tenant_spills_over_to_several_workers(self):
        self.assertEqual(simulate(BoundedLoadRouter)["hot_tenant_workers"], 8)

    def test_most_other_tenants_keep_their_home_worker(self):
        self.assertGreater(simulate(BoundedLoadRouter)["other_tenants_at_home"], 0.79)

    def test_light_load_stays_on_the_home_worker(self):
        router = bounded_router()
        home = router.ring.home("tenant-7")
        self.assertEqual(router.assign("tenant-7"), home)

    def test_overflow_goes_to_the_next_worker_clockwise(self):
        router = bounded_router()
        first, second = list(router.ring.walk(HOT_TENANT))[:2]
        placed = [router.assign(HOT_TENANT) for _ in range(5)]
        # One worker in flight -> bound ceil(1.25 x 2 / 10) = 1, so each
        # further query moves one step around the ring.
        self.assertEqual(placed[:2], [first, second])
        self.assertEqual(len(set(placed)), 5)

    def test_released_capacity_is_reused_by_the_home_worker(self):
        router = bounded_router(workers=["w1", "w2"])
        home = router.ring.home("t")
        for _ in range(8):
            router.assign("t")
        self.assertEqual(router.load[home], math.ceil(LOAD_FACTOR * 8 / 2))
        other = [w for w in router.load if w != home][0]
        router.release(other)
        router.release(home)
        self.assertEqual(router.assign("t"), home)

    def test_every_query_finds_a_worker_even_with_one_hot_key(self):
        router = bounded_router(load_factor=1.01)
        for _ in range(1_000):
            router.assign(HOT_TENANT)
        self.assertEqual(sum(router.load.values()), 1_000)
        self.assertLessEqual(max(router.load.values()), math.ceil(1.01 * 1_000 / 10))

    def test_single_worker_takes_everything(self):
        router = bounded_router(workers=["only"])
        for tenant in ("a", "b", "c"):
            self.assertEqual(router.assign(tenant), "only")

    def test_load_factor_of_one_or_less_is_rejected(self):
        with self.assertRaises(ValueError):
            bounded_router(load_factor=1.0)

    def test_walk_visits_each_worker_once(self):
        ring = HashRing(WORKERS)
        self.assertEqual(sorted(ring.walk("tenant-3")), sorted(WORKERS))


if __name__ == "__main__":
    unittest.main()
