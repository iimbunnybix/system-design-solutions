import itertools
import unittest

from solution import (DeficitRoundRobinQueue, FifoQueue, Job, run_single_worker,
                      wait_ms)

BULK_JOBS = 2_000
JOB_COST_MS = 50
SMALL_TENANT_ARRIVAL_MS = 1_000


class JobFactory:
    """Hands out jobs with unique, increasing ids."""

    def __init__(self) -> None:
        self._ids = itertools.count(1)

    def make(self, tenant: str, cost_ms: int = JOB_COST_MS, at_ms: int = 0) -> Job:
        return Job(tenant, next(self._ids), cost_ms, at_ms)

    def many(self, tenant: str, count: int, cost_ms: int = JOB_COST_MS, at_ms: int = 0) -> list[Job]:
        return [self.make(tenant, cost_ms, at_ms) for _ in range(count)]


def bulk_upload_scenario() -> tuple[list[Job], list[Job]]:
    """One tenant queues 100 s of work at t=0; two others submit one job at t=1 s."""
    factory = JobFactory()
    bulk = factory.many("bulk-tenant", BULK_JOBS)
    small = [factory.make("tenant-a", at_ms=SMALL_TENANT_ARRIVAL_MS),
             factory.make("tenant-b", at_ms=SMALL_TENANT_ARRIVAL_MS)]
    return bulk, small


class ReproduceProblem(unittest.TestCase):
    def test_small_tenants_wait_behind_the_whole_bulk_upload(self):
        bulk, small = bulk_upload_scenario()
        finished = run_single_worker(FifoQueue(), bulk + small)
        waits = [wait_ms(job, finished) for job in small]
        # 2,000 x 50 ms = 100 s of queued work; they arrived at 1 s.
        self.assertEqual(waits, [99_000, 99_050])


class FixWithDeficitRoundRobin(unittest.TestCase):
    def test_small_tenants_run_almost_immediately(self):
        bulk, small = bulk_upload_scenario()
        finished = run_single_worker(DeficitRoundRobinQueue(), bulk + small)
        waits = [wait_ms(job, finished) for job in small]
        self.assertEqual(waits, [0, 50])

    def test_bulk_tenant_still_finishes_at_the_same_time(self):
        # Fairness must not waste worker time: total work is unchanged.
        bulk, small = bulk_upload_scenario()
        fifo = run_single_worker(FifoQueue(), bulk + small)
        fair = run_single_worker(DeficitRoundRobinQueue(), bulk + small)
        self.assertEqual(max(fifo.values()), max(fair.values()))
        self.assertEqual(max(fair[job.job_id] for job in bulk), 100_100)

    def test_worker_time_is_shared_equally_regardless_of_job_size(self):
        factory = JobFactory()
        big_jobs = factory.many("big", 100, cost_ms=500)
        small_jobs = factory.many("small", 1_000, cost_ms=50)
        finished = run_single_worker(DeficitRoundRobinQueue(quantum_ms=500), big_jobs + small_jobs)
        window_ms = 10_000
        big_time = sum(job.cost_ms for job in big_jobs if finished[job.job_id] <= window_ms)
        small_time = sum(job.cost_ms for job in small_jobs if finished[job.job_id] <= window_ms)
        self.assertEqual(big_time, 5_000)
        self.assertEqual(small_time, 5_000)

    def test_each_tenant_keeps_its_own_submission_order(self):
        factory = JobFactory()
        jobs = factory.many("t1", 5) + factory.many("t2", 5, cost_ms=120)
        queue = DeficitRoundRobinQueue()
        for job in jobs:
            queue.push(job)
        served = [queue.pop() for _ in range(len(jobs))]
        for tenant in ("t1", "t2"):
            ids = [job.job_id for job in served if job.tenant == tenant]
            self.assertEqual(ids, sorted(ids))

    def test_job_larger_than_the_quantum_runs_after_saving_up_budget(self):
        factory = JobFactory()
        huge = factory.make("huge", cost_ms=180)
        others = factory.many("steady", 10)
        queue = DeficitRoundRobinQueue(quantum_ms=50)
        for job in [huge] + others:
            queue.push(job)
        order = [queue.pop().tenant for _ in range(11)]
        # "huge" needs 4 visits (4 x 50 >= 180); "steady" runs once per visit.
        self.assertEqual(order.index("huge"), 3)

    def test_tenant_that_went_idle_does_not_keep_leftover_budget(self):
        factory = JobFactory()
        queue = DeficitRoundRobinQueue(quantum_ms=100)
        queue.push(factory.make("t1", cost_ms=10))
        self.assertEqual(queue.pop().tenant, "t1")
        queue.push(factory.make("t1", cost_ms=60))
        queue.push(factory.make("t1", cost_ms=60))
        queue.push(factory.make("t2", cost_ms=60))
        # With 90 ms saved from before, t1 would run both jobs first.
        self.assertEqual([queue.pop().tenant for _ in range(3)], ["t1", "t2", "t1"])

    def test_pop_from_empty_queue_raises(self):
        with self.assertRaises(IndexError):
            DeficitRoundRobinQueue().pop()

    def test_invalid_costs_and_quantum_are_rejected(self):
        with self.assertRaises(ValueError):
            DeficitRoundRobinQueue(quantum_ms=0)
        with self.assertRaises(ValueError):
            DeficitRoundRobinQueue().push(Job("t", 1, 0, 0))

    def test_idle_worker_jumps_to_next_arrival(self):
        factory = JobFactory()
        late = factory.make("t1", at_ms=5_000)
        finished = run_single_worker(DeficitRoundRobinQueue(), [late])
        self.assertEqual(finished[late.job_id], 5_050)
        self.assertEqual(wait_ms(late, finished), 0)


if __name__ == "__main__":
    unittest.main()
