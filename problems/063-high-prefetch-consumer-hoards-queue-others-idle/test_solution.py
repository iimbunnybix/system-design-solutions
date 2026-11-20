import unittest
from collections import deque

from solution import (BURST_JOBS, NAIVE_PREFETCH, SCALE_OUT_AT_SECOND, Consumer, bounded_run,
                      dispatch_with_credit, ideal_drain_seconds, make_burst, naive_run)

SEED = 7


class ReproduceProblem(unittest.TestCase):
    def test_first_consumer_hoards_a_full_prefetch_of_jobs(self):
        self.assertEqual(naive_run(make_burst(SEED)).peak_buffered_on_one, NAIVE_PREFETCH)

    def test_backlog_drains_three_times_slower_than_ideal_despite_scaling_out(self):
        burst = make_burst(SEED)
        result = naive_run(burst)
        self.assertEqual(result.drain_seconds, 578)
        self.assertGreater(result.drain_seconds, 3 * ideal_drain_seconds(burst))

    def test_new_consumers_sit_idle_while_jobs_wait_in_the_hoarders_buffer(self):
        self.assertEqual(naive_run(make_burst(SEED)).idle_while_work_waits, 3956)

    def test_hoarding_consumer_works_alone_until_the_end(self):
        result = naive_run(make_burst(SEED))
        self.assertEqual(result.busy_seconds_by_consumer["consumer-0"], result.drain_seconds)


class FixBoundedPrefetch(unittest.TestCase):
    def test_backlog_drains_close_to_the_ideal_time(self):
        burst = make_burst(SEED)
        result = bounded_run(burst)
        self.assertEqual(result.drain_seconds, 192)
        self.assertLess(result.drain_seconds, 1.1 * ideal_drain_seconds(burst))

    def test_no_consumer_is_idle_while_work_is_stranded(self):
        self.assertEqual(bounded_run(make_burst(SEED)).idle_while_work_waits, 0)

    def test_average_job_finishes_sooner(self):
        burst = make_burst(SEED)
        self.assertEqual(naive_run(burst).mean_completion_seconds, 169.4)
        self.assertEqual(bounded_run(burst).mean_completion_seconds, 108.6)

    def test_fix_beats_naive_for_every_seed(self):
        for seed in range(10):
            with self.subTest(seed=seed):
                burst = make_burst(seed)
                self.assertLess(bounded_run(burst).drain_seconds, naive_run(burst).drain_seconds)

    def test_every_job_is_processed_exactly_once(self):
        burst = make_burst(SEED)
        result = bounded_run(burst)
        self.assertEqual(result.jobs_done, BURST_JOBS)
        self.assertEqual(sum(result.busy_seconds_by_consumer.values()), sum(burst))

    def test_moderate_prefetch_still_strands_work(self):
        self.assertGreater(bounded_run(make_burst(SEED), prefetch=10).idle_while_work_waits, 0)


class DispatchWithCredit(unittest.TestCase):
    def test_messages_stay_in_the_queue_when_no_consumer_has_credit(self):
        queue = deque([1, 1, 1, 1, 1])
        consumers = [Consumer("a"), Consumer("b")]
        dispatch_with_credit(queue, consumers, prefetch=1)
        self.assertEqual(len(queue), 3)
        self.assertEqual([c.unacked() for c in consumers], [1, 1])

    def test_messages_are_handed_out_round_robin(self):
        queue = deque([1, 2, 3, 4])
        consumers = [Consumer("a"), Consumer("b")]
        dispatch_with_credit(queue, consumers, prefetch=10)
        self.assertEqual([list(c.buffer) for c in consumers], [[1, 3], [2, 4]])

    def test_zero_prefetch_is_rejected(self):
        with self.assertRaises(ValueError):
            dispatch_with_credit(deque([1]), [Consumer("a")], prefetch=0)


class EdgeCases(unittest.TestCase):
    def test_empty_backlog_drains_immediately(self):
        self.assertEqual(bounded_run([]).drain_seconds, 0)
        self.assertEqual(naive_run([]).drain_seconds, 0)

    def test_small_backlog_before_scale_out_is_the_same_either_way(self):
        burst = [1] * (SCALE_OUT_AT_SECOND - 5)
        self.assertEqual(naive_run(burst).drain_seconds, bounded_run(burst).drain_seconds)


if __name__ == "__main__":
    unittest.main()
