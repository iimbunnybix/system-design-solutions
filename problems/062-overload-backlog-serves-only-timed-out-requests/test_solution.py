import unittest

from solution import deadline_aware, max_queue_length, naive_fifo, steady_arrivals

SPIKE_END_MS = 10_000


def spike_then_normal():
    """10 s at ~167 requests/s (capacity is 100/s), then 20 s at 50 requests/s."""
    spike = steady_arrivals(0, SPIKE_END_MS, gap_ms=6)
    normal = steady_arrivals(SPIKE_END_MS, 20_000, gap_ms=20)
    return spike + normal


def normal_arrivals_answered_in_time(arrivals, service_ms=10, timeout_ms=1_000):
    """Replay the naive FIFO and count in-time answers for post-spike arrivals only."""
    worker_free_at = 0
    answered = 0
    for arrival in arrivals:
        worker_free_at = max(worker_free_at, arrival) + service_ms
        if arrival >= SPIKE_END_MS and worker_free_at - arrival <= timeout_ms:
            answered += 1
    return answered


class ReproduceProblem(unittest.TestCase):
    def test_most_work_is_answers_nobody_waits_for(self):
        outcome = naive_fifo(spike_then_normal())
        self.assertEqual(outcome.answered_in_time, 680)
        self.assertEqual(outcome.answered_too_late, 1987)

    def test_normal_traffic_after_the_spike_still_times_out(self):
        self.assertEqual(normal_arrivals_answered_in_time(spike_then_normal()), 432)


class FixBoundedDeadlineAwareQueue(unittest.TestCase):
    def test_goodput_triples_and_no_answer_is_late(self):
        outcome = deadline_aware(spike_then_normal())
        self.assertEqual(outcome.answered_in_time, 2098)
        self.assertEqual(outcome.answered_too_late, 0)

    def test_excess_requests_are_rejected_immediately(self):
        self.assertEqual(deadline_aware(spike_then_normal()).rejected_busy, 569)

    def test_every_request_is_accounted_for(self):
        arrivals = spike_then_normal()
        outcome = deadline_aware(arrivals)
        total = outcome.answered_in_time + outcome.rejected_busy + outcome.dropped_expired
        self.assertEqual(total, len(arrivals))

    def test_traffic_under_capacity_is_all_answered(self):
        outcome = deadline_aware(steady_arrivals(0, 5_000, gap_ms=20))
        self.assertEqual((outcome.answered_in_time, outcome.rejected_busy), (250, 0))


class FixDeadlineCheck(unittest.TestCase):
    def test_too_long_queue_drops_expired_requests_instead_of_answering_late(self):
        outcome = deadline_aware(spike_then_normal(), queue_limit=10_000)
        self.assertGreater(outcome.dropped_expired, 0)
        self.assertEqual(outcome.answered_too_late, 0)

    def test_queue_limit_fits_inside_the_timeout(self):
        self.assertEqual(max_queue_length(service_ms=10, timeout_ms=1_000), 98)

    def test_timeout_shorter_than_two_services_allows_no_queue(self):
        self.assertEqual(max_queue_length(service_ms=10, timeout_ms=15), 0)


if __name__ == "__main__":
    unittest.main()
