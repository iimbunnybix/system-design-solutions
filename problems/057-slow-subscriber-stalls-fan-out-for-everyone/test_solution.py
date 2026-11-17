import unittest

from solution import (DROP_OLDEST, Subscriber, isolated_broadcast, naive_broadcast,
                      worst_latency)

MESSAGES = 100
SLOW_TICKS = 10  # the congested client takes 10 ticks per message


def audience(slow_policy: str = "disconnect") -> list[Subscriber]:
    """Two healthy clients and one on a congested mobile link."""
    return [Subscriber("fast-1", 1), Subscriber("fast-2", 1),
            Subscriber("slow", SLOW_TICKS, overflow_policy=slow_policy)]


class ReproduceProblem(unittest.TestCase):
    def test_fast_subscriber_waits_behind_the_slow_one(self):
        subscribers = audience()
        naive_broadcast(MESSAGES, subscribers)
        self.assertEqual(worst_latency(subscribers[0]), 1090)

    def test_delay_keeps_growing_with_every_message(self):
        subscribers = audience()
        naive_broadcast(MESSAGES, subscribers)
        fast = subscribers[0]
        first_delay = fast.delivered[0][1] - fast.delivered[0][0]
        last_delay = fast.delivered[-1][1] - fast.delivered[-1][0]
        self.assertGreater(last_delay, 100 * first_delay)


class FixIsolatesSubscribers(unittest.TestCase):
    def test_fast_subscribers_get_every_message_within_one_tick(self):
        subscribers = audience()
        isolated_broadcast(MESSAGES, subscribers)
        for fast in subscribers[:2]:
            self.assertEqual(len(fast.delivered), MESSAGES)
            self.assertEqual(worst_latency(fast), 1)

    def test_slow_subscriber_that_needs_every_message_is_disconnected(self):
        subscribers = audience()
        isolated_broadcast(MESSAGES, subscribers)
        slow = subscribers[2]
        self.assertEqual(slow.disconnected_at, 9)
        self.assertEqual(len(slow.delivered), 1)

    def test_queue_never_grows_beyond_its_limit(self):
        slow = Subscriber("slow", SLOW_TICKS, queue_limit=4, overflow_policy=DROP_OLDEST)
        isolated_broadcast(MESSAGES, [slow])
        self.assertLessEqual(len(slow.queue), 4)

    def test_drop_oldest_keeps_latency_bounded_by_the_queue(self):
        subscribers = audience(slow_policy=DROP_OLDEST)
        isolated_broadcast(MESSAGES, subscribers, extra_ticks=100)
        slow = subscribers[2]
        # At most queue_limit messages ahead of it, each taking SLOW_TICKS.
        self.assertLessEqual(worst_latency(slow), slow.queue_limit * SLOW_TICKS + 1)
        self.assertEqual(slow.delivered[-1][0], MESSAGES - 1)

    def test_drop_oldest_counts_what_it_discarded(self):
        subscribers = audience(slow_policy=DROP_OLDEST)
        isolated_broadcast(MESSAGES, subscribers, extra_ticks=100)
        slow = subscribers[2]
        self.assertEqual(len(slow.delivered) + slow.dropped, MESSAGES)

    def test_subscribers_that_keep_up_never_drop(self):
        steady = Subscriber("steady", 1, queue_limit=1)
        isolated_broadcast(MESSAGES, [steady])
        self.assertEqual(steady.dropped, 0)

    def test_unknown_policy_is_rejected(self):
        with self.assertRaises(ValueError):
            isolated_broadcast(1, [Subscriber("x", 1, overflow_policy="block")])

    def test_zero_queue_limit_is_rejected(self):
        with self.assertRaises(ValueError):
            isolated_broadcast(1, [Subscriber("x", 1, queue_limit=0)])


if __name__ == "__main__":
    unittest.main()
