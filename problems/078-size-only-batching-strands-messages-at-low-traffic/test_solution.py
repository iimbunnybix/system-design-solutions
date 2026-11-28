import unittest

from solution import (BATCH_SIZE, LINGER_SECONDS, BatchingProducer, FakeBroker, FakeClock,
                      OriginalBatchingProducer, delivery_delays, run_traffic)

NIGHT_GAP_SECONDS = 20.0          # 3 messages a minute
NIGHT_MESSAGES = 100
DAY_GAP_SECONDS = 0.001           # 1,000 messages a second


def setup(producer_class):
    clock = FakeClock()
    broker = FakeBroker(clock)
    return clock, broker, producer_class(broker, clock)


class ReproduceProblem(unittest.TestCase):
    def test_night_traffic_is_never_delivered(self):
        clock, broker, producer = setup(OriginalBatchingProducer)
        run_traffic(producer, clock, NIGHT_MESSAGES, NIGHT_GAP_SECONDS)
        self.assertEqual(broker.delivered, [])
        self.assertEqual(len(producer.pending), NIGHT_MESSAGES)

    def test_restart_loses_everything_waiting(self):
        clock, broker, producer = setup(OriginalBatchingProducer)
        run_traffic(producer, clock, NIGHT_MESSAGES, NIGHT_GAP_SECONDS)
        producer.close()
        self.assertEqual(broker.delivered, [])

    def test_first_message_of_a_full_batch_waits_hours_at_night(self):
        clock, broker, producer = setup(OriginalBatchingProducer)
        sent_at = run_traffic(producer, clock, BATCH_SIZE, NIGHT_GAP_SECONDS)
        self.assertEqual(max(delivery_delays(sent_at, broker)), (BATCH_SIZE - 1) * NIGHT_GAP_SECONDS)


class FixLingerFlush(unittest.TestCase):
    def test_every_night_message_arrives_within_linger_plus_one_tick(self):
        clock, broker, producer = setup(BatchingProducer)
        sent_at = run_traffic(producer, clock, NIGHT_MESSAGES, NIGHT_GAP_SECONDS)
        delays = delivery_delays(sent_at, broker)
        self.assertEqual(len(delays), NIGHT_MESSAGES)
        self.assertLessEqual(max(delays), LINGER_SECONDS + 0.5)

    def test_daytime_traffic_still_goes_in_full_batches(self):
        clock, broker, producer = setup(BatchingProducer)
        run_traffic(producer, clock, 5 * BATCH_SIZE, DAY_GAP_SECONDS)
        self.assertEqual(broker.batches_received, 5)
        self.assertEqual(len(broker.delivered), 5 * BATCH_SIZE)

    def test_close_flushes_pending_messages(self):
        clock, broker, producer = setup(BatchingProducer)
        producer.send("password-reset-1")
        producer.close()
        self.assertEqual([message for message, _ in broker.delivered], ["password-reset-1"])

    def test_linger_counts_from_the_oldest_message(self):
        clock, broker, producer = setup(BatchingProducer)
        producer.send("first")
        clock.advance(1.5)
        producer.send("second")
        clock.advance(0.5)
        producer.tick()
        self.assertEqual(len(broker.delivered), 2)

    def test_tick_with_nothing_pending_sends_no_empty_batch(self):
        clock, broker, producer = setup(BatchingProducer)
        clock.advance(10)
        producer.tick()
        self.assertEqual(broker.batches_received, 0)

    def test_message_order_is_preserved(self):
        clock, broker, producer = setup(BatchingProducer)
        run_traffic(producer, clock, 1200, 0.01)
        producer.close()
        delivered = [message for message, _ in broker.delivered]
        self.assertEqual(delivered, [f"event-{number}" for number in range(1200)])


if __name__ == "__main__":
    unittest.main()
