import unittest
from collections import deque

from solution import (BUFFER_SLOTS, CONSUMER_RATE_PER_TICK, EVENT_BYTES, MEMORY_LIMIT_BYTES,
                      CreditedConsumer, CreditedProducer, DurableLog, process,
                      simulate_credit_flow, simulate_unbounded_push, total_arrivals)


def log_with(events: int) -> DurableLog:
    """A durable log holding ``events`` events that arrived at tick 0."""
    log = DurableLog()
    log.append(0, events)
    return log


class ReproduceProblem(unittest.TestCase):
    def test_unbounded_buffer_is_oom_killed_16_seconds_into_the_sale(self):
        result = simulate_unbounded_push()
        self.assertEqual(result["crashed_at_s"], 16.2)

    def test_crash_loses_every_event_held_in_memory(self):
        result = simulate_unbounded_push()
        self.assertEqual(result["lost_events"], 16_400)
        self.assertGreater(result["lost_events"] * EVENT_BYTES, MEMORY_LIMIT_BYTES)

    def test_latency_grew_to_seconds_before_the_crash(self):
        self.assertEqual(simulate_unbounded_push()["max_latency_s"], 8.1)


class FixCreditBasedFlowControl(unittest.TestCase):
    def test_every_event_is_delivered_and_none_is_left_behind(self):
        result = simulate_credit_flow()
        self.assertEqual(result["delivered"], total_arrivals())
        self.assertEqual(result["left_in_log"], 0)

    def test_consumer_memory_never_exceeds_the_credit_window(self):
        result = simulate_credit_flow()
        self.assertEqual(result["peak_memory_bytes"], BUFFER_SLOTS * EVENT_BYTES)

    def test_burst_becomes_lag_in_the_durable_log_that_drains_later(self):
        result = simulate_credit_flow()
        self.assertEqual(result["peak_log_backlog"], 29_900)
        self.assertEqual(result["caught_up_at_s"], 89.7)
        self.assertEqual(result["max_latency_s"], 30.0)

    def test_consumer_never_grants_more_credits_than_free_slots(self):
        consumer = CreditedConsumer(slots=10)
        self.assertEqual(consumer.grant_credits(), 10)
        self.assertEqual(consumer.grant_credits(), 0)        # already promised

    def test_producer_sends_only_as_many_events_as_it_has_credits(self):
        consumer = CreditedConsumer(slots=10)
        producer = CreditedProducer()
        log = log_with(25)
        producer.add_credits(consumer.grant_credits())
        self.assertEqual(producer.send(log, consumer), 10)
        self.assertEqual(len(log.pending), 15)
        self.assertEqual(producer.send(log, consumer), 0)

    def test_processing_frees_slots_for_new_credits(self):
        consumer = CreditedConsumer(slots=BUFFER_SLOTS)
        producer = CreditedProducer()
        log = log_with(1_000)
        producer.add_credits(consumer.grant_credits())
        producer.send(log, consumer)
        process(consumer.buffer, tick=1, latencies=[])
        self.assertEqual(consumer.grant_credits(), CONSUMER_RATE_PER_TICK)

    def test_event_sent_without_credit_is_refused(self):
        consumer = CreditedConsumer(slots=10)
        with self.assertRaises(RuntimeError):
            consumer.receive(0)

    def test_process_records_latency_in_seconds(self):
        latencies: list[float] = []
        process(deque([0, 5]), tick=10, latencies=latencies)
        self.assertEqual(latencies, [1.0, 0.5])


if __name__ == "__main__":
    unittest.main()
