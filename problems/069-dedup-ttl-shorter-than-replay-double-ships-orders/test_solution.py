import unittest

from solution import (DEDUP_TTL_SECONDS, NO_OFFSET, SECONDS_PER_HOUR, FakeClock, Message,
                      OffsetMarkConsumer, TtlDedupConsumer, Warehouse, build_log,
                      consume_then_replay)

SECONDS_BETWEEN_ORDERS = 10
HOURS_OF_FAILED_COMMITS = 3
ORDERS = HOURS_OF_FAILED_COMMITS * SECONDS_PER_HOUR // SECONDS_BETWEEN_ORDERS   # 1,080


def replay_incident(make_consumer):
    """Three hours of orders, then a replay of the whole log; return the warehouse."""
    clock = FakeClock()
    warehouse = Warehouse()
    consumer = make_consumer(warehouse, clock)
    consume_then_replay(consumer, clock, build_log(0, ORDERS), SECONDS_BETWEEN_ORDERS)
    return warehouse


def ttl_consumer(warehouse, clock):
    return TtlDedupConsumer(warehouse, clock)


def offset_consumer(warehouse, clock):
    return OffsetMarkConsumer(warehouse)


class ReproduceProblem(unittest.TestCase):
    def test_replay_older_than_the_ttl_ships_721_orders_twice(self):
        warehouse = replay_incident(ttl_consumer)
        self.assertEqual(warehouse.duplicate_shipments(), 721)

    def test_quick_redelivery_inside_the_ttl_is_still_caught(self):
        clock = FakeClock()
        consumer = TtlDedupConsumer(Warehouse(), clock)
        message = Message(0, 0, "msg-0-0", "order-0-0")
        consumer.handle(message)
        clock.advance(DEDUP_TTL_SECONDS - 1)
        self.assertFalse(consumer.handle(message))

    def test_redelivery_just_after_the_ttl_ships_again(self):
        clock = FakeClock()
        consumer = TtlDedupConsumer(Warehouse(), clock)
        message = Message(0, 0, "msg-0-0", "order-0-0")
        consumer.handle(message)
        clock.advance(DEDUP_TTL_SECONDS)
        self.assertTrue(consumer.handle(message))


class FixOffsetMark(unittest.TestCase):
    def test_full_replay_ships_nothing_twice(self):
        warehouse = replay_incident(offset_consumer)
        self.assertEqual(warehouse.duplicate_shipments(), 0)
        self.assertEqual(len(warehouse.shipments), ORDERS)

    def test_new_message_after_replay_is_shipped(self):
        consumer = OffsetMarkConsumer(Warehouse())
        for message in build_log(0, 5):
            consumer.handle(message)
        self.assertTrue(consumer.handle(Message(0, 5, "msg-0-5", "order-0-5")))

    def test_partitions_have_independent_marks(self):
        consumer = OffsetMarkConsumer(Warehouse())
        for message in build_log(0, 100):
            consumer.handle(message)
        first_of_partition_one = build_log(1, 1)[0]
        self.assertTrue(consumer.handle(first_of_partition_one))

    def test_unprocessed_partition_starts_with_no_mark(self):
        self.assertEqual(OffsetMarkConsumer(Warehouse()).mark_for(7), NO_OFFSET)

    def test_state_is_one_number_per_partition(self):
        consumer = OffsetMarkConsumer(Warehouse())
        for partition in range(3):
            for message in build_log(partition, 500):
                consumer.handle(message)
        self.assertEqual(consumer.processed_up_to, {0: 499, 1: 499, 2: 499})


if __name__ == "__main__":
    unittest.main()
