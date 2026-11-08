import os
import subprocess
import sys
import unittest

from solution import (PARTITION_COUNT, Event, OrderStore, RoundRobinPartitioner,
                      generate_hot_key_events, generate_order_events, keyed_run,
                      naive_run, per_key_order_violations, stable_partition)

ORDERS = 2_000
WINDOW_MS = 60_000
SEED = 7
HOT_EVENTS = 6_000
HOT_SHARE = 0.4
HOT_WINDOW_MS = 20_000


def order_events() -> list[Event]:
    """The standard workload: 2,000 orders, each created -> paid -> shipped."""
    return generate_order_events(ORDERS, WINDOW_MS, SEED)


def builtin_hash_in_new_process(key: str, hash_seed: str) -> int:
    """Run Python's hash() in a fresh interpreter with the given hash seed."""
    env = dict(os.environ, PYTHONHASHSEED=hash_seed)
    output = subprocess.run([sys.executable, "-c", f"print(hash({key!r}))"],
                            capture_output=True, text=True, env=env, check=True)
    return int(output.stdout)


class ReproduceProblem(unittest.TestCase):
    def test_round_robin_applies_events_out_of_order(self):
        result = naive_run(order_events())
        self.assertEqual(result.store.out_of_order, 193)

    def test_round_robin_leaves_shipped_orders_in_the_wrong_state(self):
        result = naive_run(order_events())
        self.assertEqual(result.store.wrong_final_states(), 102)

    def test_round_robin_sends_one_orders_events_to_different_partitions(self):
        partitioner = RoundRobinPartitioner()
        partitions = [partitioner("order-1", PARTITION_COUNT) for _ in range(3)]
        self.assertEqual(partitions, [0, 1, 2])


class FixWithKeyedPartitions(unittest.TestCase):
    def test_every_order_ends_shipped(self):
        result = keyed_run(order_events())
        self.assertEqual(result.store.wrong_final_states(), 0)
        self.assertEqual(result.store.out_of_order, 0)

    def test_events_of_each_key_are_applied_in_publish_order(self):
        for seed in range(5):
            events = generate_order_events(ORDERS, WINDOW_MS, seed)
            result = keyed_run(events, seed=seed)
            self.assertEqual(per_key_order_violations(result.applied), 0, seed)

    def test_all_events_of_one_order_share_a_partition(self):
        result = keyed_run(order_events())
        partitions_by_order: dict[str, set[int]] = {}
        for item in result.applied:
            partitions_by_order.setdefault(item.event.order_id, set()).add(item.partition)
        for partitions in partitions_by_order.values():
            self.assertEqual(len(partitions), 1)

    def test_different_orders_still_spread_over_all_partitions(self):
        result = keyed_run(order_events())
        self.assertEqual(len(result.queue_lengths), PARTITION_COUNT)
        for length in result.queue_lengths:
            self.assertGreater(length, 0.9 * 3 * ORDERS / PARTITION_COUNT)

    def test_stable_partition_is_the_same_in_every_process(self):
        self.assertEqual(stable_partition("order-42", 4), stable_partition("order-42", 4))
        self.assertEqual(stable_partition("order-42", 1), 0)

    def test_builtin_hash_differs_between_processes(self):
        first = builtin_hash_in_new_process("order-42", "1")
        second = builtin_hash_in_new_process("order-42", "2")
        self.assertNotEqual(first, second)

    def test_store_flags_an_event_older_than_the_current_state(self):
        store = OrderStore()
        store.apply(Event("o1", "shipped", 0, 1))
        store.apply(Event("o1", "paid", 0, 0))
        self.assertEqual((store.out_of_order, store.status["o1"]), (1, "paid"))

    def test_empty_event_stream_is_handled(self):
        result = keyed_run([])
        self.assertEqual((result.applied, result.finished_at_ms), ([], 0))


class HotKeyTradeOff(unittest.TestCase):
    def test_hot_key_overloads_its_partition(self):
        events = generate_hot_key_events(HOT_EVENTS, HOT_SHARE, HOT_WINDOW_MS, 3)
        keyed = keyed_run(events)
        self.assertEqual(max(keyed.queue_lengths), 3_372)
        self.assertEqual(keyed.max_lag_ms, 2_077)

    def test_round_robin_spreads_the_same_hot_traffic_evenly(self):
        events = generate_hot_key_events(HOT_EVENTS, HOT_SHARE, HOT_WINDOW_MS, 3)
        spread = naive_run(events)
        self.assertEqual(spread.queue_lengths, [1_500] * PARTITION_COUNT)
        self.assertEqual(spread.max_lag_ms, 12)


if __name__ == "__main__":
    unittest.main()
