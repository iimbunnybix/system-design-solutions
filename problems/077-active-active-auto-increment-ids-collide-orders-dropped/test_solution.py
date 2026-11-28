import unittest

from solution import OrderNode, check_sequence_config, lost_orders, run_concurrent_traffic

ROUNDS = 100
SEQUENCE_STEP = 10              # leaves room for up to 10 regions


def original_pair() -> tuple[OrderNode, OrderNode]:
    return OrderNode("east"), OrderNode("west")


def interleaved_pair() -> tuple[OrderNode, OrderNode]:
    return (OrderNode("east", increment=SEQUENCE_STEP, offset=1),
            OrderNode("west", increment=SEQUENCE_STEP, offset=2))


class ReproduceProblem(unittest.TestCase):
    def test_both_regions_hand_out_the_same_ids(self):
        east, west = original_pair()
        self.assertEqual(east.insert_local("a"), west.insert_local("b"))

    def test_every_other_regions_order_is_dropped_by_replication(self):
        east, west = original_pair()
        placed = run_concurrent_traffic(east, west, ROUNDS)
        self.assertEqual(east.skipped_duplicates, ROUNDS)
        self.assertEqual(len(lost_orders(placed, east)), ROUNDS)
        self.assertEqual(len(lost_orders(placed, west)), ROUNDS)

    def test_regions_disagree_about_what_an_order_id_means(self):
        east, west = original_pair()
        run_concurrent_traffic(east, west, 1)
        self.assertEqual(east.orders[1], "east-order-0")
        self.assertEqual(west.orders[1], "west-order-0")

    def test_config_check_flags_the_original_settings(self):
        self.assertEqual(len(check_sequence_config(list(original_pair()))), 2)


class FixInterleavedSequences(unittest.TestCase):
    def test_no_order_is_lost_on_either_region(self):
        east, west = interleaved_pair()
        placed = run_concurrent_traffic(east, west, ROUNDS)
        self.assertEqual(lost_orders(placed, east), [])
        self.assertEqual(lost_orders(placed, west), [])
        self.assertEqual(east.skipped_duplicates + west.skipped_duplicates, 0)

    def test_both_regions_end_with_identical_tables(self):
        east, west = interleaved_pair()
        run_concurrent_traffic(east, west, ROUNDS)
        self.assertEqual(east.orders, west.orders)
        self.assertEqual(len(east.orders), 2 * ROUNDS)

    def test_each_region_only_uses_its_own_offset(self):
        east, west = interleaved_pair()
        run_concurrent_traffic(east, west, ROUNDS)
        for order_id, description in east.orders.items():
            expected_offset = 1 if description.startswith("east") else 2
            self.assertEqual(order_id % SEQUENCE_STEP, expected_offset)

    def test_next_id_moves_above_replicated_rows(self):
        east = OrderNode("east", increment=SEQUENCE_STEP, offset=1)
        east.apply_replicated(52, "from west")
        self.assertEqual(east.next_id(), 61)

    def test_interleaved_config_passes_the_check(self):
        self.assertEqual(check_sequence_config(list(interleaved_pair())), [])

    def test_check_rejects_a_reused_offset(self):
        nodes = [OrderNode("east", 10, 1), OrderNode("west", 10, 2), OrderNode("eu", 10, 2)]
        self.assertEqual(check_sequence_config(nodes), ["eu: offset 2 already used by west"])

    def test_check_rejects_too_few_slots_for_a_new_region(self):
        nodes = [OrderNode("east", 2, 1), OrderNode("west", 2, 2), OrderNode("eu", 2, 3)]
        problems = check_sequence_config(nodes)
        self.assertIn("increment 2 is smaller than the 3 nodes", problems)

    def test_check_rejects_mixed_increments(self):
        nodes = [OrderNode("east", 10, 1), OrderNode("west", 5, 2)]
        self.assertEqual(check_sequence_config(nodes), ["all nodes must use the same increment"])


if __name__ == "__main__":
    unittest.main()
