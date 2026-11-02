import copy
import unittest
from collections import Counter

from solution import ConsistentHashRing, ModuloSharding, moved_fraction

KEYS = [f"product:{i}" for i in range(100_000)]
NODES = [f"redis-{i}" for i in range(4)]


def ring(nodes, vnodes=150):
    r = ConsistentHashRing(vnodes)
    for n in nodes:
        r.add_node(n)
    return r


class ConsistentHashingTests(unittest.TestCase):
    def test_modulo_sharding_moves_most_keys(self):
        moved = moved_fraction(ModuloSharding(NODES), ModuloSharding(NODES + ["redis-4"]), KEYS)
        self.assertGreater(moved, 0.75)

    def test_adding_node_moves_about_one_fifth(self):
        before = ring(NODES)
        after = copy.deepcopy(before)
        after.add_node("redis-4")
        moved = moved_fraction(before, after, KEYS)
        self.assertLess(moved, 0.25)
        self.assertGreater(moved, 0.15)

    def test_keys_only_move_to_the_new_node(self):
        before = ring(NODES)
        after = copy.deepcopy(before)
        after.add_node("redis-4")
        for k in KEYS[:20_000]:
            if before.get_node(k) != after.get_node(k):
                self.assertEqual(after.get_node(k), "redis-4")

    def test_removing_node_only_moves_its_keys(self):
        full = ring(NODES + ["redis-4"])
        after = copy.deepcopy(full)
        after.remove_node("redis-2")
        for k in KEYS[:20_000]:
            if full.get_node(k) != "redis-2":
                self.assertEqual(full.get_node(k), after.get_node(k))

    def test_virtual_nodes_balance_load(self):
        r = ring(NODES)
        load = Counter(r.get_node(k) for k in KEYS)
        shares = [v / len(KEYS) for v in load.values()]
        self.assertLess(max(shares) - min(shares), 0.06)

    def test_weights_shift_load(self):
        r = ConsistentHashRing(150)
        r.add_node("small")
        r.add_node("big", weight=3)
        load = Counter(r.get_node(k) for k in KEYS)
        self.assertGreater(load["big"] / load["small"], 2.3)

    def test_lookup_is_deterministic_and_errors(self):
        r = ring(NODES)
        self.assertEqual(r.get_node("x"), r.get_node("x"))
        with self.assertRaises(RuntimeError):
            ConsistentHashRing().get_node("x")
        with self.assertRaises(ValueError):
            r.add_node("redis-0")


if __name__ == "__main__":
    unittest.main()
