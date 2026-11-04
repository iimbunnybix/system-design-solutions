import itertools
import random
import unittest

from solution import LwwCart, ORSetCart


class ReproduceLww(unittest.TestCase):
    def test_concurrent_adds_lose_an_item(self):
        a, b = LwwCart(), LwwCart()
        a.add("headphones", 1, now=100.0)       # phone -> region A
        b.add("charger", 1, now=100.4)          # laptop -> region B
        a.merge(b)
        b.merge(a)
        self.assertEqual(a.items, {"charger": 1})
        self.assertEqual(a.items, b.items)

    def test_clock_skew_lets_older_edit_win(self):
        a, b = LwwCart(), LwwCart()
        b.add("charger", 1, now=100.0)
        a.merge(b)
        a.add("headphones", 1, now=99.0)        # A's clock is 2s behind
        b.merge(a)
        self.assertNotIn("headphones", b.items)


class ORSetSemantics(unittest.TestCase):
    def sync(self, *reps):
        snaps = [r.snapshot() for r in reps]
        for r in reps:
            for s in snaps:
                r.merge(s)

    def test_concurrent_adds_both_survive(self):
        a, b = ORSetCart("eu"), ORSetCart("us")
        a.add("headphones")
        b.add("charger")
        self.sync(a, b)
        self.assertEqual(a.items(), {"charger": 1, "headphones": 1})
        self.assertEqual(a.items(), b.items())

    def test_remove_then_sync(self):
        a, b = ORSetCart("eu"), ORSetCart("us")
        a.add("mug", 2)
        self.sync(a, b)
        b.remove("mug")
        self.sync(a, b)
        self.assertEqual(a.items(), {})

    def test_concurrent_add_and_remove_add_wins(self):
        a, b = ORSetCart("eu"), ORSetCart("us")
        a.add("mug")
        self.sync(a, b)
        a.remove("mug")                         # removes the tag it saw
        b.add("mug")                            # concurrent re-add: new tag
        self.sync(a, b)
        self.assertEqual(a.items(), {"mug": 1})
        self.assertEqual(b.items(), {"mug": 1})

    def test_concurrent_quantity_changes_are_both_kept(self):
        a, b = ORSetCart("eu"), ORSetCart("us")
        a.add("socks", 1)
        self.sync(a, b)
        a.add("socks", 2)
        b.add("socks", 1)
        self.sync(a, b)
        self.assertEqual(a.items(), {"socks": 4})

    def test_set_quantity(self):
        a = ORSetCart("eu")
        a.add("socks", 3)
        a.set_quantity("socks", 1)
        self.assertEqual(a.items(), {"socks": 1})
        a.set_quantity("socks", 0)
        self.assertEqual(a.items(), {})

    def test_merge_is_idempotent(self):
        a, b = ORSetCart("eu"), ORSetCart("us")
        a.add("x")
        b.add("y")
        b.remove("y")
        a.merge(b)
        once = a.items()
        for _ in range(3):
            a.merge(b)
        self.assertEqual(a.items(), once)

    def test_counter_never_reuses_tags_after_restore(self):
        a = ORSetCart("eu")
        a.add("x")
        backup = a.snapshot()
        fresh = ORSetCart("eu")                 # process restarted, state reloaded
        fresh.merge(backup)
        tag = fresh.add("y")
        self.assertEqual(tag, ("eu", 2))

    def test_invalid_qty(self):
        with self.assertRaises(ValueError):
            ORSetCart("eu").add("x", 0)

    def test_compact_keeps_result(self):
        a = ORSetCart("eu")
        for _ in range(5):
            a.add("x")
        a.remove("x")
        a.add("y")
        before = a.items()
        self.assertEqual(a.compact(), 5)
        self.assertEqual(a.items(), before)


class Convergence(unittest.TestCase):
    def test_random_ops_and_any_merge_order_converge(self):
        rng = random.Random(2026)
        skus = ["a", "b", "c", "d"]
        for trial in range(30):
            reps = [ORSetCart(r) for r in ("eu", "us", "ap")]
            for _ in range(40):
                r = rng.choice(reps)
                op, sku = rng.random(), rng.choice(skus)
                if op < 0.55:
                    r.add(sku, rng.randint(1, 3))
                elif op < 0.85:
                    r.remove(sku)
                else:
                    r.merge(rng.choice(reps).snapshot())   # partial gossip
            snaps = [r.snapshot() for r in reps]
            results = set()
            for order in itertools.permutations(snaps):
                target = ORSetCart("probe")
                for s in order:
                    target.merge(s)
                results.add(tuple(target.items().items()))
            self.assertEqual(len(results), 1, f"trial {trial} diverged")


if __name__ == "__main__":
    unittest.main()
