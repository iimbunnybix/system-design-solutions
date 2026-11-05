import threading
import unittest

from solution import ShardedCounter, recommended_shards, simulate_row_contention

class CounterWithRacingResize(ShardedCounter):
    """Test double: the first shards_for() call after arm() runs a competing
    resize to ``racing_n`` in another thread, as a second app instance would."""

    def arm(self, key: str, racing_n: int) -> None:
        self._racing = (key, racing_n)

    def shards_for(self, key: str) -> int:
        current = super().shards_for(key)
        racing = getattr(self, "_racing", None)
        if racing is not None:
            self._racing = None
            racer = threading.Thread(target=self._resize_and_like, args=racing)
            racer.start()
            # Give the racer a chance to finish; if set_shards holds the lock
            # around check-and-write, the racer blocks and finishes later.
            racer.join(timeout=0.2)
            self.racer = racer
        return current

    def _resize_and_like(self, key: str, n: int) -> None:
        self.set_shards(key, n)
        for _ in range(100):
            self.increment(key)


RATE = 2000.0        # likes per second on one viral post
HOLD = 0.001         # 1 ms row-lock hold per UPDATE
DURATION = 5.0


class ReproduceHotRow(unittest.TestCase):
    def test_single_row_queue_grows_without_bound(self):
        r = simulate_row_contention(RATE, DURATION, shards=1, service_time=HOLD, seed=1)
        self.assertEqual(r["utilization_per_row"], 2.0)       # demand is twice what one row can do
        self.assertGreater(r["max_wait"], 4.0)                  # last writes wait seconds
        self.assertGreater(r["p50_wait"], 2.0)

    def test_longer_spike_means_longer_waits(self):
        short = simulate_row_contention(RATE, 2.0, 1, HOLD, seed=1)
        long = simulate_row_contention(RATE, 8.0, 1, HOLD, seed=1)
        self.assertGreater(long["max_wait"], 3 * short["max_wait"])


class Sharding(unittest.TestCase):
    def test_sixteen_shards_keep_waits_in_milliseconds(self):
        r = simulate_row_contention(RATE, DURATION, shards=16, service_time=HOLD, seed=1)
        self.assertAlmostEqual(r["utilization_per_row"], 0.125)
        self.assertLess(r["p99_wait"], 0.002)
        self.assertLess(r["max_wait"], 0.01)

    def test_more_shards_monotonically_better(self):
        p99 = [simulate_row_contention(RATE, DURATION, n, HOLD, seed=1)["p99_wait"] for n in (1, 2, 4, 16)]
        self.assertEqual(p99, sorted(p99, reverse=True))

    def test_readme_table_matches_the_model(self):
        # (shards, p50 wait ms, p99 wait ms) exactly as printed in README.md
        readme_rows = [(1, 2630, 5150), (2, 87, 167), (4, 0.05, 3.2), (16, 0, 1.0)]
        for shards, p50_ms, p99_ms in readme_rows:
            r = simulate_row_contention(RATE, DURATION, shards, HOLD, seed=1)
            self.assertEqual(r["writes"], 10210)
            self.assertAlmostEqual(r["p50_wait"] * 1000, p50_ms, delta=max(0.005, p50_ms * 0.01))
            self.assertAlmostEqual(r["p99_wait"] * 1000, p99_ms, delta=max(0.05, p99_ms * 0.01))

    def test_recommended_shards(self):
        self.assertEqual(recommended_shards(2000, 0.001), 4)
        self.assertEqual(recommended_shards(10, 0.001), 1)

    def test_exact_count_under_concurrency(self):
        c = ShardedCounter(default_shards=1, seed=3)
        c.set_shards("post:viral", 8)
        go = threading.Barrier(8)

        def liker():
            go.wait()
            for _ in range(2000):
                c.increment("post:viral")

        ts = [threading.Thread(target=liker) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(c.value("post:viral"), 16000)
        rows = c.rows("post:viral")
        self.assertEqual(len(rows), 8)
        self.assertTrue(all(r > 0 for r in rows))               # load actually spread

    def test_growing_shards_keeps_old_counts(self):
        c = ShardedCounter(seed=1)
        for _ in range(10):
            c.increment("p")
        c.set_shards("p", 4)
        for _ in range(10):
            c.increment("p")
        self.assertEqual(c.value("p"), 20)

    def test_shrinking_is_refused(self):
        c = ShardedCounter(default_shards=4)
        with self.assertRaises(ValueError):
            c.set_shards("p", 2)

    def test_concurrent_resizes_never_shrink_the_counter_or_hide_counts(self):
        c = CounterWithRacingResize(seed=1)
        c.arm("p", 8)
        c.set_shards("p", 4)            # checked against 1 while a resize to 8 races
        c.racer.join()
        self.assertEqual(c.shards_for("p"), 8)
        self.assertEqual(c.value("p"), 100)

    def test_cold_keys_and_negative_amounts(self):
        c = ShardedCounter(seed=1)
        self.assertEqual(c.value("never-liked"), 0)
        c.increment("p", 1)
        c.increment("p", -1)                                    # unlike
        self.assertEqual(c.value("p"), 0)

    def test_invalid_parameters(self):
        with self.assertRaises(ValueError):
            ShardedCounter(default_shards=0)
        with self.assertRaises(ValueError):
            simulate_row_contention(0, 1, 1, HOLD)


if __name__ == "__main__":
    unittest.main()
