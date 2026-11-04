import threading
import unittest

from solution import ShardedCounter, recommended_shards, simulate_row_contention

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
