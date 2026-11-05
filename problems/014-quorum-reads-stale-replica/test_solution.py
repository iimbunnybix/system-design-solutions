import unittest

from solution import Cluster, Outages, Replica, simulate

OUTAGE = Outages({2: [(3_000, 9_000)], 0: [(14_000, 14_500)]})


class ReproduceStaleReads(unittest.TestCase):
    def test_w1_r1_returns_stale_values(self):
        s = simulate(3, 1, 1, OUTAGE)
        self.assertEqual(s.stale_reads, 767)
        self.assertAlmostEqual(s.stale_rate, 0.0551, places=4)

    def test_stale_even_without_outages(self):
        s = simulate(3, 1, 1, Outages())
        self.assertAlmostEqual(s.stale_rate, 0.0611, places=4)   # replication lag alone

    def test_rejoined_replica_serves_old_data(self):
        c = Cluster(3, 1, 1, Outages({2: [(0, 100)]}), seed=0, read_repair=False)
        c.write("k", "new", 10)
        c.advance(200)
        self.assertEqual(c.replicas[2].get("k"), (0, None))      # missed while down
        self.assertEqual(c.replicas[0].get("k")[1], "new")

    def test_partial_overlap_configs_still_stale(self):
        for w, r in [(1, 2), (2, 1)]:
            self.assertGreater(simulate(3, w, r, OUTAGE).stale_reads, 0, (w, r))


class Quorum(unittest.TestCase):
    def test_r_plus_w_greater_than_n_never_stale(self):
        for seed in range(5):
            s = simulate(3, 2, 2, OUTAGE, seed=seed)
            self.assertEqual(s.stale_reads, 0, seed)
            self.assertEqual(s.unavailable_reads, 0)            # one replica down is fine
            self.assertEqual(s.failed_writes, 0)

    def test_five_replicas_w3_r3(self):
        out = Outages({1: [(0, 5_000)], 4: [(2_000, 7_000)]})
        s = simulate(5, 3, 3, out, ops=10_000)
        self.assertEqual(s.stale_reads, 0)
        self.assertEqual(s.unavailable_reads, 0)

    def test_two_of_three_down_is_unavailable_not_stale(self):
        s = simulate(3, 2, 2, Outages({1: [(1_000, 2_000)], 2: [(1_500, 2_500)]}))
        self.assertEqual(s.stale_reads, 0)
        self.assertEqual((s.unavailable_reads, s.failed_writes), (348, 158))

    def test_extreme_configs_trade_availability(self):
        self.assertEqual(simulate(3, 3, 1, OUTAGE).failed_writes, 1_972)
        self.assertEqual(simulate(3, 1, 3, OUTAGE).unavailable_reads, 4_529)

    def test_strict_quorum_flag_and_validation(self):
        self.assertTrue(Cluster(3, 2, 2, Outages()).strict_quorum)
        self.assertFalse(Cluster(3, 1, 2, Outages()).strict_quorum)
        with self.assertRaises(ValueError):
            Cluster(3, 4, 1, Outages())


class Repair(unittest.TestCase):
    def test_read_repair_heals_contacted_replicas(self):
        c = Cluster(3, 2, 2, Outages({2: [(0, 100)]}), seed=3)
        c.write("k", "new", 10)
        c.advance(200)
        self.assertEqual(c.replicas[2].get("k"), (0, None))
        for t in range(200, 220):
            self.assertEqual(c.read("k", t), "new")
        self.assertEqual(c.replicas[2].get("k")[1], "new")
        self.assertGreater(c.stats.repairs, 0)

    def test_anti_entropy_fixes_unread_keys(self):
        c = Cluster(3, 2, 2, Outages({2: [(0, 100)]}), seed=0)
        for i in range(10):
            c.write(f"k{i}", f"v{i}", i)
        fixed = c.anti_entropy(500)
        self.assertEqual(fixed, 10)
        self.assertEqual(c.replicas[2].data, c.replicas[0].data)
        self.assertEqual(c.anti_entropy(501), 0)

    def test_replica_keeps_highest_version(self):
        r = Replica()
        self.assertTrue(r.apply("k", 2, "b"))
        self.assertFalse(r.apply("k", 1, "a"))                  # late, older message
        self.assertEqual(r.get("k"), (2, "b"))


if __name__ == "__main__":
    unittest.main()
