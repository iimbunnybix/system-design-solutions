import unittest

from solution import (NODE_RACKS, REPLICATION_FACTOR, HashRing, all_keys, distinct_replicas,
                      keys_lost_when_failed, keys_without_quorum, naive_replicas,
                      nodes_in_rack, place_all, placement_report, worst_single_failure)

KEYS = all_keys()


def production_ring() -> HashRing:
    """Six machines in three racks, as in the incident."""
    return HashRing(NODE_RACKS)


def first_key_with_duplicate_replicas(ring: HashRing) -> str:
    """A key whose naive replica list repeats a machine."""
    for key in KEYS:
        if len(set(naive_replicas(ring, key))) < REPLICATION_FACTOR:
            return key
    raise AssertionError("expected at least one key with duplicate replicas")


class ReproduceProblem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ring = production_ring()
        cls.placements = place_all(cls.ring, use_fix=False, keys=KEYS)

    def test_over_a_third_of_keys_have_fewer_than_three_distinct_machines(self):
        report = placement_report(self.ring, self.placements)
        self.assertAlmostEqual(report["fewer_than_3_nodes"], 0.37685)

    def test_most_keys_have_fewer_than_three_distinct_racks(self):
        report = placement_report(self.ring, self.placements)
        self.assertAlmostEqual(report["fewer_than_3_racks"], 0.76755)

    def test_one_machine_failure_loses_keys_and_quorum(self):
        worst = worst_single_failure(self.ring, self.placements)
        self.assertEqual(worst["keys_lost"], 96)
        self.assertEqual(worst["keys_without_quorum"], 2629)

    def test_one_rack_failure_loses_hundreds_of_keys(self):
        rack_a = nodes_in_rack(self.ring, "rack-a")
        self.assertEqual(keys_lost_when_failed(self.placements, rack_a), 828)


class FixDistinctReplicas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ring = production_ring()
        cls.placements = place_all(cls.ring, use_fix=True, keys=KEYS)

    def test_every_key_has_three_distinct_machines_and_racks(self):
        report = placement_report(self.ring, self.placements)
        self.assertEqual(report, {"fewer_than_3_nodes": 0.0, "fewer_than_3_racks": 0.0})

    def test_no_single_machine_failure_loses_data_or_quorum(self):
        worst = worst_single_failure(self.ring, self.placements)
        self.assertEqual(worst, {"keys_lost": 0, "keys_without_quorum": 0})

    def test_no_single_rack_failure_loses_data_or_quorum(self):
        for rack in ("rack-a", "rack-b", "rack-c"):
            failed = nodes_in_rack(self.ring, rack)
            self.assertEqual(keys_lost_when_failed(self.placements, failed), 0)
            self.assertEqual(keys_without_quorum(self.placements, failed), 0)

    def test_first_replica_is_still_the_key_owner(self):
        for key in KEYS[:500]:
            self.assertEqual(distinct_replicas(self.ring, key)[0],
                             naive_replicas(self.ring, key)[0])

    def test_key_with_duplicates_now_gets_distinct_machines(self):
        key = first_key_with_duplicate_replicas(self.ring)
        self.assertEqual(len(set(distinct_replicas(self.ring, key))), REPLICATION_FACTOR)

    def test_without_rack_awareness_machines_are_still_distinct(self):
        for key in KEYS[:500]:
            replicas = distinct_replicas(self.ring, key, rack_aware=False)
            self.assertEqual(len(set(replicas)), REPLICATION_FACTOR)

    def test_placement_is_identical_on_a_freshly_built_ring(self):
        other_ring = production_ring()
        for key in KEYS[:500]:
            self.assertEqual(distinct_replicas(other_ring, key), self.placements[key])


class FixSmallClusters(unittest.TestCase):
    def test_two_machines_give_two_replicas(self):
        ring = HashRing({"node-1": "rack-a", "node-2": "rack-b"})
        self.assertEqual(sorted(distinct_replicas(ring, "user:1")), ["node-1", "node-2"])

    def test_one_machine_gives_one_replica(self):
        ring = HashRing({"node-1": "rack-a"})
        self.assertEqual(distinct_replicas(ring, "user:1"), ["node-1"])

    def test_fewer_racks_than_replicas_still_gives_three_machines_on_both_racks(self):
        ring = HashRing({"node-1": "rack-a", "node-2": "rack-a",
                         "node-3": "rack-b", "node-4": "rack-b"})
        for key in KEYS[:500]:
            replicas = distinct_replicas(ring, key)
            self.assertEqual(len(set(replicas)), REPLICATION_FACTOR)
            self.assertEqual({ring.node_racks[node] for node in replicas}, {"rack-a", "rack-b"})


if __name__ == "__main__":
    unittest.main()
