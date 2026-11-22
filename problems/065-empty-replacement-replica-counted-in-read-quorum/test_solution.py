import unittest

from solution import (CREATED_WHILE_B_DOWN, KEY_COUNT, UPDATED_WHILE_B_DOWN, Cluster,
                      QuorumUnavailable, ReplicaState, Versioned, build_incident_cluster,
                      wrong_reads)


def cluster_after_disk_failure(use_naive_replacement: bool):
    """The incident cluster with C's disk replaced by an empty one."""
    cluster, latest = build_incident_cluster()
    if use_naive_replacement:
        cluster.naive_replace("C")
    else:
        cluster.replace("C")
    return cluster, latest


class ReproduceProblem(unittest.TestCase):
    def test_reads_are_correct_before_the_disk_failure(self):
        cluster, latest = build_incident_cluster()
        self.assertEqual(wrong_reads(cluster, latest, use_naive=True), {"stale": 0, "missing": 0})

    def test_empty_replica_in_the_read_set_returns_old_values(self):
        cluster, latest = cluster_after_disk_failure(use_naive_replacement=True)
        counts = wrong_reads(cluster, latest, use_naive=True)
        self.assertEqual(counts["stale"], UPDATED_WHILE_B_DOWN)

    def test_empty_replica_in_the_read_set_loses_new_keys(self):
        cluster, latest = cluster_after_disk_failure(use_naive_replacement=True)
        counts = wrong_reads(cluster, latest, use_naive=True)
        self.assertEqual(counts["missing"], CREATED_WHILE_B_DOWN)

    def test_new_empty_node_is_the_fastest_responder(self):
        cluster, _ = cluster_after_disk_failure(use_naive_replacement=True)
        self.assertEqual(cluster.replicas[0].name, "C")


class FixBootstrapBeforeServing(unittest.TestCase):
    def test_replacement_starts_bootstrapping(self):
        cluster, _ = cluster_after_disk_failure(use_naive_replacement=False)
        self.assertIs(cluster.replica("C").state, ReplicaState.BOOTSTRAPPING)

    def test_reads_during_bootstrap_skip_the_empty_replica(self):
        cluster, latest = cluster_after_disk_failure(use_naive_replacement=False)
        self.assertEqual(wrong_reads(cluster, latest, use_naive=False), {"stale": 0, "missing": 0})

    def test_bootstrap_copies_the_newest_version_of_every_key(self):
        cluster, latest = cluster_after_disk_failure(use_naive_replacement=False)
        cluster.bootstrap("C")
        self.assertEqual(cluster.replica("C").data, latest)
        self.assertEqual(len(latest), KEY_COUNT + CREATED_WHILE_B_DOWN)

    def test_reads_are_correct_after_bootstrap_with_the_new_node_in_front(self):
        cluster, latest = cluster_after_disk_failure(use_naive_replacement=False)
        cluster.bootstrap("C")
        self.assertIs(cluster.replica("C").state, ReplicaState.SERVING)
        self.assertEqual(wrong_reads(cluster, latest, use_naive=False), {"stale": 0, "missing": 0})

    def test_writes_during_bootstrap_reach_the_new_node(self):
        cluster, _ = cluster_after_disk_failure(use_naive_replacement=False)
        cluster.write("user:new", Versioned(1, "fresh"))
        self.assertEqual(cluster.replica("C").get("user:new"), Versioned(1, "fresh"))

    def test_streamed_old_copy_does_not_overwrite_a_newer_live_write(self):
        cluster, _ = cluster_after_disk_failure(use_naive_replacement=False)
        cluster.write("user:0", Versioned(3, "profile-0-v3"))
        cluster.bootstrap("C")
        self.assertEqual(cluster.replica("C").get("user:0"), Versioned(3, "profile-0-v3"))


class FixFailLoudlyWithoutQuorum(unittest.TestCase):
    def test_bootstrapping_ack_does_not_count_toward_the_write_quorum(self):
        cluster, _ = cluster_after_disk_failure(use_naive_replacement=False)
        with self.assertRaises(QuorumUnavailable):
            cluster.write("user:x", Versioned(1, "x"), unreachable=("B",))

    def test_read_fails_when_two_replicas_are_bootstrapping(self):
        cluster = Cluster(["A", "B", "C"])
        cluster.write("user:1", Versioned(1, "v1"))
        cluster.replace("B")
        cluster.replace("C")
        with self.assertRaises(QuorumUnavailable):
            cluster.read("user:1")

    def test_missing_key_reads_as_none_when_quorum_is_healthy(self):
        cluster = Cluster(["A", "B", "C"])
        self.assertIsNone(cluster.read("user:never-written"))


if __name__ == "__main__":
    unittest.main()
