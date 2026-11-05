import unittest

from solution import (LEADER, NaiveCluster, NotCommitted, NotLeader, QuorumCluster,
                      majority_size)

FIVE_NODES = ["a", "b", "c", "d", "e"]
MINORITY_SIDE = ["a", "b"]
MAJORITY_SIDE = ["c", "d", "e"]


def partition_with_both_sides_electing(cluster_class, minority_elects: bool = True):
    """'a' leads, the network splits 2 + 3, and both sides try to elect a leader."""
    cluster = cluster_class(FIVE_NODES)
    assert cluster.elect("a")
    cluster.write("a", "balance", 50)
    cluster.network.partition(MINORITY_SIDE, MAJORITY_SIDE)
    cluster.elect("c")          # majority side times out on 'a' and elects
    if minority_elects:
        cluster.elect("b")      # minority side's follower also times out
    return cluster


def leaders(cluster):
    return sorted(name for name, node in cluster.nodes.items() if node.role == LEADER)


class ReproduceProblem(unittest.TestCase):
    def test_both_sides_of_a_partition_elect_a_leader(self):
        cluster = partition_with_both_sides_electing(NaiveCluster)
        self.assertEqual(leaders(cluster), ["a", "b", "c"])

    def test_both_sides_accept_writes_and_data_diverges(self):
        cluster = partition_with_both_sides_electing(NaiveCluster)
        cluster.write("a", "balance", 100)
        cluster.write("c", "balance", 200)
        cluster.network.heal()
        self.assertEqual(cluster.nodes["a"].data["balance"], 100)
        self.assertEqual(cluster.nodes["e"].data["balance"], 200)

    def test_lone_isolated_node_elects_itself(self):
        cluster = NaiveCluster(FIVE_NODES)
        cluster.network.partition(["a"], ["b", "c", "d", "e"])
        self.assertTrue(cluster.elect("a"))


class FixMajorityOfConfiguredCluster(unittest.TestCase):
    def test_majority_size_counts_more_than_half(self):
        self.assertEqual(majority_size(5), 3)
        self.assertEqual(majority_size(4), 3)
        self.assertEqual(majority_size(1), 1)

    def test_only_the_majority_side_elects_a_leader(self):
        cluster = partition_with_both_sides_electing(QuorumCluster)
        # 'b' failed to win, and its newer term also made old leader 'a' step down.
        self.assertEqual(leaders(cluster), ["c"])

    def test_majority_side_leader_commits_writes(self):
        cluster = partition_with_both_sides_electing(QuorumCluster)
        cluster.write("c", "balance", 200)
        for name in MAJORITY_SIDE:
            self.assertEqual(cluster.nodes[name].data["balance"], 200)

    def test_stale_leader_on_minority_side_refuses_writes(self):
        cluster = partition_with_both_sides_electing(QuorumCluster, minority_elects=False)
        with self.assertRaises(NotCommitted):
            cluster.write("a", "balance", 100)
        self.assertEqual(cluster.nodes["a"].data["balance"], 50)

    def test_stale_leader_steps_down_after_heal(self):
        cluster = partition_with_both_sides_electing(QuorumCluster, minority_elects=False)
        cluster.write("c", "balance", 200)
        cluster.network.heal()
        with self.assertRaises(NotLeader):
            cluster.write("a", "balance", 100)
        self.assertEqual(leaders(cluster), ["c"])

    def test_no_divergence_after_heal(self):
        cluster = partition_with_both_sides_electing(QuorumCluster)
        cluster.write("c", "balance", 200)
        cluster.network.heal()
        cluster.write("c", "balance", 300)
        values = {node.data["balance"] for node in cluster.nodes.values()}
        self.assertEqual(values, {300})

    def test_even_split_leaves_cluster_without_leader(self):
        cluster = QuorumCluster(["a", "b", "c", "d"])
        cluster.network.partition(["a", "b"], ["c", "d"])
        self.assertFalse(cluster.elect("a"))
        self.assertFalse(cluster.elect("c"))
        self.assertEqual(leaders(cluster), [])

    def test_node_votes_only_once_per_term(self):
        cluster = QuorumCluster(["a", "b", "c"])
        self.assertTrue(cluster.nodes["b"].handle_vote_request(1, "a"))
        self.assertFalse(cluster.nodes["b"].handle_vote_request(1, "c"))

    def test_vote_for_older_term_is_refused(self):
        cluster = QuorumCluster(["a", "b", "c"])
        cluster.nodes["b"].current_term = 5
        self.assertFalse(cluster.nodes["b"].handle_vote_request(4, "a"))

    def test_isolated_node_cannot_elect_itself(self):
        cluster = QuorumCluster(FIVE_NODES)
        cluster.network.partition(["a"], ["b", "c", "d", "e"])
        self.assertFalse(cluster.elect("a"))


if __name__ == "__main__":
    unittest.main()
