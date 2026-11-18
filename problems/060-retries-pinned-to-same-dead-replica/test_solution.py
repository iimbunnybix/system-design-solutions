import unittest

from solution import (MAX_ATTEMPTS, REPLICATION_FACTOR, AllAttemptsFailed, Cluster,
                      failover_get, failure_rate, naive_get, node_names, preference_list,
                      replicas_to_try)

DEAD_NODE = "kv-05"
KEY_COUNT = 10_000


def all_keys() -> list[str]:
    return [f"user:{number}" for number in range(KEY_COUNT)]


def cluster_with_down(*dead_nodes: str) -> Cluster:
    cluster = Cluster(node_names())
    cluster.down = set(dead_nodes)
    return cluster


def key_whose_first_replica_is(node: str) -> str:
    for key in all_keys():
        if preference_list(key, node_names())[0] == node:
            return key
    raise AssertionError("no such key")


class ReproduceProblem(unittest.TestCase):
    def test_one_dead_node_fails_every_key_it_is_first_choice_for(self):
        rate = failure_rate(cluster_with_down(DEAD_NODE), all_keys(), naive_get)
        self.assertAlmostEqual(rate, 1 / 12, delta=0.01)       # 8.42% in this key set

    def test_all_retries_hit_the_same_dead_node(self):
        cluster = cluster_with_down(DEAD_NODE)
        with self.assertRaises(AllAttemptsFailed):
            naive_get(cluster, key_whose_first_replica_is(DEAD_NODE))
        self.assertEqual(cluster.requests_per_node[DEAD_NODE], MAX_ATTEMPTS)
        self.assertEqual(sum(cluster.requests_per_node.values()), MAX_ATTEMPTS)


class FixRetryOnNextReplica(unittest.TestCase):
    def test_one_dead_node_causes_no_failed_reads(self):
        rate = failure_rate(cluster_with_down(DEAD_NODE), all_keys(), failover_get)
        self.assertEqual(rate, 0.0)

    def test_two_dead_nodes_cause_no_failed_reads(self):
        rate = failure_rate(cluster_with_down("kv-05", "kv-07"), all_keys(), failover_get)
        self.assertEqual(rate, 0.0)

    def test_only_keys_with_every_replica_dead_fail(self):
        dead = node_names()[:REPLICATION_FACTOR]
        rate = failure_rate(cluster_with_down(*dead), all_keys(), failover_get)
        # 1 of the C(12, 3) = 220 replica sets is entirely dead.
        self.assertAlmostEqual(rate, 1 / 220, delta=0.002)

    def test_dead_node_gets_one_attempt_then_the_next_replica_answers(self):
        cluster = cluster_with_down(DEAD_NODE)
        key = key_whose_first_replica_is(DEAD_NODE)
        self.assertEqual(failover_get(cluster, key), f"value-of-{key}")
        self.assertEqual(cluster.requests_per_node[DEAD_NODE], 1)
        self.assertEqual(sum(cluster.requests_per_node.values()), 2)

    def test_healthy_first_replica_is_used_for_locality(self):
        cluster = cluster_with_down()
        key = key_whose_first_replica_is("kv-03")
        failover_get(cluster, key)
        self.assertEqual(cluster.requests_per_node["kv-03"], 1)

    def test_each_attempt_uses_a_different_replica(self):
        order = replicas_to_try("user:1", node_names(), MAX_ATTEMPTS)
        self.assertEqual(len(set(order)), MAX_ATTEMPTS)

    def test_more_attempts_than_replicas_wraps_around(self):
        order = replicas_to_try("user:1", node_names(), REPLICATION_FACTOR + 1)
        self.assertEqual(order[REPLICATION_FACTOR], order[0])

    def test_preference_list_is_stable_and_distinct(self):
        first = preference_list("user:42", node_names())
        self.assertEqual(first, preference_list("user:42", node_names()))
        self.assertEqual(len(set(first)), REPLICATION_FACTOR)


if __name__ == "__main__":
    unittest.main()
