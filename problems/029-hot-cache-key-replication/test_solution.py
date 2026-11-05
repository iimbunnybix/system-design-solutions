import unittest

from solution import (COPY_COUNT, HOT_THRESHOLD, NODE_CAPACITY_PER_S, NODE_COUNT, VIRAL_KEY,
                      CacheNode, HotKeyDetector, NaiveCacheClient, ReplicatingCacheClient,
                      copy_key, copy_node_index, fake_database, home_node_index, node_loads,
                      rejected_reads, spike_workload)


def new_nodes() -> list[CacheNode]:
    """A fresh, empty cluster."""
    return [CacheNode() for _ in range(NODE_COUNT)]


def run_spike(client: NaiveCacheClient | ReplicatingCacheClient) -> list[int]:
    """Send one second of spike traffic and return the reads per node."""
    for key in spike_workload():
        client.get(key)
    return node_loads(client.nodes)


def replicating_client(nodes: list[CacheNode]) -> ReplicatingCacheClient:
    """Client with the default detector and a fixed seed."""
    return ReplicatingCacheClient(nodes, fake_database, HotKeyDetector(), seed=1)


def make_hot(client: ReplicatingCacheClient, key: str, reads: int = 200) -> None:
    """Read a key often enough that the detector marks it hot."""
    for _ in range(reads):
        client.get(key)


class ReproduceProblem(unittest.TestCase):
    def test_viral_key_sends_45_percent_of_reads_to_one_node(self):
        loads = run_spike(NaiveCacheClient(new_nodes(), fake_database))
        self.assertEqual(max(loads), 45_468)
        self.assertEqual(loads.index(max(loads)), home_node_index(VIRAL_KEY))

    def test_hot_node_drops_a_quarter_of_all_reads_while_others_idle(self):
        loads = run_spike(NaiveCacheClient(new_nodes(), fake_database))
        self.assertEqual(rejected_reads(loads), 25_468)
        others = [load for load in loads if load != max(loads)]
        self.assertLess(max(others), NODE_CAPACITY_PER_S // 3)


class FixHotKeyReplication(unittest.TestCase):
    def test_no_node_exceeds_its_capacity(self):
        loads = run_spike(replicating_client(new_nodes()))
        self.assertEqual(max(loads), 14_156)
        self.assertEqual(rejected_reads(loads), 0)

    def test_replication_does_not_add_database_reads(self):
        naive = NaiveCacheClient(new_nodes(), fake_database)
        fixed = replicating_client(new_nodes())
        run_spike(naive)
        run_spike(fixed)
        self.assertEqual(fixed.db_reads, naive.db_reads)

    def test_key_becomes_hot_at_the_threshold(self):
        detector = HotKeyDetector()
        for _ in range(HOT_THRESHOLD - 1):
            detector.record("k")
        self.assertFalse(detector.is_hot("k"))
        detector.record("k")
        self.assertTrue(detector.is_hot("k"))

    def test_key_cools_down_when_it_leaves_the_window(self):
        detector = HotKeyDetector(window=100, threshold=20)
        for _ in range(20):
            detector.record("k")
        for number in range(100):
            detector.record(f"other:{number}")
        self.assertFalse(detector.is_hot("k"))

    def test_cold_keys_are_read_only_from_their_home_node(self):
        nodes = new_nodes()
        client = replicating_client(nodes)
        client.get("product:42")
        self.assertEqual(nodes[home_node_index("product:42")].reads, 1)
        self.assertEqual(sum(node_loads(nodes)), 1)

    def test_copies_of_a_key_are_on_distinct_nodes(self):
        placements = {copy_node_index(VIRAL_KEY, number) for number in range(COPY_COUNT)}
        self.assertEqual(len(placements), COPY_COUNT)

    def test_update_of_a_hot_key_is_seen_through_every_copy(self):
        nodes = new_nodes()
        client = replicating_client(nodes)
        make_hot(client, VIRAL_KEY)
        client.set(VIRAL_KEY, "price 499")
        answers = {client.get(VIRAL_KEY) for _ in range(200)}
        self.assertEqual(answers, {"price 499"})

    def test_invalidate_after_cooling_down_leaves_no_stale_copy(self):
        nodes = new_nodes()
        client = replicating_client(nodes)
        make_hot(client, VIRAL_KEY)
        for number in range(2_000):              # the key cools down
            client.get(f"product:{number}")
        client.invalidate(VIRAL_KEY)
        for number in range(COPY_COUNT):
            node = nodes[copy_node_index(VIRAL_KEY, number)]
            self.assertNotIn(copy_key(VIRAL_KEY, number), node.store)

    def test_more_copies_than_nodes_is_rejected(self):
        with self.assertRaises(ValueError):
            ReplicatingCacheClient([CacheNode()], fake_database, HotKeyDetector())


if __name__ == "__main__":
    unittest.main()
