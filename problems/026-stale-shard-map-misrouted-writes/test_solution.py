import unittest

from solution import Client, Cluster, ShardMap, WrongShard


def initial_map():
    # s1 owns ids from "" up to "m", s2 owns "m" and above.
    return ShardMap(version=1, range_starts=("", "m"), shards=("s1", "s2"))


def cluster_with_users(check_ownership):
    cluster = Cluster(initial_map(), check_ownership=check_ownership)
    writer = Client(cluster)
    writer.put("alice", "v1")
    writer.put("kate", "v1")
    return cluster


class ReproduceProblem(unittest.TestCase):
    def test_write_through_a_stale_map_is_lost_after_a_split(self):
        cluster = cluster_with_users(check_ownership=False)
        stale_client = Client(cluster)
        cluster.split("s1", "f", "s3")          # "kate" moves to s3
        stale_client.put("kate", "v2")           # still routed to s1
        fresh_client = Client(cluster)
        self.assertEqual(fresh_client.get("kate"), "v1")
        self.assertEqual(cluster.shards["s1"].data["kate"], "v2")   # orphaned copy


class FixShardsRejectKeysTheyDoNotOwn(unittest.TestCase):
    def test_stale_client_write_reaches_the_new_owner(self):
        cluster = cluster_with_users(check_ownership=True)
        stale_client = Client(cluster)
        cluster.split("s1", "f", "s3")
        stale_client.put("kate", "v2")
        self.assertEqual(Client(cluster).get("kate"), "v2")
        self.assertNotIn("kate", cluster.shards["s1"].data)

    def test_stale_client_refreshes_its_map_once(self):
        cluster = cluster_with_users(check_ownership=True)
        stale_client = Client(cluster)
        cluster.split("s1", "f", "s3")
        stale_client.put("kate", "v2")
        stale_client.put("kim", "v1")
        self.assertEqual(stale_client.refreshes, 1)
        self.assertEqual(stale_client.cached_map.version, 2)

    def test_stale_client_read_finds_moved_data(self):
        cluster = cluster_with_users(check_ownership=True)
        stale_client = Client(cluster)
        cluster.split("s1", "f", "s3")
        self.assertEqual(stale_client.get("kate"), "v1")

    def test_keys_that_did_not_move_need_no_refresh(self):
        cluster = cluster_with_users(check_ownership=True)
        stale_client = Client(cluster)
        cluster.split("s1", "f", "s3")
        stale_client.put("alice", "v2")
        self.assertEqual(stale_client.refreshes, 0)

    def test_shard_rejects_a_key_it_does_not_own(self):
        cluster = cluster_with_users(check_ownership=True)
        with self.assertRaises(WrongShard) as raised:
            cluster.shards["s2"].put("alice", "x")
        self.assertEqual(raised.exception.map_version, 1)


class ShardMapLookup(unittest.TestCase):
    def test_range_boundaries(self):
        shard_map = ShardMap(2, ("", "f", "m"), ("s1", "s3", "s2"))
        self.assertEqual(shard_map.shard_for("e"), "s1")
        self.assertEqual(shard_map.shard_for("f"), "s3")
        self.assertEqual(shard_map.shard_for("lz"), "s3")
        self.assertEqual(shard_map.shard_for("m"), "s2")
        self.assertEqual(shard_map.shard_for(""), "s1")

    def test_split_moves_only_keys_above_the_split_point(self):
        cluster = cluster_with_users(check_ownership=True)
        cluster.split("s1", "f", "s3")
        self.assertEqual(cluster.shards["s1"].data, {"alice": "v1"})
        self.assertEqual(cluster.shards["s3"].data, {"kate": "v1"})


if __name__ == "__main__":
    unittest.main()
