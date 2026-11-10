import unittest

from solution import (ELECTION_TIMEOUT_MS, LEASE_DURATION_MS, Cluster, FakeClock, NotLeader,
                      linearizable_read, naive_read)

NODES = ["a", "b", "c", "d", "e"]
OLD_SIDE = ["a", "b"]
NEW_SIDE = ["c", "d", "e"]
PARTITION_AT_MS = 100


def healthy_cluster() -> tuple[Cluster, FakeClock]:
    """Five nodes, 'a' is leader and balance=100 is committed everywhere at t=0."""
    clock = FakeClock()
    cluster = Cluster(NODES, clock)
    assert cluster.elect("a")
    cluster.write("a", "balance", 100)
    return cluster, clock


def partition_and_replace_leader(cluster: Cluster, clock: FakeClock) -> None:
    """Cut 'a' and 'b' off, wait an election timeout, elect 'c' and write 200."""
    clock.advance(PARTITION_AT_MS)
    cluster.partition(OLD_SIDE, NEW_SIDE)
    clock.advance(ELECTION_TIMEOUT_MS - PARTITION_AT_MS)
    assert cluster.elect("c")
    cluster.write("c", "balance", 200)


class ReproduceProblem(unittest.TestCase):
    def test_old_leader_returns_overwritten_value_after_failover(self):
        cluster, clock = healthy_cluster()
        partition_and_replace_leader(cluster, clock)
        self.assertEqual(naive_read(cluster, "c", "balance"), 200)
        self.assertEqual(naive_read(cluster, "a", "balance"), 100)

    def test_old_leader_keeps_serving_stale_reads_for_the_whole_partition(self):
        cluster, clock = healthy_cluster()
        partition_and_replace_leader(cluster, clock)
        clock.advance(20 * 60 * 1_000)
        self.assertEqual(naive_read(cluster, "a", "balance"), 100)


class FixWithLeaseAndReadIndex(unittest.TestCase):
    def test_old_leader_refuses_reads_after_it_is_replaced(self):
        cluster, clock = healthy_cluster()
        partition_and_replace_leader(cluster, clock)
        with self.assertRaises(NotLeader):
            linearizable_read(cluster, "a", "balance")

    def test_new_leader_returns_the_latest_value(self):
        cluster, clock = healthy_cluster()
        partition_and_replace_leader(cluster, clock)
        self.assertEqual(linearizable_read(cluster, "c", "balance"), 200)

    def test_no_new_leader_can_be_elected_while_the_old_lease_is_valid(self):
        cluster, clock = healthy_cluster()
        cluster.partition(OLD_SIDE, NEW_SIDE)
        clock.advance(LEASE_DURATION_MS - 1)
        self.assertEqual(linearizable_read(cluster, "a", "balance"), 100)
        self.assertFalse(cluster.elect("c"))

    def test_lease_ends_before_followers_may_vote_again(self):
        self.assertLess(LEASE_DURATION_MS, ELECTION_TIMEOUT_MS)

    def test_reads_inside_the_lease_need_no_extra_round_trip(self):
        cluster, clock = healthy_cluster()
        rounds_before = cluster.heartbeat_rounds
        for _ in range(800):
            linearizable_read(cluster, "a", "balance")
            clock.advance(1)
        self.assertEqual(cluster.heartbeat_rounds, rounds_before)

    def test_expired_lease_on_healthy_leader_is_renewed_by_read_index(self):
        cluster, clock = healthy_cluster()
        clock.advance(LEASE_DURATION_MS + 1)
        self.assertEqual(linearizable_read(cluster, "a", "balance"), 100)
        self.assertGreater(cluster.nodes["a"].lease_expires_ms, clock.now_ms)

    def test_old_leader_steps_down_when_it_meets_a_newer_term(self):
        cluster, clock = healthy_cluster()
        partition_and_replace_leader(cluster, clock)
        cluster.heal()
        self.assertFalse(cluster.heartbeat("a"))
        with self.assertRaises(NotLeader):
            linearizable_read(cluster, "a", "balance")

    def test_minority_leader_cannot_write(self):
        cluster, clock = healthy_cluster()
        cluster.partition(OLD_SIDE, NEW_SIDE)
        with self.assertRaises(NotLeader):
            cluster.write("a", "balance", 300)


if __name__ == "__main__":
    unittest.main()
