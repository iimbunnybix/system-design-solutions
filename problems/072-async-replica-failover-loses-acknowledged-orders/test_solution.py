import unittest

from solution import (CRASH_AT_MS, CROSS_REGION_LAG_MS, SAME_REGION_LAG_MS, Replica,
                      acknowledged_writes, async_ack_time, default_replicas,
                      lost_acknowledged_writes, promote_first_listed, promote_most_caught_up,
                      semi_sync_ack_time)


def zone_b_down() -> list[Replica]:
    """Topology where the same-region replica is unreachable."""
    return [Replica("dr-replica", CROSS_REGION_LAG_MS),
            Replica("zone-b-replica", SAME_REGION_LAG_MS, reachable=False)]


def no_replicas_reachable() -> list[Replica]:
    """Topology where the primary cannot reach any replica."""
    return [Replica("dr-replica", CROSS_REGION_LAG_MS, reachable=False),
            Replica("zone-b-replica", SAME_REGION_LAG_MS, reachable=False)]


class ReproduceProblem(unittest.TestCase):
    def test_promoting_the_dr_replica_loses_79_acknowledged_orders(self):
        lost = lost_acknowledged_writes(async_ack_time, promote_first_listed, default_replicas())
        self.assertEqual(lost, 79)

    def test_async_acks_every_write_immediately(self):
        acked = acknowledged_writes(async_ack_time, default_replicas(), CRASH_AT_MS)
        self.assertEqual(len(acked), 10_000)

    def test_even_the_most_caught_up_replica_misses_async_acks(self):
        lost = lost_acknowledged_writes(async_ack_time, promote_most_caught_up, default_replicas())
        self.assertEqual(lost, 1)


class FixSemiSyncAndMostCaughtUpPromotion(unittest.TestCase):
    def test_no_acknowledged_order_is_lost(self):
        lost = lost_acknowledged_writes(semi_sync_ack_time, promote_most_caught_up,
                                        default_replicas())
        self.assertEqual(lost, 0)

    def test_ack_waits_for_the_fastest_replica(self):
        self.assertEqual(semi_sync_ack_time(100, default_replicas()), 100 + SAME_REGION_LAG_MS)

    def test_write_whose_ack_was_due_after_the_crash_was_not_acknowledged(self):
        acked = acknowledged_writes(semi_sync_ack_time, default_replicas(), CRASH_AT_MS)
        self.assertNotIn(CRASH_AT_MS - 1, acked)
        self.assertIn(CRASH_AT_MS - SAME_REGION_LAG_MS, acked)

    def test_most_caught_up_replica_is_promoted_even_if_listed_last(self):
        promoted = promote_most_caught_up(default_replicas(), CRASH_AT_MS)
        self.assertEqual(promoted.name, "zone-b-replica")

    def test_semi_sync_alone_is_not_enough_with_the_old_runbook(self):
        lost = lost_acknowledged_writes(semi_sync_ack_time, promote_first_listed,
                                        default_replicas())
        self.assertEqual(lost, 78)

    def test_with_one_replica_down_acks_wait_for_the_other_and_nothing_is_lost(self):
        replicas = zone_b_down()
        self.assertEqual(semi_sync_ack_time(0, replicas), CROSS_REGION_LAG_MS)
        lost = lost_acknowledged_writes(semi_sync_ack_time, promote_most_caught_up, replicas)
        self.assertEqual(lost, 0)

    def test_writes_are_refused_when_no_replica_can_confirm(self):
        self.assertIsNone(semi_sync_ack_time(0, no_replicas_reachable()))
        acked = acknowledged_writes(semi_sync_ack_time, no_replicas_reachable(), CRASH_AT_MS)
        self.assertEqual(acked, [])


if __name__ == "__main__":
    unittest.main()
