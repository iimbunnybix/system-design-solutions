import math
import unittest

from solution import (BACKLOG_PER_PARTITION, HEADROOM, PARTITIONS_BEFORE, PEAK_INFLOW_PER_SECOND,
                      PER_CONSUMER_PER_SECOND, TARGET_DRAIN_SECONDS, Partition,
                      add_partitions_to_live_topic, capacity_warnings, consumer_names,
                      drain_seconds, even_topic, fraction_of_keys_remapped, group_drain_seconds,
                      idle_consumers, naive_scale_consumers, partitions_needed, plan_capacity,
                      range_assign, round_robin_assign, stable_partition)

TOTAL_BACKLOG = PARTITIONS_BEFORE * BACKLOG_PER_PARTITION


def incident_topic() -> list[Partition]:
    """The 6-partition topic as it was when the backlog alert fired."""
    return even_topic(PARTITIONS_BEFORE, TOTAL_BACKLOG, PEAK_INFLOW_PER_SECOND)


def incident_plan(consumer_count: int, current_partitions: int | None = PARTITIONS_BEFORE):
    """Run the planner with the incident's numbers."""
    return plan_capacity(PEAK_INFLOW_PER_SECOND, TOTAL_BACKLOG, TARGET_DRAIN_SECONDS,
                         PER_CONSUMER_PER_SECOND, HEADROOM, consumer_count, current_partitions)


def drain_with_one_consumer_per_partition(topic: list[Partition]) -> float:
    """Drain time when the group has exactly as many members as partitions."""
    assignment = range_assign(len(topic), consumer_names(len(topic)))
    return group_drain_seconds(topic, assignment, PER_CONSUMER_PER_SECOND)


class ReproduceProblem(unittest.TestCase):
    def test_tripling_consumers_does_not_change_drain_time(self):
        topic = incident_topic()
        self.assertEqual(naive_scale_consumers(topic, 6), 3000.0)
        self.assertEqual(naive_scale_consumers(topic, 18), 3000.0)

    def test_range_assignor_leaves_twelve_of_eighteen_consumers_idle(self):
        assignment = range_assign(PARTITIONS_BEFORE, consumer_names(18))
        self.assertEqual(len(idle_consumers(assignment)), 12)

    def test_round_robin_assignor_also_leaves_twelve_idle(self):
        assignment = round_robin_assign(PARTITIONS_BEFORE, consumer_names(18))
        self.assertEqual(len(idle_consumers(assignment)), 12)

    def test_each_partition_still_has_exactly_one_owner(self):
        assignment = range_assign(PARTITIONS_BEFORE, consumer_names(18))
        owned = sorted(p for partitions in assignment.values() for p in partitions)
        self.assertEqual(owned, list(range(PARTITIONS_BEFORE)))


class FixWithCapacityPlanner(unittest.TestCase):
    def test_planner_sizes_partitions_from_throughput_with_headroom(self):
        plan = incident_plan(consumer_count=13)
        self.assertEqual(plan.required_throughput, 7000.0)
        self.assertEqual(plan.partitions, 13)

    def test_planned_topic_drains_within_target(self):
        topic = even_topic(incident_plan(13).partitions, TOTAL_BACKLOG, PEAK_INFLOW_PER_SECOND)
        self.assertLess(drain_with_one_consumer_per_partition(topic), TARGET_DRAIN_SECONDS)

    def test_planner_warns_when_consumers_exceed_partitions(self):
        warnings = incident_plan(consumer_count=18, current_partitions=None).warnings
        self.assertEqual(len(warnings), 1)
        self.assertIn("5 consumers will sit idle", warnings[0])

    def test_no_warning_when_consumers_match_partitions_and_count_unchanged(self):
        self.assertEqual(capacity_warnings(13, 13, current_partition_count=13), [])

    def test_planner_warns_that_adding_partitions_breaks_key_ordering(self):
        warnings = incident_plan(consumer_count=13).warnings
        self.assertEqual(len(warnings), 1)
        self.assertIn("per-key ordering", warnings[0])

    def test_two_thirds_of_keys_move_when_going_from_6_to_18_partitions(self):
        keys = [f"order-{index}" for index in range(10_000)]
        self.assertAlmostEqual(fraction_of_keys_remapped(keys, 6, 18), 2 / 3, delta=0.02)

    def test_key_mapping_is_stable_for_a_fixed_partition_count(self):
        self.assertEqual(stable_partition("order-42", 13), stable_partition("order-42", 13))

    def test_growing_live_topic_leaves_old_backlog_on_old_partitions(self):
        grown = add_partitions_to_live_topic(incident_topic(), 13)
        self.assertEqual(grown[0].backlog, BACKLOG_PER_PARTITION)
        self.assertEqual(grown[12].backlog, 0)
        # Old partitions still hold 600k messages each, so the target is missed.
        self.assertGreater(drain_with_one_consumer_per_partition(grown), TARGET_DRAIN_SECONDS)


class EdgeCases(unittest.TestCase):
    def test_range_assignor_gives_extra_partitions_to_first_consumers(self):
        assignment = range_assign(7, consumer_names(3))
        self.assertEqual(assignment["consumer-00"], [0, 1, 2])
        self.assertEqual(assignment["consumer-02"], [5, 6])

    def test_consumer_slower_than_inflow_never_drains(self):
        self.assertEqual(drain_seconds(100, inflow_per_second=700, capacity_per_second=700),
                         math.inf)

    def test_empty_backlog_drains_immediately(self):
        self.assertEqual(drain_seconds(0, inflow_per_second=900, capacity_per_second=700), 0.0)

    def test_tiny_load_still_needs_one_partition(self):
        self.assertEqual(partitions_needed(1, PER_CONSUMER_PER_SECOND, HEADROOM), 1)

    def test_zero_per_consumer_rate_is_rejected(self):
        with self.assertRaises(ValueError):
            partitions_needed(1000, 0, HEADROOM)

    def test_zero_target_drain_time_is_rejected(self):
        with self.assertRaises(ValueError):
            plan_capacity(1000, 0, 0, PER_CONSUMER_PER_SECOND, HEADROOM, 1)


if __name__ == "__main__":
    unittest.main()
