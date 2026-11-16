import unittest

from solution import (NoConsumers, moved_partitions, naive_range_assign,
                      partition_counts, sticky_assign)

PARTITION_COUNT = 120
PARTITIONS = list(range(PARTITION_COUNT))
TWELVE_WORKERS = [f"worker-{number:02d}" for number in range(1, 13)]


def assert_valid_and_balanced(test: unittest.TestCase, assignment: dict[int, str],
                              consumers: list[str]) -> None:
    """Every partition has exactly one live owner and shares differ by <= 1."""
    test.assertEqual(sorted(assignment), PARTITIONS)
    test.assertTrue(set(assignment.values()) <= set(consumers))
    counts = [partition_counts(assignment).get(consumer, 0) for consumer in consumers]
    test.assertLessEqual(max(counts) - min(counts), 1)


class ReproduceProblem(unittest.TestCase):
    def test_adding_one_consumer_moves_45_of_120_partitions(self):
        before = naive_range_assign(PARTITIONS, TWELVE_WORKERS)
        after = naive_range_assign(PARTITIONS, TWELVE_WORKERS + ["worker-13"])
        self.assertEqual(len(moved_partitions(before, after)), 45)

    def test_new_consumer_sorting_first_moves_75_partitions(self):
        before = naive_range_assign(PARTITIONS, TWELVE_WORKERS)
        after = naive_range_assign(PARTITIONS, ["worker-00"] + TWELVE_WORKERS)
        self.assertEqual(len(moved_partitions(before, after)), 75)

    def test_removing_one_consumer_moves_31_partitions(self):
        before = naive_range_assign(PARTITIONS, TWELVE_WORKERS)
        remaining = [worker for worker in TWELVE_WORKERS if worker != "worker-05"]
        after = naive_range_assign(PARTITIONS, remaining)
        self.assertEqual(len(moved_partitions(before, after)), 31)


class FixStickyAssignment(unittest.TestCase):
    def setUp(self):
        self.before = naive_range_assign(PARTITIONS, TWELVE_WORKERS)

    def test_adding_one_consumer_moves_only_its_fair_share(self):
        consumers = ["worker-00"] + TWELVE_WORKERS
        after = sticky_assign(PARTITIONS, consumers, self.before)
        moved = moved_partitions(self.before, after)
        self.assertEqual(len(moved), PARTITION_COUNT // len(consumers))
        self.assertTrue(all(after[partition] == "worker-00" for partition in moved))
        assert_valid_and_balanced(self, after, consumers)

    def test_removing_one_consumer_moves_only_its_partitions(self):
        remaining = [worker for worker in TWELVE_WORKERS if worker != "worker-05"]
        after = sticky_assign(PARTITIONS, remaining, self.before)
        moved = moved_partitions(self.before, after)
        self.assertEqual(len(moved), 10)
        self.assertTrue(all(self.before[partition] == "worker-05" for partition in moved))
        assert_valid_and_balanced(self, after, remaining)

    def test_unchanged_membership_moves_nothing(self):
        after = sticky_assign(PARTITIONS, TWELVE_WORKERS, self.before)
        self.assertEqual(moved_partitions(self.before, after), [])

    def test_first_assignment_with_no_history_is_balanced(self):
        after = sticky_assign(PARTITIONS, TWELVE_WORKERS, {})
        assert_valid_and_balanced(self, after, TWELVE_WORKERS)

    def test_rolling_restart_keeps_assignment_balanced_and_cheap(self):
        assignment = sticky_assign(PARTITIONS, TWELVE_WORKERS, {})
        consumers = list(TWELVE_WORKERS)
        total_moved = 0
        for old_name in TWELVE_WORKERS:
            # A restarted pod comes back under a new name.
            consumers = [c for c in consumers if c != old_name] + [old_name + "-new"]
            new_assignment = sticky_assign(PARTITIONS, consumers, assignment)
            total_moved += len(moved_partitions(assignment, new_assignment))
            assignment = new_assignment
            assert_valid_and_balanced(self, assignment, consumers)
        # Each restart hands over exactly the restarted pod's 10 partitions.
        self.assertEqual(total_moved, PARTITION_COUNT)


class EdgeCases(unittest.TestCase):
    def test_more_consumers_than_partitions_leaves_some_idle(self):
        consumers = ["a", "b", "c", "d"]
        after = sticky_assign([0, 1], consumers, {})
        self.assertEqual(sorted(after), [0, 1])
        self.assertEqual(len(set(after.values())), 2)

    def test_partition_removed_from_topic_is_dropped_from_assignment(self):
        before = {0: "a", 1: "a", 2: "b"}
        after = sticky_assign([0, 2], ["a", "b"], before)
        self.assertEqual(after, {0: "a", 2: "b"})

    def test_new_partitions_go_to_the_least_loaded_consumer(self):
        before = {0: "a", 1: "a", 2: "b"}
        after = sticky_assign([0, 1, 2, 3], ["a", "b"], before)
        self.assertEqual(after[3], "b")
        self.assertEqual(moved_partitions(before, after), [3])

    def test_zero_consumers_is_an_error(self):
        with self.assertRaises(NoConsumers):
            sticky_assign(PARTITIONS, [], {})
        with self.assertRaises(NoConsumers):
            naive_range_assign(PARTITIONS, [])


if __name__ == "__main__":
    unittest.main()
