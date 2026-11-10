import unittest

from solution import (MerkleTree, Record, Replica, merkle_repair, naive_full_compare_repair)

KEY_COUNT = 100_000
MISSED_WRITES = 20


def make_identical_replicas(key_count: int = KEY_COUNT) -> tuple[Replica, Replica]:
    """Two replicas holding the same data, as if replication had worked."""
    left = Replica("replica-a")
    right = Replica("replica-b")
    for index in range(key_count):
        key = f"user:{index}"
        left.put(key, 1, f"profile-{index}")
        right.put(key, 1, f"profile-{index}")
    return left, right


def apply_missed_writes(left: Replica, right: Replica) -> None:
    """Replica B was down: it misses 20 updates and one new key, and A misses one write."""
    for index in range(MISSED_WRITES):
        left.put(f"user:{index * 997}", 2, "updated")
    left.put("user:new-signup", 1, "fresh")
    # Replica B received one write that replica A missed.
    right.put("user:42", 3, "changed-on-b")


class ReproduceProblem(unittest.TestCase):
    def test_naive_repair_ships_every_key_even_when_few_differ(self):
        left, right = make_identical_replicas()
        apply_missed_writes(left, right)
        stats = naive_full_compare_repair(left, right)
        self.assertEqual(stats.keys_exchanged, KEY_COUNT + 1)
        self.assertEqual(stats.keys_repaired, MISSED_WRITES + 2)

    def test_naive_repair_ships_every_key_when_nothing_differs(self):
        left, right = make_identical_replicas()
        stats = naive_full_compare_repair(left, right)
        self.assertEqual(stats.keys_exchanged, KEY_COUNT)
        self.assertEqual(stats.keys_repaired, 0)


class FixWithMerkleTree(unittest.TestCase):
    def test_identical_replicas_compare_only_the_root(self):
        left, right = make_identical_replicas()
        stats = merkle_repair(left, right)
        self.assertEqual(stats.hashes_compared, 1)
        self.assertEqual(stats.keys_exchanged, 0)

    def test_repair_makes_replicas_equal(self):
        left, right = make_identical_replicas()
        apply_missed_writes(left, right)
        merkle_repair(left, right)
        self.assertEqual(left.data, right.data)
        self.assertEqual(MerkleTree(left).root, MerkleTree(right).root)

    def test_repair_fixes_the_same_keys_as_the_naive_job(self):
        left, right = make_identical_replicas()
        apply_missed_writes(left, right)
        stats = merkle_repair(left, right)
        self.assertEqual(stats.keys_repaired, MISSED_WRITES + 2)

    def test_repair_exchanges_far_fewer_keys(self):
        left, right = make_identical_replicas()
        apply_missed_writes(left, right)
        stats = merkle_repair(left, right)
        # 22 differing keys land in at most 22 buckets of ~98 keys each.
        self.assertLess(stats.keys_exchanged, KEY_COUNT // 30)

    def test_newer_version_wins_in_both_directions(self):
        left, right = make_identical_replicas(1_000)
        left.put("user:1", 5, "newer-on-a")
        right.put("user:2", 7, "newer-on-b")
        merkle_repair(left, right)
        self.assertEqual(right.data["user:1"].value, "newer-on-a")
        self.assertEqual(left.data["user:2"].value, "newer-on-b")

    def test_key_missing_on_one_side_is_copied(self):
        left, right = make_identical_replicas(1_000)
        right.put("only-on-b", 1, "x")
        merkle_repair(left, right)
        self.assertIn("only-on-b", left.data)

    def test_empty_replicas_need_no_repair(self):
        stats = merkle_repair(Replica("a"), Replica("b"))
        self.assertEqual(stats.keys_exchanged, 0)

    def test_same_version_with_different_value_is_detected(self):
        left, right = make_identical_replicas(1_000)
        # Same version but different value: the hashes still differ.
        right.data["user:3"] = Record(1, "corrupted")
        self.assertNotEqual(MerkleTree(left).root, MerkleTree(right).root)

    def test_same_version_conflict_is_reported_not_guessed(self):
        left, right = make_identical_replicas(1_000)
        right.data["user:3"] = Record(1, "corrupted")
        stats = merkle_repair(left, right)
        self.assertEqual(stats.conflicts, ["user:3"])
        self.assertEqual(stats.keys_repaired, 0)

    def test_corrupt_copy_that_sorts_higher_does_not_overwrite_healthy_copy(self):
        left, right = make_identical_replicas(1_000)
        right.data["user:3"] = Record(1, "zzz-corrupted")
        merkle_repair(left, right)
        self.assertEqual(left.data["user:3"], Record(1, "profile-3"))
        self.assertEqual(right.data["user:3"], Record(1, "zzz-corrupted"))

    def test_naive_repair_also_reports_same_version_conflicts(self):
        left, right = make_identical_replicas(1_000)
        left.data["user:7"] = Record(1, "zzz-corrupted")
        stats = naive_full_compare_repair(left, right)
        self.assertEqual(stats.conflicts, ["user:7"])
        self.assertEqual(right.data["user:7"], Record(1, "profile-7"))

    def test_values_containing_separator_bytes_cannot_hide_a_missing_key(self):
        # user:0 and user:1783 share a bucket. Replica B is missing user:1783,
        # and its user:0 value contains the bytes A's bucket would serialize to.
        left, right = Replica("a"), Replica("b")
        left.put("user:0", 1, "x")
        left.put("user:1783", 1, "y")
        right.put("user:0", 1, "x\x01user:1783\x001\x00y")
        self.assertNotEqual(MerkleTree(left).root, MerkleTree(right).root)

    def test_missing_key_hidden_by_separator_bytes_is_repaired(self):
        left, right = Replica("a"), Replica("b")
        left.put("user:0", 1, "x")
        left.put("user:1783", 1, "y")
        right.put("user:0", 1, "x\x01user:1783\x001\x00y")
        merkle_repair(left, right)
        self.assertIn("user:1783", right.data)

    def test_leaf_count_must_be_power_of_two(self):
        with self.assertRaises(ValueError):
            MerkleTree(Replica("a"), leaf_count=1000)


if __name__ == "__main__":
    unittest.main()
