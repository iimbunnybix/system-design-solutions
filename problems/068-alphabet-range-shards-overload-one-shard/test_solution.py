import random
import string
import unittest

from solution import (HOT_SHARD_RATIO, imbalance, naive_split_points, shard_for, shard_sizes,
                      split_hot_shard, split_points_from_sample)

SHARDS = 4
USERS = 40_000
SAMPLE_SIZE = 2_000
# Synthetic first-letter weights for this test: "s" is by far the most common
# initial and letters such as "q" and "x" are rare.
FIRST_LETTER_WEIGHTS = {"a": 7, "b": 5, "c": 6, "d": 6, "e": 3, "f": 2, "g": 2, "h": 3, "i": 1,
                        "j": 8, "k": 6, "l": 4, "m": 9, "n": 3, "o": 2, "p": 4, "q": 1, "r": 5,
                        "s": 24, "t": 9, "u": 1, "v": 2, "w": 4, "x": 1, "y": 2, "z": 2}


def make_usernames(count, seed):
    rng = random.Random(seed)
    letters = list(string.ascii_lowercase)
    weights = [FIRST_LETTER_WEIGHTS[letter] for letter in letters]
    tail_characters = string.ascii_lowercase + string.digits
    names = []
    for _ in range(count):
        first = rng.choices(letters, weights)[0]
        tail = "".join(rng.choice(tail_characters) for _ in range(7))
        names.append(first + tail)
    return names


class ReproduceProblem(unittest.TestCase):
    def test_alphabet_split_overloads_one_shard(self):
        sizes = shard_sizes(make_usernames(USERS, seed=1), naive_split_points())
        self.assertGreater(imbalance(sizes), 1.4)

    def test_s_to_z_shard_is_the_biggest(self):
        sizes = shard_sizes(make_usernames(USERS, seed=1), naive_split_points())
        self.assertEqual(sizes.index(max(sizes)), 3)


class FixSampledSplitPoints(unittest.TestCase):
    def setUp(self):
        sample = make_usernames(SAMPLE_SIZE, seed=2)
        self.points = split_points_from_sample(sample, SHARDS)

    def test_new_users_spread_evenly(self):
        sizes = shard_sizes(make_usernames(USERS, seed=1), self.points)
        self.assertLess(imbalance(sizes), 1.1)

    def test_routing_keeps_prefix_neighbours_together(self):
        self.assertEqual(shard_for("sam0001", self.points), shard_for("sam0002", self.points))

    def test_routing_is_case_insensitive(self):
        self.assertEqual(shard_for("Sam0001", self.points), shard_for("sam0001", self.points))

    def test_boundary_key_belongs_to_the_right_hand_shard(self):
        boundary = self.points[0]
        self.assertEqual(shard_for(boundary, self.points), 1)

    def test_duplicate_heavy_sample_gives_no_empty_ranges(self):
        points = split_points_from_sample(["same"] * 10 + ["zed"], 4)
        self.assertEqual(points, ["same"])

    def test_sample_smaller_than_shard_count_is_rejected(self):
        with self.assertRaises(ValueError):
            split_points_from_sample(["a", "b"], 4)


class FixSplitHotShard(unittest.TestCase):
    def test_hot_shard_is_split_at_its_median(self):
        users = make_usernames(USERS, seed=1)
        before = shard_sizes(users, naive_split_points())
        points = split_hot_shard(users, naive_split_points())
        after = shard_sizes(users, points)
        self.assertEqual(len(points), 4)
        self.assertLess(max(after), max(before))

    def test_only_the_hot_shards_keys_move(self):
        users = make_usernames(USERS, seed=1)
        old_points = naive_split_points()
        new_points = split_hot_shard(users, old_points)
        hot = shard_sizes(users, old_points).index(max(shard_sizes(users, old_points)))
        for user in users:
            if shard_for(user, old_points) != hot:
                self.assertEqual(shard_for(user, old_points), shard_for(user, new_points))

    def test_balanced_shards_are_left_alone(self):
        users = make_usernames(USERS, seed=1)
        points = split_points_from_sample(make_usernames(SAMPLE_SIZE, seed=2), SHARDS)
        self.assertLess(imbalance(shard_sizes(users, points)), HOT_SHARD_RATIO)
        self.assertEqual(split_hot_shard(users, points), points)

    def test_shard_of_identical_keys_cannot_be_split(self):
        keys = ["bob"] * 100 + ["zed"]
        self.assertEqual(split_hot_shard(keys, ["m"]), ["m"])


if __name__ == "__main__":
    unittest.main()
