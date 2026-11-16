import random
import unittest

from solution import (ScoreShard, exact_top_k_full_scan, naive_merged_top_k,
                      tput_top_k)

TOP_K = 10
SHARD_COUNT = 16
PLAYER_COUNT = 20_000
SEED = 7
AVERAGE_SKILL = 10


def steady_player_shards() -> list[ScoreShard]:
    """Sam scores 30 on every shard and never makes any local top-2."""
    return [
        ScoreShard({"a": 50, "b": 40, "sam": 30}),
        ScoreShard({"c": 50, "d": 40, "sam": 30}),
        ScoreShard({"e": 50, "f": 40, "sam": 30}),
    ]


def season_shards(seed: int = SEED) -> list[ScoreShard]:
    """Seeded season: each player has a skill and plays on a random set of shards."""
    rng = random.Random(seed)
    partial_scores: list[dict[str, int]] = [{} for _ in range(SHARD_COUNT)]
    for number in range(PLAYER_COUNT):
        player_id = f"player-{number:05d}"
        skill = rng.expovariate(1 / AVERAGE_SKILL)
        shard_count = rng.randint(1, SHARD_COUNT)
        for shard_index in rng.sample(range(SHARD_COUNT), shard_count):
            partial_scores[shard_index][player_id] = int(skill * rng.uniform(0.5, 1.5))
    return [ScoreShard(scores) for scores in partial_scores]


def total_entries_sent(shards: list[ScoreShard]) -> int:
    return sum(shard.entries_sent for shard in shards)


class ReproduceProblem(unittest.TestCase):
    def test_steady_player_is_missing_from_merged_local_top_lists(self):
        ranking = naive_merged_top_k(steady_player_shards(), 2)
        self.assertEqual(ranking, [("a", 50), ("c", 50)])
        self.assertEqual(exact_top_k_full_scan(steady_player_shards(), 1), [("sam", 90)])

    def test_merged_lists_miss_four_of_the_true_top_ten(self):
        merged = [player for player, _ in naive_merged_top_k(season_shards(), TOP_K)]
        exact = [player for player, _ in exact_top_k_full_scan(season_shards(), TOP_K)]
        missing = [player for player in exact if player not in merged]
        self.assertEqual(missing, ["player-07910", "player-03549",
                                   "player-11814", "player-14995"])

    def test_merged_lists_undercount_every_total(self):
        merged = naive_merged_top_k(season_shards(), TOP_K)
        exact = exact_top_k_full_scan(season_shards(), TOP_K)
        self.assertEqual(merged[0], ("player-10602", 913))
        self.assertEqual(exact[0], ("player-10602", 1258))
        self.assertEqual([entry for entry in merged if entry in exact], [])


class FixThreePhaseThreshold(unittest.TestCase):
    def test_steady_player_is_found_as_the_leader(self):
        self.assertEqual(tput_top_k(steady_player_shards(), 1), [("sam", 90)])

    def test_season_leaderboard_matches_full_scan(self):
        self.assertEqual(tput_top_k(season_shards(), TOP_K),
                         exact_top_k_full_scan(season_shards(), TOP_K))

    def test_sends_under_a_tenth_of_what_a_full_scan_sends(self):
        tput = season_shards()
        full = season_shards()
        tput_top_k(tput, TOP_K)
        exact_top_k_full_scan(full, TOP_K)
        self.assertEqual(total_entries_sent(full), 168_906)
        self.assertEqual(total_entries_sent(tput), 15_715)

    def test_matches_full_scan_on_many_small_random_seasons(self):
        rng = random.Random(SEED)
        for _ in range(300):
            shards = random_small_season(rng)
            k = rng.randint(1, 5)
            self.assertEqual(tput_top_k(shards, k), exact_top_k_full_scan(shards, k))


def random_small_season(rng: random.Random) -> list[ScoreShard]:
    """Tiny seasons with many ties, to probe the pruning bounds."""
    shards = []
    for _ in range(rng.randint(1, 5)):
        scores = {}
        for number in range(rng.randint(0, 8)):
            scores[f"p{rng.randint(0, 9)}"] = rng.randint(0, 5)
        shards.append(ScoreShard(scores))
    return shards


class EdgeCases(unittest.TestCase):
    def test_fewer_players_than_k_returns_everyone(self):
        shards = [ScoreShard({"a": 3}), ScoreShard({"a": 1, "b": 2})]
        self.assertEqual(tput_top_k(shards, 5), [("a", 4), ("b", 2)])

    def test_no_shards_or_zero_k_returns_empty(self):
        self.assertEqual(tput_top_k([], TOP_K), [])
        self.assertEqual(tput_top_k(steady_player_shards(), 0), [])

    def test_ties_are_broken_by_player_id(self):
        shards = [ScoreShard({"b": 5, "a": 5}), ScoreShard({"c": 10})]
        self.assertEqual(tput_top_k(shards, 2), [("c", 10), ("a", 5)])

    def test_negative_scores_are_rejected(self):
        with self.assertRaises(ValueError):
            ScoreShard({"a": -1})


if __name__ == "__main__":
    unittest.main()
