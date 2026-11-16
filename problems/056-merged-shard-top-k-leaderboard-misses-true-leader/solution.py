"""Exact global top-k leaderboard when each player's score is spread over shards.

Score events are sharded by match, so one player's season points are split
across every shard. The leaderboard asked each shard for its local top-k and
summed them. A player who scores steadily everywhere but tops no single
shard never appears in any local list, so the true leader was missing and
other players' totals were undercounted. This module keeps that merge and
adds TPUT (Cao & Wang, 2004): three rounds of bounded fetches that prove which
players can still reach the top-k, then fetch only their exact totals.
"""
from __future__ import annotations

Ranking = list[tuple[str, int]]   # (player_id, total score), best first


class ScoreShard:
    """One shard's partial scores. Counts how many entries it sends back."""

    def __init__(self, partial_scores: dict[str, int]) -> None:
        for player_id, score in partial_scores.items():
            if score < 0:
                # The pruning bounds below assume an unseen score is >= 0.
                raise ValueError(f"negative score for {player_id}")
        self.partial_scores = dict(partial_scores)
        self.entries_sent = 0

    def local_top(self, k: int) -> dict[str, int]:
        """The k highest partial scores on this shard."""
        ranked = sorted(self.partial_scores.items(), key=lambda item: (-item[1], item[0]))
        return self._send(dict(ranked[:k]))

    def scores_at_least(self, threshold: float) -> dict[str, int]:
        """Every partial score >= threshold."""
        found = {}
        for player_id, score in self.partial_scores.items():
            if score >= threshold:
                found[player_id] = score
        return self._send(found)

    def scores_for(self, player_ids: set[str]) -> dict[str, int]:
        """Exact partial scores for the given players (absent means 0)."""
        found = {}
        for player_id in player_ids:
            if player_id in self.partial_scores:
                found[player_id] = self.partial_scores[player_id]
        return self._send(found)

    def _send(self, entries: dict[str, int]) -> dict[str, int]:
        self.entries_sent += len(entries)
        return entries


def rank(totals: dict[str, int], k: int) -> Ranking:
    """Sort by score (high first), then player id so ties are deterministic."""
    ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    return ordered[:k]


def naive_merged_top_k(shards: list[ScoreShard], k: int) -> Ranking:
    """Original leaderboard: sum each shard's local top-k and rank that."""
    totals: dict[str, int] = {}
    for shard in shards:
        for player_id, score in shard.local_top(k).items():
            totals[player_id] = totals.get(player_id, 0) + score
    return rank(totals, k)


def exact_top_k_full_scan(shards: list[ScoreShard], k: int) -> Ranking:
    """Ground truth: pull every partial score. Correct but sends everything."""
    totals: dict[str, int] = {}
    for shard in shards:
        for player_id, score in shard.scores_at_least(0).items():
            totals[player_id] = totals.get(player_id, 0) + score
    return rank(totals, k)


def kth_highest(values: list[int | float], k: int) -> float:
    """The k-th largest value, or 0 if there are fewer than k values."""
    if len(values) < k:
        return 0
    return sorted(values, reverse=True)[k - 1]


def tput_top_k(shards: list[ScoreShard], k: int) -> Ranking:
    """Exact global top-k using the three-phase uniform threshold algorithm."""
    shard_count = len(shards)
    if shard_count == 0 or k <= 0:
        return []

    # Phase 1: local top-k from each shard gives lower bounds on totals.
    # The k-th best lower bound is a score the final top-k must reach.
    phase1 = collect([shard.local_top(k) for shard in shards])
    phase1_threshold = kth_highest([sum(parts.values()) for parts in phase1.values()], k)

    # Phase 2: a player can only reach that score if at least one shard holds
    # >= threshold / shard_count of it (pigeonhole), so fetch exactly those.
    per_shard_floor = phase1_threshold / shard_count
    seen = collect([shard.scores_at_least(per_shard_floor) for shard in shards])
    # Phase-1 scores below the floor are still exact; keep them for tighter bounds.
    merge_known_scores(seen, phase1)
    candidates = players_that_can_still_reach_top_k(seen, per_shard_floor,
                                                    shard_count, k)

    # Phase 3: exact totals for the survivors only.
    exact = collect([shard.scores_for(candidates) for shard in shards])
    return rank({player: sum(parts.values()) for player, parts in exact.items()}, k)


def players_that_can_still_reach_top_k(seen: dict[str, dict[int, int]],
                                       per_shard_floor: float,
                                       shard_count: int, k: int) -> set[str]:
    """Drop players whose best possible total is below the k-th known total."""
    lower_bounds = {player: sum(parts.values()) for player, parts in seen.items()}
    kth_lower_bound = kth_highest(list(lower_bounds.values()), k)
    candidates = set()
    for player_id, parts in seen.items():
        # A shard that never reported a player holds less than the floor for
        # it, so known + floor * missing_shards bounds the true total.
        missing_shards = shard_count - len(parts)
        upper_bound = lower_bounds[player_id] + per_shard_floor * missing_shards
        if upper_bound >= kth_lower_bound:
            candidates.add(player_id)
    return candidates


def collect(replies: list[dict[str, int]]) -> dict[str, dict[int, int]]:
    """Group shard replies as player -> {shard index: partial score}."""
    grouped: dict[str, dict[int, int]] = {}
    for shard_index, reply in enumerate(replies):
        for player_id, score in reply.items():
            grouped.setdefault(player_id, {})[shard_index] = score
    return grouped


def merge_known_scores(target: dict[str, dict[int, int]],
                       extra: dict[str, dict[int, int]]) -> None:
    """Add exact partial scores from ``extra`` to players already in ``target``.

    Players only seen in ``extra`` are skipped: every one of their partial
    scores is below the floor, so their total cannot reach the top-k.
    """
    for player_id, parts in extra.items():
        if player_id in target:
            for shard_index, score in parts.items():
                target[player_id].setdefault(shard_index, score)
