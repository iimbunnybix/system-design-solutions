"""Count likes across regions so retried sync messages cannot double count.

Each region accepts likes locally and periodically sends the other regions
"I got N new likes since last time". The network delivers those messages at
least once: when an acknowledgement is lost, the sender retries and the
receiver adds the same delta again. Like counts drifted upwards, and
"fixing" it by keeping the max of the two totals lost likes instead.

The fix is a PN-counter (a CRDT): every region keeps, per origin region, how
many likes and unlikes that region has accepted. Syncing sends the whole
map, and merging takes the per-region maximum. Max is idempotent, so a
duplicate, late or reordered message changes nothing, and all regions
converge to the same, correct total.
"""
from __future__ import annotations

from dataclasses import dataclass, field


# --- the original, buggy versions ---------------------------------------------
@dataclass
class DeltaSyncReplica:
    """Original: ships "new likes since last sync" and adds what it receives."""

    region: str
    total: int = 0
    unsent_delta: int = 0

    def like(self) -> None:
        self.total += 1
        self.unsent_delta += 1

    def take_delta(self) -> int:
        """The message to send; the caller may resend it if the ack is lost."""
        delta, self.unsent_delta = self.unsent_delta, 0
        return delta

    def receive_delta(self, delta: int) -> None:
        # Not idempotent: the same message delivered twice is added twice.
        self.total += delta


@dataclass
class MaxTotalReplica:
    """The first "fix": keep the bigger total. Idempotent, but loses likes."""

    region: str
    total: int = 0

    def like(self) -> None:
        self.total += 1

    def receive_total(self, other_total: int) -> None:
        # Two regions that both gained likes keep only one region's gains.
        self.total = max(self.total, other_total)


# --- the fix -------------------------------------------------------------------
def merge_counts(mine: dict[str, int], theirs: dict[str, int]) -> dict[str, int]:
    """Per-region maximum: each region's count only ever grows, so max is safe."""
    merged = dict(mine)
    for region, count in theirs.items():
        if count > merged.get(region, 0):
            merged[region] = count
    return merged


@dataclass
class PNCounterReplica:
    """Likes and unlikes per origin region; the value is likes minus unlikes."""

    region: str
    likes: dict[str, int] = field(default_factory=dict)
    unlikes: dict[str, int] = field(default_factory=dict)

    def like(self) -> None:
        """Only this region's own slot is ever incremented here."""
        self.likes[self.region] = self.likes.get(self.region, 0) + 1

    def unlike(self) -> None:
        self.unlikes[self.region] = self.unlikes.get(self.region, 0) + 1

    def state(self) -> tuple[dict[str, int], dict[str, int]]:
        """A full snapshot to send; copies so later changes do not leak into it."""
        return dict(self.likes), dict(self.unlikes)

    def receive_state(self, state: tuple[dict[str, int], dict[str, int]]) -> None:
        """Merge a snapshot. Safe to call twice, late, or in any order."""
        other_likes, other_unlikes = state
        self.likes = merge_counts(self.likes, other_likes)
        self.unlikes = merge_counts(self.unlikes, other_unlikes)

    @property
    def total(self) -> int:
        return sum(self.likes.values()) - sum(self.unlikes.values())
