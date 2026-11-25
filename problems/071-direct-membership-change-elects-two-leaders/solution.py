"""Growing a 3-node cluster to 5 in one step let two leaders win the same term.

The incident: a Raft-style metadata cluster {A, B, C} was grown to
{A, B, C, D, E} by telling every node the new member list at once. Nodes
switch configuration at different moments, so for a while A and B still
counted votes against the old list (majority 2 of 3) while C, D and E
counted against the new one (majority 3 of 5). A network split between
{A, B} and {C, D, E} let both sides elect a leader in the same term.

The fix: change membership one server at a time, and do not start the next
change until the previous one is committed. When two configurations differ
by a single server, every majority of one overlaps every majority of the
other, so two disjoint groups of voters can never both win.
"""
from __future__ import annotations

from itertools import combinations

OLD_CLUSTER = frozenset({"A", "B", "C"})
TARGET_CLUSTER = frozenset({"A", "B", "C", "D", "E"})

Config = frozenset[str]


def majority_size(config: Config) -> int:
    """Votes needed to win an election in ``config``."""
    return len(config) // 2 + 1


def wins_election(voters: frozenset[str], config: Config) -> bool:
    """True if ``voters`` include a majority of ``config``.

    Only members of the config count: a vote from a node the candidate does
    not consider a member is ignored.
    """
    return len(voters & config) >= majority_size(config)


def elect_on_each_side(side_a: frozenset[str], config_a: Config,
                       side_b: frozenset[str], config_b: Config) -> list[str]:
    """Run one election per side of a network split, in the same term.

    Each side's candidate counts votes with the config it currently holds.
    Returns the sides that elected a leader.
    """
    leaders = []
    if wins_election(side_a, config_a):
        leaders.append("side A")
    if wins_election(side_b, config_b):
        leaders.append("side B")
    return leaders


def disjoint_winning_splits(config_one: Config, config_two: Config) -> list[tuple[frozenset[str], frozenset[str]]]:
    """Every split of the nodes into two groups where both groups can win.

    Group one counts votes with ``config_one`` and group two with
    ``config_two``. Any split returned is a way to get two leaders.
    """
    all_nodes = sorted(config_one | config_two)
    unsafe_splits = []
    for size in range(len(all_nodes) + 1):
        for group in combinations(all_nodes, size):
            group_one = frozenset(group)
            group_two = frozenset(all_nodes) - group_one
            if wins_election(group_one, config_one) and wins_election(group_two, config_two):
                unsafe_splits.append((group_one, group_two))
    return unsafe_splits


def can_elect_two_leaders(config_one: Config, config_two: Config) -> bool:
    """True if nodes holding these two configs could elect two leaders at once."""
    return bool(disjoint_winning_splits(config_one, config_two))


# ---------------------------------------------------------------- original
def naive_membership_plan(current: Config, target: Config) -> list[Config]:
    """Original change: jump straight to the target member list."""
    return [current, target]


# ---------------------------------------------------------------- the fix
def single_server_plan(current: Config, target: Config) -> list[Config]:
    """Configs to pass through, adding or removing one server per step.

    WHY add before remove: while replacing nodes, adding first keeps the
    cluster at least as fault tolerant as it was during every step.
    """
    plan = [current]
    config = set(current)
    for node in sorted(target - current):
        config.add(node)
        plan.append(frozenset(config))
    for node in sorted(current - target):
        config.discard(node)
        plan.append(frozenset(config))
    return plan


def plan_is_safe(plan: list[Config]) -> bool:
    """True if no two consecutive configs can elect two leaders.

    Only consecutive configs matter because the manager below never lets more
    than one change be in flight.
    """
    for older, newer in zip(plan, plan[1:]):
        if can_elect_two_leaders(older, newer) or can_elect_two_leaders(newer, older):
            return False
    return True


class ChangeInProgress(Exception):
    """A membership change was proposed before the previous one committed."""


class MembershipManager:
    """Accepts membership changes one server at a time, one change at a time."""

    def __init__(self, config: Config) -> None:
        self.committed = config
        self.pending: Config | None = None

    def propose(self, new_config: Config) -> None:
        """Start a change; refuse a second one or a change of more than one node.

        WHY wait for commit: with A->B still in flight, starting B->C would let
        nodes on A and nodes on C vote at the same time, and A and C differ by
        two servers.
        """
        if self.pending is not None:
            raise ChangeInProgress("previous membership change is not committed yet")
        changed_nodes = self.committed ^ new_config
        if len(changed_nodes) != 1:
            raise ValueError(f"change exactly one server at a time, not {len(changed_nodes)}")
        self.pending = new_config

    def commit(self) -> None:
        """The pending config reached a majority of itself; it is now in force."""
        if self.pending is None:
            raise RuntimeError("no membership change to commit")
        self.committed = self.pending
        self.pending = None

    def configs_in_use(self) -> list[Config]:
        """Configs that some node may be counting votes with right now."""
        if self.pending is None:
            return [self.committed]
        return [self.committed, self.pending]


def grow_cluster(manager: MembershipManager, target: Config) -> list[Config]:
    """Walk the manager to ``target`` one committed step at a time."""
    history = [manager.committed]
    for step in single_server_plan(manager.committed, target)[1:]:
        manager.propose(step)
        manager.commit()
        history.append(manager.committed)
    return history
