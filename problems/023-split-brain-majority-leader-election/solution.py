"""Leader election that cannot produce two leaders during a network partition.

The original election counted votes only among the nodes a candidate could
reach. When a partition split a 5-node cluster into 2 + 3, each side saw a
"majority of reachable nodes", elected its own leader, and both accepted
writes, so the data diverged (split brain). This module keeps the naive
version for comparison and fixes it Raft-style: a leader needs votes from a
strict majority of the *full configured* cluster, every election starts a new
term, and a write is acknowledged only after a majority of the full cluster
accepts it. The minority side therefore refuses writes instead of diverging.
"""
from __future__ import annotations

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class NotLeader(Exception):
    """The node asked to write is not (or is no longer) the leader."""


class NotCommitted(Exception):
    """The leader could not reach a majority, so the write was refused."""


def majority_size(cluster_size: int) -> int:
    """Smallest number of nodes that is more than half of the cluster."""
    return cluster_size // 2 + 1


class Network:
    """Which nodes can talk to each other; a partition is a list of groups."""

    def __init__(self, node_names: list[str]) -> None:
        self.groups: list[set[str]] = [set(node_names)]

    def partition(self, *groups: list[str]) -> None:
        self.groups = [set(group) for group in groups]

    def heal(self) -> None:
        everyone: set[str] = set()
        for group in self.groups:
            everyone |= group
        self.groups = [everyone]

    def can_reach(self, sender: str, receiver: str) -> bool:
        for group in self.groups:
            if sender in group and receiver in group:
                return True
        return False


class Node:
    """One replica: its term, its vote in that term, its role and its data."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.current_term = 0
        self.voted_for: str | None = None
        self.role = FOLLOWER
        self.data: dict[str, object] = {}

    def observe_term(self, term: int) -> None:
        """Adopt a newer term; seeing one means someone else may lead now."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.role = FOLLOWER

    def handle_vote_request(self, term: int, candidate: str) -> bool:
        """Grant at most one vote per term, never to an older term."""
        if term < self.current_term:
            return False
        self.observe_term(term)
        if self.voted_for is None or self.voted_for == candidate:
            self.voted_for = candidate
            return True
        return False


class NaiveCluster:
    """The original design: 'majority' means majority of reachable nodes."""

    def __init__(self, node_names: list[str]) -> None:
        self.nodes = {name: Node(name) for name in node_names}
        self.network = Network(node_names)

    def reachable_from(self, name: str) -> list[Node]:
        reachable = []
        for other in self.nodes.values():
            if self.network.can_reach(name, other.name):
                reachable.append(other)
        return reachable

    def elect(self, candidate_name: str) -> bool:
        """Become leader if most of the nodes *we can see* agree (the bug)."""
        reachable = self.reachable_from(candidate_name)
        # Every reachable node votes yes, and the threshold is computed from
        # the reachable side only, so each side of a partition always "wins".
        votes = len(reachable)
        if votes >= majority_size(len(reachable)):
            self.nodes[candidate_name].role = LEADER
            return True
        return False

    def write(self, leader_name: str, key: str, value: object) -> None:
        """Apply the write wherever the leader can reach, then acknowledge."""
        if self.nodes[leader_name].role != LEADER:
            raise NotLeader(leader_name)
        for node in self.reachable_from(leader_name):
            node.data[key] = value


class QuorumCluster(NaiveCluster):
    """The fix: votes and writes both need a majority of the configured cluster."""

    def quorum(self) -> int:
        # Size of the configured membership, never of the reachable subset:
        # two disjoint sides cannot both hold more than half of it.
        return majority_size(len(self.nodes))

    def elect(self, candidate_name: str) -> bool:
        """Start a new term and ask every reachable node for its vote."""
        candidate = self.nodes[candidate_name]
        candidate.current_term += 1
        candidate.voted_for = candidate_name
        candidate.role = CANDIDATE
        votes = 1
        for node in self.reachable_from(candidate_name):
            if node is candidate:
                continue
            if node.handle_vote_request(candidate.current_term, candidate_name):
                votes += 1
        if candidate.role == CANDIDATE and votes >= self.quorum():
            candidate.role = LEADER
            return True
        candidate.role = FOLLOWER
        return False

    def write(self, leader_name: str, key: str, value: object) -> None:
        """Commit only if a majority of the full cluster accepts this term."""
        leader = self.nodes[leader_name]
        if leader.role != LEADER:
            raise NotLeader(leader_name)
        accepting = self.followers_accepting(leader)
        if leader.role != LEADER:
            raise NotLeader(f"{leader_name} saw a newer term and stepped down")
        if len(accepting) < self.quorum():
            raise NotCommitted(f"{len(accepting)} of {len(self.nodes)} nodes reachable")
        # Simplification: real Raft appends to the log first and marks the
        # entry committed later; here we only apply once a quorum is known.
        for node in accepting:
            node.data[key] = value

    def followers_accepting(self, leader: Node) -> list[Node]:
        """Reachable nodes (leader included) that accept the leader's term."""
        accepting = []
        for node in self.reachable_from(leader.name):
            if node.current_term > leader.current_term:
                # A newer leader exists: this one is stale and must step down.
                leader.observe_term(node.current_term)
                return []
            node.observe_term(leader.current_term)
            accepting.append(node)
        return accepting
