"""Linearizable reads from a leader: leader lease plus read-index fallback.

A leader cut off from the rest of the cluster by a network partition does not
know it has been replaced. The original read path answered every read from
the leader's local copy, so for as long as the partition lasted clients of the
old leader read values that the new leader had already overwritten.

The fix: a leader may answer a read locally only while it holds a lease,
which a majority granted by acknowledging a heartbeat. Followers promise not
to elect anyone else until an election timeout has passed since that
heartbeat, and the lease is shorter than that timeout by a clock-drift margin,
so the lease always ends before a new leader can exist. When the lease has
expired, the leader must confirm leadership with a majority again (a "read
index" round) before answering, or refuse the read.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# A follower only votes for a new leader after hearing nothing from the
# current one for this long.
ELECTION_TIMEOUT_MS = 1_000

# Upper bound on how much faster one node's clock can run than another's
# over one lease. The lease is shortened by this much to stay safe.
MAX_CLOCK_DRIFT_MS = 100

LEASE_DURATION_MS = ELECTION_TIMEOUT_MS - MAX_CLOCK_DRIFT_MS


class NotLeader(Exception):
    """The node cannot prove it is still the leader, so it must not answer."""


class FakeClock:
    """Shared test clock in milliseconds."""

    def __init__(self) -> None:
        self.now_ms = 0

    def advance(self, milliseconds: int) -> None:
        """Move time forward."""
        self.now_ms += milliseconds


@dataclass
class Node:
    """One replica of a small key-value store."""

    name: str
    term: int = 0
    data: dict[str, int] = field(default_factory=dict)
    # When this node last heard a heartbeat from a leader (None = never).
    last_heartbeat_ms: int | None = None
    # Only meaningful on a leader: until when it may answer reads locally.
    lease_expires_ms: int = -1


def heard_from_leader_recently(node: Node, now_ms: int) -> bool:
    """True if the node's election timer has not run out yet."""
    if node.last_heartbeat_ms is None:
        return False
    return now_ms - node.last_heartbeat_ms < ELECTION_TIMEOUT_MS


class Cluster:
    """Nodes, a network that can be partitioned, and a simple election rule."""

    def __init__(self, names: list[str], clock: FakeClock) -> None:
        self.clock = clock
        self.nodes = {name: Node(name) for name in names}
        self.blocked_links: set[frozenset[str]] = set()
        self.heartbeat_rounds = 0

    def majority(self) -> int:
        """Votes or acknowledgements needed from the configured cluster."""
        return len(self.nodes) // 2 + 1

    def partition(self, side_a: list[str], side_b: list[str]) -> None:
        """Cut every link between the two sides."""
        for left in side_a:
            for right in side_b:
                self.blocked_links.add(frozenset((left, right)))

    def heal(self) -> None:
        """Restore every link."""
        self.blocked_links.clear()

    def can_reach(self, sender: str, receiver: str) -> bool:
        """True if a message from sender gets to receiver."""
        return frozenset((sender, receiver)) not in self.blocked_links

    def reachable_from(self, sender: str) -> list[Node]:
        """All nodes (including the sender itself) the sender can talk to."""
        found = []
        for name, node in self.nodes.items():
            if name == sender or self.can_reach(sender, name):
                found.append(node)
        return found

    def elect(self, candidate: str) -> bool:
        """Try to make ``candidate`` leader of a new term.

        A node votes only if it has not heard from a leader for a full
        election timeout. That promise is what makes leases safe.
        """
        now = self.clock.now_ms
        voters = []
        for node in self.reachable_from(candidate):
            if node.name == candidate or not heard_from_leader_recently(node, now):
                voters.append(node)
        if len(voters) < self.majority():
            return False
        new_term = max(node.term for node in self.nodes.values()) + 1
        for node in voters:
            node.term = new_term
            node.last_heartbeat_ms = now
        return True

    def heartbeat(self, leader: str) -> bool:
        """Send one heartbeat round; True if a majority acknowledged this term.

        On success the leader's lease is renewed from the moment the round
        *started*, because followers reset their election timers no earlier
        than that.
        """
        self.heartbeat_rounds += 1
        round_started_ms = self.clock.now_ms
        leader_node = self.nodes[leader]
        acknowledgements = 0
        for node in self.reachable_from(leader):
            if node.term > leader_node.term:
                # Someone newer exists: step down by giving up the lease.
                leader_node.lease_expires_ms = -1
                return False
            node.term = leader_node.term
            node.last_heartbeat_ms = round_started_ms
            acknowledgements += 1
        if acknowledgements < self.majority():
            return False
        leader_node.lease_expires_ms = round_started_ms + LEASE_DURATION_MS
        return True

    def write(self, leader: str, key: str, value: int) -> None:
        """Commit a write through ``leader`` on a majority, or raise NotLeader."""
        if not self.heartbeat(leader):
            raise NotLeader(f"{leader} cannot reach a majority")
        for node in self.reachable_from(leader):
            node.data[key] = value


def naive_read(cluster: Cluster, leader: str, key: str) -> int | None:
    """Original read path: answer from the leader's local copy, no checks."""
    return cluster.nodes[leader].data.get(key)


def linearizable_read(cluster: Cluster, leader: str, key: str) -> int | None:
    """Answer only when ``leader`` can prove it is still the leader.

    Inside the lease the local copy is current, because no other leader can
    have been elected yet. After the lease, confirm with a majority first.
    """
    node = cluster.nodes[leader]
    lease_is_valid = cluster.clock.now_ms < node.lease_expires_ms
    if not lease_is_valid and not cluster.heartbeat(leader):
        raise NotLeader(f"{leader} lost its lease and cannot reach a majority")
    return node.data.get(key)
