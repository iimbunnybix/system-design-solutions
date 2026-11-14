"""Gossip membership where a stale "alive" rumour can no longer revive a dead node.

A cache cluster learns which nodes are up through gossip: each node passes on
what it has heard about every member. The original merge kept whatever message
arrived last, so after one node declared a crashed member dead, peers that had
not heard yet kept gossiping the older "alive" view and the member flipped back
to alive; clients were routed to a dead node. This module adds SWIM-style
incarnation numbers: a newer incarnation always wins, and for the same
incarnation "dead" beats "suspect" beats "alive". Only the member itself may
raise its incarnation, which is how a live node refutes a false suspicion.
"""
from __future__ import annotations

from dataclasses import dataclass

ALIVE = "alive"
SUSPECT = "suspect"
DEAD = "dead"

# For the same incarnation, a more pessimistic status is newer information:
# nobody goes from "dead" back to "alive" without a new incarnation.
STATUS_RANK = {ALIVE: 0, SUSPECT: 1, DEAD: 2}


@dataclass(frozen=True)
class MemberState:
    """What one gossip message says about one member."""
    status: str
    incarnation: int


@dataclass(frozen=True)
class GossipMessage:
    member: str
    state: MemberState


class NaiveMembershipTable:
    """Original version: the last message received about a member wins."""

    def __init__(self) -> None:
        self.members: dict[str, MemberState] = {}

    def apply(self, message: GossipMessage) -> None:
        """Overwrite our view with whatever the peer said, however old it is."""
        self.members[message.member] = message.state

    def routable_members(self) -> list[str]:
        """Members clients may be sent to."""
        return sorted(name for name, state in self.members.items() if state.status == ALIVE)


def overrides(current: MemberState, incoming: MemberState) -> bool:
    """Return True if `incoming` is newer information than `current`.

    A higher incarnation is always newer. With equal incarnations the more
    severe status wins, so a stale "alive" cannot undo a "dead" verdict.
    """
    if incoming.incarnation != current.incarnation:
        return incoming.incarnation > current.incarnation
    return STATUS_RANK[incoming.status] > STATUS_RANK[current.status]


class MembershipTable:
    """One node's view of the cluster, merged with incarnation precedence."""

    def __init__(self, self_name: str) -> None:
        self.self_name = self_name
        self.self_incarnation = 0
        self.members: dict[str, MemberState] = {self_name: MemberState(ALIVE, 0)}

    def apply(self, message: GossipMessage) -> GossipMessage | None:
        """Merge one gossip message; return a message to broadcast, if any.

        A rumour that we ourselves are suspect or dead is not stored: we are
        alive, so we refute it with a higher incarnation and gossip that.
        """
        if message.member == self.self_name:
            return self._refute_if_needed(message.state)
        current = self.members.get(message.member)
        if current is None or overrides(current, message.state):
            self.members[message.member] = message.state
            # Changed information is worth passing on to other peers.
            return message
        return None

    def _refute_if_needed(self, rumour: MemberState) -> GossipMessage | None:
        """Answer a suspicion about ourselves by bumping our incarnation."""
        if rumour.status == ALIVE or rumour.incarnation < self.self_incarnation:
            return None
        self.self_incarnation = rumour.incarnation + 1
        refutation = MemberState(ALIVE, self.self_incarnation)
        self.members[self.self_name] = refutation
        return GossipMessage(self.self_name, refutation)

    def routable_members(self) -> list[str]:
        """Members clients may be sent to (suspects are still served, like SWIM)."""
        routable = []
        for name, state in self.members.items():
            if state.status != DEAD:
                routable.append(name)
        return sorted(routable)

    def status_of(self, member: str) -> str | None:
        """Current status of a member, or None if we have never heard of it."""
        state = self.members.get(member)
        if state is None:
            return None
        return state.status


def count_status_flips(statuses: list[str]) -> int:
    """How many times a member's observed status changed in a sequence."""
    flips = 0
    for index in range(1, len(statuses)):
        if statuses[index] != statuses[index - 1]:
            flips += 1
    return flips
