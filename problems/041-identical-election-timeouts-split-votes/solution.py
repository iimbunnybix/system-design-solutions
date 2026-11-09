"""Elect a new leader quickly after a crash by randomizing election timeouts.

A five-node Raft-style metadata cluster lost its leader. Every follower used
the same fixed election timeout, and they had all heard the last heartbeat at
almost the same moment, so they all became candidates at almost the same
moment, voted for themselves and refused each other: a split vote. Then
they all timed out together again, term after term, with no leader.

The fix is the one from the Raft paper: each follower picks a new random
election timeout (here 150-300 ms) every time it resets its timer. Usually one
node times out clearly first and collects votes before the others wake up.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field
from typing import Callable

CLUSTER_SIZE = 5
CRASHED_LEADER = 0
FIXED_TIMEOUT_MS = 150
RANDOM_TIMEOUT_MIN_MS = 150
RANDOM_TIMEOUT_MAX_MS = 300
HEARTBEAT_SKEW_MAX_MS = 2     # followers heard the last heartbeat 0-2 ms apart
MIN_DELAY_MS = 3              # one-way message delay between nodes
MAX_DELAY_MS = 8
GIVE_UP_AFTER_MS = 10_000     # report "no leader" if none is elected by then

TimeoutPolicy = Callable[[random.Random], int]


def naive_fixed_timeout(rng: random.Random) -> int:
    """The original policy: every node waits exactly the same time."""
    return FIXED_TIMEOUT_MS


def randomized_timeout(rng: random.Random) -> int:
    """The fix: a fresh random timeout on every reset breaks the symmetry."""
    return rng.randint(RANDOM_TIMEOUT_MIN_MS, RANDOM_TIMEOUT_MAX_MS)


@dataclass
class Node:
    """Election state of one live node."""
    node_id: int
    term: int = 0
    voted_for: int | None = None
    is_candidate: bool = False
    votes: set[int] = field(default_factory=set)
    timer_generation: int = 0     # bumps on reset, so old timer events are ignored


@dataclass
class ElectionResult:
    leader: int | None        # None: no leader within GIVE_UP_AFTER_MS
    elected_at_ms: int | None
    elections_started: int    # candidacies, one per (node, term)
    highest_term: int


class Election:
    """Discrete-event simulation of Raft leader election after the leader dies."""

    def __init__(self, policy: TimeoutPolicy, seed: int,
                 cluster_size: int = CLUSTER_SIZE) -> None:
        self.policy = policy
        self.rng = random.Random(seed)
        self.cluster_size = cluster_size
        self.majority = cluster_size // 2 + 1   # counts the dead node too
        self.nodes = {i: Node(i) for i in range(cluster_size) if i != CRASHED_LEADER}
        self.events: list[tuple[int, int, str, tuple]] = []
        self.sequence = 0
        self.elections_started = 0
        self.leader: int | None = None
        self.elected_at_ms: int | None = None

    def schedule(self, at_ms: int, kind: str, args: tuple) -> None:
        """Queue an event; the sequence number keeps ties in insertion order."""
        self.sequence += 1
        heapq.heappush(self.events, (at_ms, self.sequence, kind, args))

    def reset_timer(self, node: Node, now_ms: int) -> None:
        """Start a new election timer and invalidate the previous one."""
        node.timer_generation += 1
        deadline = now_ms + self.policy(self.rng)
        self.schedule(deadline, "timeout", (node.node_id, node.timer_generation))

    def send(self, now_ms: int, kind: str, args: tuple) -> None:
        """Deliver a message after a random network delay."""
        self.schedule(now_ms + self.rng.randint(MIN_DELAY_MS, MAX_DELAY_MS), kind, args)

    def on_timeout(self, now_ms: int, node_id: int, generation: int) -> None:
        """No heartbeat and no vote granted in time: become a candidate."""
        node = self.nodes[node_id]
        if generation != node.timer_generation:
            return
        node.term += 1
        node.is_candidate = True
        node.voted_for = node_id
        node.votes = {node_id}
        self.elections_started += 1
        for other in self.nodes:
            if other != node_id:
                self.send(now_ms, "request_vote", (other, node_id, node.term))
        self.reset_timer(node, now_ms)

    def on_request_vote(self, now_ms: int, voter_id: int, candidate_id: int,
                        term: int) -> None:
        """Grant at most one vote per term (Raft's rule; log checks omitted)."""
        voter = self.nodes[voter_id]
        if term > voter.term:
            # A newer term always wins: step down and forget the old vote.
            voter.term = term
            voter.is_candidate = False
            voter.voted_for = None
        granted = term == voter.term and voter.voted_for in (None, candidate_id)
        if granted:
            voter.voted_for = candidate_id
            self.reset_timer(voter, now_ms)    # give the candidate time to win
        self.send(now_ms, "vote_reply", (candidate_id, voter_id, term, granted))

    def on_vote_reply(self, now_ms: int, candidate_id: int, voter_id: int,
                      term: int, granted: bool) -> None:
        """Count votes for the current term; a majority makes a leader."""
        candidate = self.nodes[candidate_id]
        if not (granted and candidate.is_candidate and term == candidate.term):
            return
        candidate.votes.add(voter_id)
        if len(candidate.votes) >= self.majority:
            self.leader = candidate_id
            self.elected_at_ms = now_ms

    def run(self) -> ElectionResult:
        """Leader dies at t=0; followers last heard it 0-2 ms apart."""
        for node in self.nodes.values():
            heard_at = self.rng.randint(0, HEARTBEAT_SKEW_MAX_MS)
            self.reset_timer(node, heard_at)
        while self.events and self.leader is None:
            now_ms, _, kind, args = heapq.heappop(self.events)
            if now_ms > GIVE_UP_AFTER_MS:
                break
            if kind == "timeout":
                self.on_timeout(now_ms, *args)
            elif kind == "request_vote":
                self.on_request_vote(now_ms, *args)
            else:
                self.on_vote_reply(now_ms, *args)
        highest_term = max(node.term for node in self.nodes.values())
        return ElectionResult(self.leader, self.elected_at_ms,
                              self.elections_started, highest_term)


@dataclass
class FailoverReport:
    trials: int
    no_leader: int            # trials with no leader after GIVE_UP_AFTER_MS
    median_ms: int | None     # median time to a leader, over trials that got one
    worst_ms: int | None


def simulate_failovers(policy: TimeoutPolicy, trials: int = 1_000) -> FailoverReport:
    """Crash the leader once per seed and summarise how long elections take."""
    times: list[int] = []
    no_leader = 0
    for seed in range(trials):
        result = Election(policy, seed).run()
        if result.elected_at_ms is None:
            no_leader += 1
        else:
            times.append(result.elected_at_ms)
    times.sort()
    median = times[len(times) // 2] if times else None
    worst = times[-1] if times else None
    return FailoverReport(trials, no_leader, median, worst)
