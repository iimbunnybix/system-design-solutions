"""Retry on a different replica instead of hammering the one that just failed.

A key-value read client picked one replica per key with rendezvous hashing
(for cache locality) and, on a timeout, retried the same request three times.
Every retry went to the same replica, so when one node of twelve died, every
key whose first choice was that node failed all of its attempts, even though
two healthy copies of the data existed. The fix walks the key's replica
preference list on retry, never repeating a replica already tried, so one
dead node costs one extra attempt instead of a failed request.
"""
from __future__ import annotations

import hashlib

NODE_COUNT = 12
REPLICATION_FACTOR = 3        # copies of every key
MAX_ATTEMPTS = 3              # first try plus two retries


class NodeDown(Exception):
    """The chosen replica did not answer (crashed, partitioned, timed out)."""


class AllAttemptsFailed(Exception):
    """Every attempt the client was allowed to make failed."""


def node_names(count: int = NODE_COUNT) -> list[str]:
    return [f"kv-{number:02d}" for number in range(count)]


def rendezvous_score(key: str, node: str) -> int:
    """Stable score of a (key, node) pair; the highest scores own the key."""
    digest = hashlib.sha256(f"{key}|{node}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def preference_list(key: str, nodes: list[str],
                    replicas: int = REPLICATION_FACTOR) -> list[str]:
    """The replicas holding key, best first (rendezvous / highest-random-weight)."""
    scored = []
    for node in nodes:
        scored.append((rendezvous_score(key, node), node))
    scored.sort(reverse=True)
    best = []
    for _score, node in scored[:replicas]:
        best.append(node)
    return best


class Cluster:
    """Fake storage nodes; a node in `down` raises NodeDown on every request."""

    def __init__(self, nodes: list[str]) -> None:
        self.nodes = nodes
        self.down: set[str] = set()
        self.requests_per_node: dict[str, int] = {}
        for node in nodes:
            self.requests_per_node[node] = 0

    def read(self, node: str, key: str) -> str:
        self.requests_per_node[node] += 1
        if node in self.down:
            raise NodeDown(node)
        return f"value-of-{key}"


def naive_get(cluster: Cluster, key: str, max_attempts: int = MAX_ATTEMPTS) -> str:
    """Original client: choose the first replica once, then retry it."""
    node = preference_list(key, cluster.nodes)[0]
    for _attempt in range(max_attempts):
        try:
            return cluster.read(node, key)
        except NodeDown:
            # The bug: the retry reuses the same node, so a dead node
            # fails every attempt even though other replicas hold the data.
            continue
    raise AllAttemptsFailed(key)


def replicas_to_try(key: str, nodes: list[str], max_attempts: int) -> list[str]:
    """One distinct replica per attempt, in preference order.

    If there are fewer replicas than attempts, we wrap around: a replica that
    timed out may have recovered, and it is still better than giving up.
    """
    replicas = preference_list(key, nodes)
    order = []
    for attempt in range(max_attempts):
        order.append(replicas[attempt % len(replicas)])
    return order


def failover_get(cluster: Cluster, key: str, max_attempts: int = MAX_ATTEMPTS) -> str:
    """Fixed client: each retry goes to the next replica in the preference list."""
    for node in replicas_to_try(key, cluster.nodes, max_attempts):
        try:
            return cluster.read(node, key)
        except NodeDown:
            continue
    raise AllAttemptsFailed(key)


def failure_rate(cluster: Cluster, keys: list[str], get_function) -> float:
    """Share of keys whose read failed after all attempts."""
    failures = 0
    for key in keys:
        try:
            get_function(cluster, key)
        except AllAttemptsFailed:
            failures += 1
    return failures / len(keys)
