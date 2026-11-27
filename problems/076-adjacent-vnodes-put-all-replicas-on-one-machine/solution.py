"""Keys lost all three "replicas" when a single storage node died.

The store places each key on a consistent-hash ring with virtual nodes and
takes the next 3 vnodes clockwise as the key's 3 replicas. Vnodes of the same
physical machine are scattered at random, so they are often neighbours: many
keys get two or even three copies on one machine (and one rack), and losing
that machine loses those keys. The fix keeps walking clockwise, skipping
vnodes whose machine (and, when possible, rack) is already chosen, until it
has 3 distinct machines, and copes with clusters smaller than 3 machines.
"""
from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass

REPLICATION_FACTOR = 3
VNODES_PER_NODE = 16
KEY_COUNT = 20_000
# Six storage machines in three racks, two machines per rack.
NODE_RACKS = {
    "node-1": "rack-a", "node-2": "rack-a",
    "node-3": "rack-b", "node-4": "rack-b",
    "node-5": "rack-c", "node-6": "rack-c",
}


def ring_position(label: str) -> int:
    """Stable 64-bit position on the ring.

    WHY hashlib: Python's built-in hash() of a str changes between processes,
    so every server would build a different ring.
    """
    digest = hashlib.md5(label.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


@dataclass(frozen=True)
class VirtualNode:
    """One point on the ring, owned by a physical machine."""

    position: int
    node: str


class HashRing:
    """A sorted ring of virtual nodes for a set of machines."""

    def __init__(self, node_racks: dict[str, str], vnodes_per_node: int = VNODES_PER_NODE) -> None:
        """Place ``vnodes_per_node`` points per machine on the ring."""
        self.node_racks = dict(node_racks)
        vnodes = []
        for node in node_racks:
            for index in range(vnodes_per_node):
                vnodes.append(VirtualNode(ring_position(f"{node}#vnode-{index}"), node))
        self.vnodes = sorted(vnodes, key=lambda vnode: vnode.position)
        self.positions = [vnode.position for vnode in self.vnodes]

    def walk_clockwise(self, key: str) -> list[VirtualNode]:
        """Every vnode once, starting at the first one clockwise from the key."""
        start = bisect.bisect_right(self.positions, ring_position(key))
        walk = []
        for step in range(len(self.vnodes)):
            # WHY modulo: past the largest position the ring wraps to the start.
            walk.append(self.vnodes[(start + step) % len(self.vnodes)])
        return walk


# ---------------------------------------------------------------- original
def naive_replicas(ring: HashRing, key: str, count: int = REPLICATION_FACTOR) -> list[str]:
    """Original: the machines owning the next ``count`` vnodes, duplicates and all."""
    replicas = []
    for vnode in ring.walk_clockwise(key)[:count]:
        replicas.append(vnode.node)
    return replicas


# ---------------------------------------------------------------- the fix
def distinct_replicas(ring: HashRing, key: str, count: int = REPLICATION_FACTOR,
                      rack_aware: bool = True) -> list[str]:
    """Walk clockwise until ``count`` distinct machines, preferring distinct racks.

    Machines skipped only because their rack was taken are kept in ring order
    and used to top up when there are fewer racks than replicas.
    """
    # WHY min: a 2-machine cluster cannot hold 3 distinct copies; return what exists.
    target = min(count, len(ring.node_racks))
    chosen: list[str] = []
    used_racks: set[str] = set()
    skipped_for_rack: list[str] = []
    for vnode in ring.walk_clockwise(key):
        node = vnode.node
        if node in chosen or node in skipped_for_rack:
            continue                                   # another vnode of a machine we already handled
        rack = ring.node_racks[node]
        if rack_aware and rack in used_racks:
            skipped_for_rack.append(node)
            continue
        chosen.append(node)
        used_racks.add(rack)
        if len(chosen) == target:
            return chosen
    # Fewer racks than replicas: fill up with the next machines on used racks.
    for node in skipped_for_rack:
        if len(chosen) < target:
            chosen.append(node)
    return chosen


# ---------------------------------------------------------------- measuring
def all_keys(count: int = KEY_COUNT) -> list[str]:
    """Deterministic key names, so every run measures the same keys."""
    return [f"user:{number}" for number in range(count)]


def placement_report(ring: HashRing, placements: dict[str, list[str]]) -> dict[str, float]:
    """Fractions of keys with fewer than 3 distinct machines or racks."""
    fewer_nodes = 0
    fewer_racks = 0
    for replicas in placements.values():
        if len(set(replicas)) < REPLICATION_FACTOR:
            fewer_nodes += 1
        racks = {ring.node_racks[node] for node in replicas}
        if len(racks) < REPLICATION_FACTOR:
            fewer_racks += 1
    total = len(placements)
    return {"fewer_than_3_nodes": fewer_nodes / total, "fewer_than_3_racks": fewer_racks / total}


def keys_lost_when_failed(placements: dict[str, list[str]], failed_nodes: set[str]) -> int:
    """Keys whose every replica sits on a failed machine (data gone)."""
    lost = 0
    for replicas in placements.values():
        if set(replicas) <= failed_nodes:
            lost += 1
    return lost


def keys_without_quorum(placements: dict[str, list[str]], failed_nodes: set[str]) -> int:
    """Keys with fewer than 2 surviving copies, so quorum reads/writes fail."""
    quorum = REPLICATION_FACTOR // 2 + 1
    unavailable = 0
    for replicas in placements.values():
        surviving = 0
        for node in replicas:
            if node not in failed_nodes:
                surviving += 1
        if surviving < quorum:
            unavailable += 1
    return unavailable


def place_all(ring: HashRing, use_fix: bool, keys: list[str]) -> dict[str, list[str]]:
    """Replica list for every key, using the original or the fixed placement."""
    placements = {}
    for key in keys:
        if use_fix:
            placements[key] = distinct_replicas(ring, key)
        else:
            placements[key] = naive_replicas(ring, key)
    return placements


def worst_single_failure(ring: HashRing, placements: dict[str, list[str]]) -> dict[str, int]:
    """Kill each machine in turn; report the worst data loss and quorum loss."""
    worst_lost = 0
    worst_no_quorum = 0
    for node in ring.node_racks:
        worst_lost = max(worst_lost, keys_lost_when_failed(placements, {node}))
        worst_no_quorum = max(worst_no_quorum, keys_without_quorum(placements, {node}))
    return {"keys_lost": worst_lost, "keys_without_quorum": worst_no_quorum}


def nodes_in_rack(ring: HashRing, rack: str) -> set[str]:
    """All machines in one rack, for simulating a rack (switch or power) failure."""
    nodes = set()
    for node, node_rack in ring.node_racks.items():
        if node_rack == rack:
            nodes.add(node)
    return nodes
