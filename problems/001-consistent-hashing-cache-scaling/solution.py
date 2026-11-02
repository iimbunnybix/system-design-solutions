"""Consistent hash ring with virtual nodes and weights."""
from __future__ import annotations

import bisect
import hashlib


def _h(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:4], "big")   # placement, not security


class ConsistentHashRing:
    def __init__(self, vnodes: int = 150):
        self.vnodes = vnodes
        self._ring: list[int] = []             # sorted positions
        self._owner: dict[int, str] = {}       # position -> node
        self.weights: dict[str, float] = {}

    def add_node(self, node: str, weight: float = 1.0) -> None:
        if node in self.weights:
            raise ValueError(f"{node} already in ring")
        self.weights[node] = weight
        for i in range(max(1, int(self.vnodes * weight))):
            pos = _h(f"{node}#{i}")
            while pos in self._owner:              # extremely rare collision
                pos = (pos + 1) % 2 ** 32
            self._owner[pos] = node
            bisect.insort(self._ring, pos)

    def remove_node(self, node: str) -> None:
        if node not in self.weights:
            raise KeyError(node)
        del self.weights[node]
        self._ring = [p for p in self._ring if self._owner[p] != node]
        self._owner = {p: n for p, n in self._owner.items() if n != node}

    def get_node(self, key: str) -> str:
        if not self._ring:
            raise RuntimeError("ring is empty")
        idx = bisect.bisect(self._ring, _h(key)) % len(self._ring)
        return self._owner[self._ring[idx]]


class ModuloSharding:
    """The original approach, kept for comparison."""

    def __init__(self, nodes: list[str]):
        self.nodes = list(nodes)

    def get_node(self, key: str) -> str:
        return self.nodes[_h(key) % len(self.nodes)]


def moved_fraction(before, after, keys) -> float:
    return sum(before.get_node(k) != after.get_node(k) for k in keys) / len(keys)
