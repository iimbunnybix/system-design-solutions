"""Give each primary in an active-active pair its own auto-increment sequence.

Orders were written to two primaries (one per region) that replicate to each
other. Both used auto-increment with step 1 from the same starting point, so
both handed out the same order ids. Replication hit duplicate-key errors,
which had been configured to be skipped, so the other region's order with
that id was silently dropped.

Fix: interleave the sequences (MySQL's auto_increment_increment and
auto_increment_offset): with step N, node k only generates ids k, k+N, k+2N...
A config check rejects overlapping settings before a node starts.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OrderNode:
    """One primary with its own id sequence and its copy of the orders table."""

    name: str
    increment: int = 1           # auto_increment_increment
    offset: int = 1              # auto_increment_offset
    orders: dict[int, str] = field(default_factory=dict)
    skipped_duplicates: int = 0

    def next_id(self) -> int:
        """Smallest id above the current max that belongs to this node's sequence.

        WHY "above the current max": replicated rows raise the max too, which
        is how MySQL picks the next value; it keeps ids roughly increasing.
        """
        current_max = max(self.orders, default=0)
        candidate = self.offset
        if current_max >= self.offset:
            steps = (current_max - self.offset) // self.increment + 1
            candidate = self.offset + steps * self.increment
        return candidate

    def insert_local(self, description: str) -> int:
        order_id = self.next_id()
        self.orders[order_id] = description
        return order_id

    def apply_replicated(self, order_id: int, description: str) -> None:
        """Apply a row from the other primary; duplicates are skipped (the old setting)."""
        if order_id in self.orders:
            # WHY this mattered: "skip duplicate-key errors" kept replication
            # running, but it threw away the other region's order.
            self.skipped_duplicates += 1
            return
        self.orders[order_id] = description


def replicate(source: OrderNode, target: OrderNode, order_ids: list[int]) -> None:
    """Ship rows written on ``source`` to ``target``."""
    for order_id in order_ids:
        target.apply_replicated(order_id, source.orders[order_id])


def run_concurrent_traffic(east: OrderNode, west: OrderNode, rounds: int) -> list[str]:
    """Both regions take one order per round, then replicate. Returns every order placed."""
    placed = []
    for round_number in range(rounds):
        east_description = f"east-order-{round_number}"
        west_description = f"west-order-{round_number}"
        east_id = east.insert_local(east_description)
        west_id = west.insert_local(west_description)
        placed.extend([east_description, west_description])
        # WHY replicate after both inserts: real replication lag means each
        # region picks its next id before it has seen the other's newest row.
        replicate(east, west, [east_id])
        replicate(west, east, [west_id])
    return placed


def lost_orders(placed: list[str], node: OrderNode) -> list[str]:
    """Orders that were acknowledged somewhere but are missing on ``node``."""
    stored = set(node.orders.values())
    missing = []
    for description in placed:
        if description not in stored:
            missing.append(description)
    return missing


def check_sequence_config(nodes: list[OrderNode]) -> list[str]:
    """Problems that would let two nodes generate the same id (empty list = safe)."""
    problems = []
    increments = {node.increment for node in nodes}
    if len(increments) != 1:
        problems.append("all nodes must use the same increment")
        return problems
    increment = increments.pop()
    if increment < len(nodes):
        problems.append(f"increment {increment} is smaller than the {len(nodes)} nodes")
    seen_offsets: dict[int, str] = {}
    for node in nodes:
        if not 1 <= node.offset <= increment:
            problems.append(f"{node.name}: offset {node.offset} must be between 1 and {increment}")
        elif node.offset in seen_offsets:
            problems.append(f"{node.name}: offset {node.offset} already used by "
                            f"{seen_offsets[node.offset]}")
        else:
            seen_offsets[node.offset] = node.name
    return problems
