"""Conflict-free shopping cart for an active-active multi-region store.

The incident: carts were replicated between two regions with last-write-wins
(LWW) on the whole cart document. A customer added an item on their phone
(routed to region A) and another on their laptop (routed to region B) within
the same replication window. Both regions stored a full cart; when they
synced, the newer timestamp won and the other item silently disappeared.
Clock skew between regions made it worse: an *older* edit could win.

``ORSetCart`` is an Observed-Remove Set CRDT:

* every add creates a unique tag ``(replica_id, counter)``;
* a remove deletes only the tags the remover has **observed**;
* ``merge`` is a union of adds and removes, so it is commutative,
  associative and idempotent - replicas can sync in any order, any number of
  times, and still converge;
* a concurrent add and remove of the same item resolves to "add wins"
  (the safer choice for a cart: an extra item can be removed at checkout, a
  lost item is lost revenue).

Quantities are tracked per add-tag, so concurrent quantity changes on two
devices are both kept (the cart shows the sum of live tags for the SKU).
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field

Tag = tuple[str, int]          # (replica id, per-replica counter)


@dataclass
class LwwCart:
    """The original design: whole-cart last-write-wins by wall-clock time."""
    items: dict[str, int] = field(default_factory=dict)
    ts: float = 0.0

    def add(self, sku: str, qty: int, now: float) -> None:
        self.items[sku] = self.items.get(sku, 0) + qty
        self.ts = now

    def merge(self, other: "LwwCart") -> None:
        if other.ts > self.ts:
            self.items, self.ts = dict(other.items), other.ts


class ORSetCart:
    """Observed-remove set of cart lines; mergeable across regions."""

    def __init__(self, replica_id: str) -> None:
        self.replica_id = replica_id
        self._counter = 0
        self.adds: dict[str, dict[Tag, int]] = {}   # sku -> {tag: qty}
        self.removed: set[Tag] = set()               # tombstones

    # -- local operations ------------------------------------------------
    def _next_tag(self) -> Tag:
        self._counter += 1
        return (self.replica_id, self._counter)

    def add(self, sku: str, qty: int = 1) -> Tag:
        """Add ``qty`` of ``sku`` under a fresh tag."""
        if qty <= 0:
            raise ValueError("qty must be positive")
        tag = self._next_tag()
        self.adds.setdefault(sku, {})[tag] = qty
        return tag

    def set_quantity(self, sku: str, qty: int) -> None:
        """Replace the quantity this replica can see with ``qty`` (0 removes)."""
        self.remove(sku)
        if qty > 0:
            self.add(sku, qty)

    def remove(self, sku: str) -> None:
        """Remove ``sku``: tombstone every tag observed here, and only those."""
        self.removed |= self._live_tags(sku)

    # -- queries ---------------------------------------------------------
    def _live_tags(self, sku: str) -> set[Tag]:
        return {t for t in self.adds.get(sku, {}) if t not in self.removed}

    def items(self) -> dict[str, int]:
        out = {}
        for sku, tags in self.adds.items():
            q = sum(qty for t, qty in tags.items() if t not in self.removed)
            if q:
                out[sku] = q
        return dict(sorted(out.items()))

    # -- replication -----------------------------------------------------
    def merge(self, other: "ORSetCart") -> None:
        """Absorb another replica's state. Safe to repeat and to reorder."""
        for sku, tags in other.adds.items():
            self.adds.setdefault(sku, {}).update(tags)
        self.removed |= other.removed
        # Never reuse a tag this replica may already have issued.
        own = [c for (rid, c) in self.removed | {t for ts in self.adds.values() for t in ts}
               if rid == self.replica_id]
        self._counter = max([self._counter, *own])

    def snapshot(self) -> "ORSetCart":
        """Copy of the state as it would be shipped over the network."""
        return copy.deepcopy(self)

    def compact(self) -> int:
        """Drop removed tags from ``adds``; return how many were dropped.

        Tombstones themselves are kept: they are needed until every replica
        has seen the removal (in production, garbage-collect them once all
        replicas acknowledge, e.g. with version vectors).
        """
        dropped = 0
        for sku in list(self.adds):
            tags = self.adds[sku]
            for t in [t for t in tags if t in self.removed]:
                del tags[t]
                dropped += 1
            if not tags:
                del self.adds[sku]
        return dropped
