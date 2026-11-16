"""Rebalance partitions across consumers without reshuffling all of them.

The order-enrichment consumers split a topic's partitions with a range
assignor: sort the consumers, give each a contiguous block. Adding or removing
one consumer shifts every block boundary, so most partitions change owner,
and every moved partition loses its warm local cache and pauses while offsets
are committed and handed over. This module keeps that assignor and adds a
sticky one: it keeps every partition on its current owner unless the owner is
gone or holds more than its fair share, and moves only the remainder.
"""
from __future__ import annotations

Assignment = dict[int, str]   # partition number -> consumer name


class NoConsumers(Exception):
    """There is nobody to assign partitions to."""


def naive_range_assign(partitions: list[int], consumers: list[str]) -> Assignment:
    """Original assignor: contiguous blocks of partitions per sorted consumer."""
    if not consumers:
        raise NoConsumers("cannot assign partitions to zero consumers")
    ordered_partitions = sorted(partitions)
    ordered_consumers = sorted(consumers)
    base_share, extra = divmod(len(ordered_partitions), len(ordered_consumers))
    assignment: Assignment = {}
    start = 0
    for position, consumer in enumerate(ordered_consumers):
        block_size = base_share + (1 if position < extra else 0)
        for partition in ordered_partitions[start:start + block_size]:
            assignment[partition] = consumer
        start += block_size
    return assignment


def partitions_by_owner(previous: Assignment, consumers: list[str]) -> dict[str, list[int]]:
    """Group the previous assignment by owner, ignoring owners that have left."""
    owned: dict[str, list[int]] = {consumer: [] for consumer in consumers}
    for partition in sorted(previous):
        owner = previous[partition]
        if owner in owned:
            owned[owner].append(partition)
    return owned


def fair_quotas(partition_count: int, owned: dict[str, list[int]]) -> dict[str, int]:
    """How many partitions each consumer should hold (shares differ by at most 1).

    When the split is uneven, the consumers that already hold the most get the
    larger share, because that lets them keep one more partition instead of
    handing it over.
    """
    base_share, extra = divmod(partition_count, len(owned))
    busiest_first = sorted(owned, key=lambda consumer: (-len(owned[consumer]), consumer))
    quotas: dict[str, int] = {}
    for position, consumer in enumerate(busiest_first):
        quotas[consumer] = base_share + (1 if position < extra else 0)
    return quotas


def sticky_assign(partitions: list[int], consumers: list[str],
                  previous: Assignment) -> Assignment:
    """Balanced assignment that moves as few partitions as possible."""
    if not consumers:
        raise NoConsumers("cannot assign partitions to zero consumers")
    current_partitions = set(partitions)
    still_existing = {p: owner for p, owner in previous.items() if p in current_partitions}
    owned = partitions_by_owner(still_existing, sorted(set(consumers)))
    quotas = fair_quotas(len(current_partitions), owned)

    assignment: Assignment = {}
    # Step 1: every consumer keeps its old partitions up to its quota.
    # Keeping is free; only partitions that change owner cost a cache warm-up.
    for consumer, kept in owned.items():
        for partition in kept[:quotas[consumer]]:
            assignment[partition] = consumer

    # Step 2: hand the rest (orphaned or over-quota) to consumers with room.
    unassigned = [p for p in sorted(current_partitions) if p not in assignment]
    for consumer in sorted(owned):
        room = quotas[consumer] - min(len(owned[consumer]), quotas[consumer])
        for _ in range(room):
            assignment[unassigned.pop(0)] = consumer
    return assignment


def partition_counts(assignment: Assignment) -> dict[str, int]:
    """How many partitions each consumer holds."""
    counts: dict[str, int] = {}
    for owner in assignment.values():
        counts[owner] = counts.get(owner, 0) + 1
    return counts


def moved_partitions(before: Assignment, after: Assignment) -> list[int]:
    """Partitions whose owner changed: each one loses its warm cache."""
    moved = []
    for partition, owner in sorted(after.items()):
        if before.get(partition) != owner:
            moved.append(partition)
    return moved
