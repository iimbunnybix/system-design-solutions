"""Size a consumer group by its partition count, not by how many pods you start.

During a backlog the team tripled a Kafka-style consumer group from 6 to 18
consumers, but drain time did not move: each partition is read by at most one
member of a group, the topic had 6 partitions, so 12 consumers got nothing.
This module simulates partition assignment (range and round-robin) and backlog
drain time, and adds a capacity planner that sizes the partition count from
target throughput / per-consumer throughput with headroom, warns when a group
has more consumers than partitions, and warns that adding partitions changes
which partition a key maps to.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field

PARTITIONS_BEFORE = 6
BACKLOG_PER_PARTITION = 600_000            # messages waiting when the alert fired
PEAK_INFLOW_PER_SECOND = 3_000             # producers keep writing during the backlog
PER_CONSUMER_PER_SECOND = 700              # measured throughput of one consumer
TARGET_DRAIN_SECONDS = 15 * 60             # the backlog must be gone within 15 minutes
HEADROOM = 0.25                            # plan 25% above the computed need


@dataclass
class Partition:
    """One partition's waiting messages and the rate producers add to it."""
    backlog: int
    inflow_per_second: float


def even_topic(partition_count: int, total_backlog: int, total_inflow: float) -> list[Partition]:
    """A topic whose backlog and inflow are spread evenly over its partitions."""
    partitions = []
    for _ in range(partition_count):
        partitions.append(Partition(total_backlog // partition_count,
                                    total_inflow / partition_count))
    return partitions


def consumer_names(count: int) -> list[str]:
    """Group members named consumer-00, consumer-01, ... (sorted like Kafka sorts member ids)."""
    return [f"consumer-{index:02d}" for index in range(count)]


def range_assign(partition_count: int, consumers: list[str]) -> dict[str, list[int]]:
    """Kafka RangeAssignor: contiguous ranges, the first members get one extra."""
    assignment: dict[str, list[int]] = {name: [] for name in consumers}
    per_consumer = partition_count // len(consumers)
    extra = partition_count % len(consumers)
    next_partition = 0
    for index, name in enumerate(sorted(consumers)):
        # With more consumers than partitions, per_consumer is 0 and only the
        # first `extra` (= partition_count) members get anything.
        share = per_consumer + (1 if index < extra else 0)
        assignment[name] = list(range(next_partition, next_partition + share))
        next_partition += share
    return assignment


def round_robin_assign(partition_count: int, consumers: list[str]) -> dict[str, list[int]]:
    """Kafka RoundRobinAssignor: deal partitions out one at a time."""
    ordered = sorted(consumers)
    assignment: dict[str, list[int]] = {name: [] for name in ordered}
    for partition in range(partition_count):
        assignment[ordered[partition % len(ordered)]].append(partition)
    return assignment


def idle_consumers(assignment: dict[str, list[int]]) -> list[str]:
    """Members that were assigned no partition and so process nothing."""
    return [name for name, owned in assignment.items() if not owned]


def drain_seconds(backlog: int, inflow_per_second: float, capacity_per_second: float) -> float:
    """Time for one consumer to empty its backlog while new messages keep arriving."""
    if backlog == 0:
        return 0.0
    # Only the capacity left over after keeping up with new messages eats the backlog.
    spare = capacity_per_second - inflow_per_second
    if spare <= 0:
        return math.inf
    return backlog / spare


def group_drain_seconds(topic: list[Partition], assignment: dict[str, list[int]],
                        per_consumer_rate: float) -> float:
    """The backlog is gone when the slowest consumer finishes its partitions."""
    slowest = 0.0
    for owned in assignment.values():
        backlog = sum(topic[partition].backlog for partition in owned)
        inflow = sum(topic[partition].inflow_per_second for partition in owned)
        slowest = max(slowest, drain_seconds(backlog, inflow, per_consumer_rate))
    return slowest


def naive_scale_consumers(topic: list[Partition], consumer_count: int,
                          per_consumer_rate: float = PER_CONSUMER_PER_SECOND) -> float:
    """Original response: start more consumers and hope. Returns the drain time."""
    assignment = range_assign(len(topic), consumer_names(consumer_count))
    return group_drain_seconds(topic, assignment, per_consumer_rate)


@dataclass
class CapacityPlan:
    """How many partitions (and consumers) a topic needs, and what to watch out for."""
    required_throughput: float
    partitions: int
    warnings: list[str] = field(default_factory=list)


def required_throughput(peak_inflow: float, backlog: int, target_drain_seconds: float) -> float:
    """Messages/second needed to keep up with producers AND clear the backlog in time."""
    if target_drain_seconds <= 0:
        raise ValueError("target drain time must be positive")
    return peak_inflow + backlog / target_drain_seconds


def partitions_needed(throughput: float, per_consumer_rate: float, headroom: float) -> int:
    """Partitions = consumers needed, because one partition feeds at most one consumer."""
    if per_consumer_rate <= 0 or headroom < 0:
        raise ValueError("per-consumer rate must be positive and headroom non-negative")
    return max(1, math.ceil(throughput * (1 + headroom) / per_consumer_rate))


def capacity_warnings(consumer_count: int, partition_count: int,
                      current_partition_count: int | None = None) -> list[str]:
    """Human-readable warnings for an on-call engineer about to scale a group."""
    warnings = []
    if consumer_count > partition_count:
        idle = consumer_count - partition_count
        warnings.append(f"{consumer_count} consumers but only {partition_count} partitions: "
                        f"{idle} consumers will sit idle")
    if current_partition_count is not None and partition_count > current_partition_count:
        # hash(key) % N changes with N, so a key's new events go to a different
        # partition than its old ones and per-key order breaks during the switch.
        warnings.append(f"growing from {current_partition_count} to {partition_count} partitions "
                        "remaps keys to new partitions: per-key ordering is not guaranteed "
                        "across the change, and existing backlog stays on the old partitions")
    return warnings


def plan_capacity(peak_inflow: float, backlog: int, target_drain_seconds: float,
                  per_consumer_rate: float, headroom: float, consumer_count: int,
                  current_partition_count: int | None = None) -> CapacityPlan:
    """Size the partition count for a target and list warnings for the planned group."""
    throughput = required_throughput(peak_inflow, backlog, target_drain_seconds)
    partitions = partitions_needed(throughput, per_consumer_rate, headroom)
    warnings = capacity_warnings(consumer_count, partitions, current_partition_count)
    return CapacityPlan(throughput, partitions, warnings)


def add_partitions_to_live_topic(topic: list[Partition], new_count: int) -> list[Partition]:
    """Grow a topic in place: old backlog stays put, only new messages spread out."""
    total_inflow = sum(partition.inflow_per_second for partition in topic)
    grown = []
    for index in range(new_count):
        old_backlog = topic[index].backlog if index < len(topic) else 0
        grown.append(Partition(old_backlog, total_inflow / new_count))
    return grown


def stable_partition(key: str, partition_count: int) -> int:
    """Producer's key -> partition mapping (stable across processes, unlike hash())."""
    digest = hashlib.blake2b(key.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") % partition_count


def fraction_of_keys_remapped(keys: list[str], old_count: int, new_count: int) -> float:
    """Share of keys whose partition changes when the partition count changes."""
    moved = 0
    for key in keys:
        if stable_partition(key, old_count) != stable_partition(key, new_count):
            moved += 1
    return moved / len(keys)
