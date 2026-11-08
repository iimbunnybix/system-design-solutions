"""Keep events for the same order in order while consuming partitions in parallel.

The order service published "created", "paid" and "shipped" events to a queue
with several partitions, choosing the partition round-robin. Each partition has
its own consumer running in parallel, so two events of one order sat in
different queues and the faster queue won: "shipped" was sometimes applied
before "paid" and the order ended up stuck in "paid".

The fix routes every event by its entity key (the order id) with a stable hash
(blake2b, not Python's per-process randomised ``hash()``). All events of one
order go to one partition, which is consumed in FIFO order, so per-key order is
preserved while different orders still run in parallel.
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import Callable

PARTITION_COUNT = 4
EVENT_TYPES = ("created", "paid", "shipped")
MIN_STEP_GAP_MS = 1             # producer delay between two events of one order
MAX_STEP_GAP_MS = 30
MIN_PROCESS_MS = 1              # consumer time to handle one event
MAX_PROCESS_MS = 12

Partitioner = Callable[[str, int], int]


@dataclass(frozen=True)
class Event:
    """One state change published by the order service."""
    order_id: str
    event_type: str
    produced_at_ms: int
    sequence: int               # global publish order, used as a tie-breaker


@dataclass(frozen=True)
class Applied:
    """An event after a consumer finished processing it."""
    event: Event
    partition: int
    finished_at_ms: int


def stable_partition(key: str, partition_count: int) -> int:
    """Map a key to a partition the same way on every process and host.

    Python's hash() of a str changes per process (PYTHONHASHSEED), so two
    producers would send the same order to different partitions.
    """
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % partition_count


class RoundRobinPartitioner:
    """The original producer: spread events evenly, ignoring the key."""

    def __init__(self) -> None:
        self.next_partition = 0

    def __call__(self, key: str, partition_count: int) -> int:
        """Return the next partition in turn, whatever the key is."""
        chosen = self.next_partition % partition_count
        self.next_partition += 1
        return chosen


def generate_order_events(order_count: int, window_ms: int, seed: int) -> list[Event]:
    """Each order emits created -> paid -> shipped with small random gaps."""
    rng = random.Random(seed)
    timed: list[tuple[int, str, int]] = []
    for number in range(order_count):
        order_id = f"order-{number}"
        moment = rng.randrange(window_ms)
        for step in range(len(EVENT_TYPES)):
            timed.append((moment, order_id, step))
            moment += rng.randint(MIN_STEP_GAP_MS, MAX_STEP_GAP_MS)
    timed.sort()
    events = []
    for sequence, (moment, order_id, step) in enumerate(timed):
        events.append(Event(order_id, EVENT_TYPES[step], moment, sequence))
    return events


def assign_partitions(events: list[Event], partitioner: Partitioner,
                      partition_count: int) -> list[list[Event]]:
    """Append each event to the queue of the partition the producer picks."""
    queues: list[list[Event]] = [[] for _ in range(partition_count)]
    for event in events:
        queues[partitioner(event.order_id, partition_count)].append(event)
    return queues


def consume_in_parallel(queues: list[list[Event]], seed: int) -> list[Applied]:
    """Simulate one consumer per partition, all running at the same time.

    Each consumer handles its own queue strictly FIFO; processing times are
    seeded-random. The result is every event in the order it was applied.
    """
    rng = random.Random(seed)
    applied: list[Applied] = []
    for partition, queue in enumerate(queues):
        consumer_free_at = 0
        for event in queue:
            start = max(event.produced_at_ms, consumer_free_at)
            consumer_free_at = start + rng.randint(MIN_PROCESS_MS, MAX_PROCESS_MS)
            applied.append(Applied(event, partition, consumer_free_at))
    # The shared order database sees writes in the order consumers finish.
    applied.sort(key=completion_order)
    return applied


def completion_order(item: Applied) -> tuple[int, int]:
    """Sort key: finish time, then publish order for simultaneous finishes."""
    return (item.finished_at_ms, item.event.sequence)


@dataclass
class OrderStore:
    """Order state as the consumers write it: last event applied wins."""
    status: dict[str, str] = field(default_factory=dict)
    out_of_order: int = 0

    def apply(self, event: Event) -> None:
        """Set the order's status, counting events that arrive after a later one."""
        current = self.status.get(event.order_id)
        if current is not None and EVENT_TYPES.index(current) > EVENT_TYPES.index(event.event_type):
            self.out_of_order += 1
        self.status[event.order_id] = event.event_type

    def wrong_final_states(self) -> int:
        """Orders that should be 'shipped' (every order shipped) but are not."""
        wrong = 0
        for status in self.status.values():
            if status != "shipped":
                wrong += 1
        return wrong


@dataclass
class RunResult:
    """Outcome of one simulated run."""
    store: OrderStore
    applied: list[Applied]
    queue_lengths: list[int]
    finished_at_ms: int
    max_lag_ms: int             # worst time from publish to applied


def run_pipeline(events: list[Event], partitioner: Partitioner,
                 partition_count: int = PARTITION_COUNT, seed: int = 1) -> RunResult:
    """Produce, consume in parallel and apply every event to the store."""
    queues = assign_partitions(events, partitioner, partition_count)
    applied = consume_in_parallel(queues, seed)
    store = OrderStore()
    for item in applied:
        store.apply(item.event)
    lengths = [len(queue) for queue in queues]
    finished = 0
    max_lag = 0
    for item in applied:
        finished = max(finished, item.finished_at_ms)
        max_lag = max(max_lag, item.finished_at_ms - item.event.produced_at_ms)
    return RunResult(store, applied, lengths, finished, max_lag)


def naive_run(events: list[Event], partition_count: int = PARTITION_COUNT,
              seed: int = 1) -> RunResult:
    """Original pipeline: round-robin partitions."""
    return run_pipeline(events, RoundRobinPartitioner(), partition_count, seed)


def keyed_run(events: list[Event], partition_count: int = PARTITION_COUNT,
              seed: int = 1) -> RunResult:
    """Fixed pipeline: partition by order id with a stable hash."""
    return run_pipeline(events, stable_partition, partition_count, seed)


def per_key_order_violations(applied: list[Applied]) -> int:
    """Count events applied before an event of the same key published earlier."""
    last_sequence: dict[str, int] = {}
    violations = 0
    for item in applied:
        key = item.event.order_id
        if key in last_sequence and last_sequence[key] > item.event.sequence:
            violations += 1
        last_sequence[key] = max(last_sequence.get(key, -1), item.event.sequence)
    return violations


def generate_hot_key_events(event_count: int, hot_share: float, window_ms: int,
                            seed: int) -> list[Event]:
    """Steady traffic where one key (a viral seller's order) gets hot_share of it."""
    rng = random.Random(seed)
    events = []
    for sequence in range(event_count):
        if rng.random() < hot_share:
            key = "order-hot"
        else:
            key = f"order-{rng.randrange(event_count)}"
        moment = sequence * window_ms // event_count
        events.append(Event(key, "paid", moment, sequence))
    return events
