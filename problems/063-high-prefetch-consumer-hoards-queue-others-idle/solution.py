"""Stop one queue consumer from hoarding the backlog while new consumers sit idle.

An image-processing queue had very uneven job durations (most thumbnails
take 1 s, some RAW files take 20 s). Consumers were configured with a
prefetch of 100 unacknowledged messages each, so the one consumer that was
running when a burst arrived pulled 100 jobs into its local buffer. When the
autoscaler added nine consumers, those jobs were no longer in the shared
queue: the new consumers drained the rest and went idle while the first one
worked through its private pile alone.

The fix bounds prefetch (the consumer's credit) to the number of jobs it can
work on right now. Messages stay in the shared queue until some consumer has
capacity, so whichever consumer frees up first takes the next job.
"""
from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field
from functools import partial
from typing import Callable

NAIVE_PREFETCH = 100          # the original consumer setting
BOUNDED_PREFETCH = 1          # one job in hand: each consumer works on one job at a time
BURST_JOBS = 300
FAST_JOB_SECONDS = 1          # thumbnail
SLOW_JOB_SECONDS = 20         # large RAW file
SLOW_JOB_FRACTION = 0.2
FIRST_CONSUMERS = 1
SCALED_OUT_CONSUMERS = 9
SCALE_OUT_AT_SECOND = 30      # autoscaler reacts to queue depth after 30 s
MAX_SECONDS = 100_000         # safety stop for the simulation loop

Dispatcher = Callable[[deque[int], list["Consumer"]], None]


@dataclass
class Consumer:
    """A worker that processes one job at a time from its local buffer."""
    name: str
    buffer: deque[int] = field(default_factory=deque)   # delivered, not yet started
    current_remaining: int = 0                           # seconds left on the job in hand
    busy_seconds: int = 0

    def unacked(self) -> int:
        """Messages delivered to this consumer and not yet acknowledged."""
        in_hand = 1 if self.current_remaining > 0 else 0
        return len(self.buffer) + in_hand


@dataclass
class RunResult:
    """Totals from one simulated drain of the backlog."""
    drain_seconds: int
    idle_while_work_waits: int     # consumer-seconds idle while jobs sat in another buffer
    peak_buffered_on_one: int      # most unacked messages any single consumer held
    jobs_done: int
    mean_completion_seconds: float  # average time from burst arrival to job done
    busy_seconds_by_consumer: dict[str, int]


def make_burst(seed: int, count: int = BURST_JOBS) -> list[int]:
    """Job durations in seconds: mostly fast thumbnails, some slow RAW files."""
    rng = random.Random(seed)
    durations = []
    for _ in range(count):
        if rng.random() < SLOW_JOB_FRACTION:
            durations.append(SLOW_JOB_SECONDS)
        else:
            durations.append(FAST_JOB_SECONDS)
    return durations


def naive_dispatch(queue: deque[int], consumers: list[Consumer]) -> None:
    """Original broker behaviour: push up to NAIVE_PREFETCH messages to each consumer."""
    dispatch_with_credit(queue, consumers, NAIVE_PREFETCH)


def dispatch_with_credit(queue: deque[int], consumers: list[Consumer], prefetch: int) -> None:
    """Hand out messages round-robin, never exceeding ``prefetch`` unacked per consumer."""
    if prefetch < 1:
        raise ValueError("prefetch must be at least 1")
    delivered = True
    # Round-robin one message at a time, like RabbitMQ, until no one has credit.
    while queue and delivered:
        delivered = False
        for consumer in consumers:
            if queue and consumer.unacked() < prefetch:
                consumer.buffer.append(queue.popleft())
                delivered = True


def work_one_second(consumer: Consumer) -> bool:
    """Advance the consumer by one second; return True if it finished (acked) a job."""
    if consumer.current_remaining == 0 and consumer.buffer:
        consumer.current_remaining = consumer.buffer.popleft()
    if consumer.current_remaining == 0:
        return False
    consumer.current_remaining -= 1
    consumer.busy_seconds += 1
    return consumer.current_remaining == 0


def connected_consumers(second: int) -> list[Consumer]:
    """The consumers that exist at ``second``: one at start, nine more after scale-out."""
    count = FIRST_CONSUMERS
    if second >= SCALE_OUT_AT_SECOND:
        count += SCALED_OUT_CONSUMERS
    return [Consumer(f"consumer-{i}") for i in range(count)]


def simulate(durations: list[int], dispatch: Dispatcher) -> RunResult:
    """Drain a backlog that arrived at second 0, using a fake one-second clock."""
    queue: deque[int] = deque(durations)
    consumers: list[Consumer] = []
    idle_while_waiting = 0
    peak_buffered = 0
    completion_times: list[int] = []
    second = 0
    while second < MAX_SECONDS and (queue or any(c.unacked() for c in consumers)):
        # Scale-out adds consumers; existing ones keep their buffers.
        for new in connected_consumers(second)[len(consumers):]:
            consumers.append(new)
        dispatch(queue, consumers)
        peak_buffered = max(peak_buffered, max(c.unacked() for c in consumers))
        idle_while_waiting += count_stranded_idle(consumers)
        for consumer in consumers:
            if work_one_second(consumer):
                completion_times.append(second + 1)
        second += 1
    mean_completion = sum(completion_times) / max(1, len(completion_times))
    return RunResult(second, idle_while_waiting, peak_buffered, len(completion_times),
                     round(mean_completion, 1), {c.name: c.busy_seconds for c in consumers})


def count_stranded_idle(consumers: list[Consumer]) -> int:
    """Idle consumers this second while another consumer has jobs waiting in its buffer."""
    waiting_elsewhere = sum(len(c.buffer) for c in consumers)
    idle = 0
    for consumer in consumers:
        if consumer.unacked() == 0 and waiting_elsewhere > 0:
            idle += 1
    return idle


def naive_run(durations: list[int]) -> RunResult:
    """The incident: every consumer prefetches up to 100 messages."""
    return simulate(durations, naive_dispatch)


def bounded_run(durations: list[int], prefetch: int = BOUNDED_PREFETCH) -> RunResult:
    """The fix: each consumer holds only as many messages as it can work on."""
    return simulate(durations, partial(dispatch_with_credit, prefetch=prefetch))


def ideal_drain_seconds(durations: list[int]) -> float:
    """Lower bound: all work spread perfectly once the fleet has scaled out."""
    early_work = SCALE_OUT_AT_SECOND * FIRST_CONSUMERS
    remaining = max(0, sum(durations) - early_work)
    fleet = FIRST_CONSUMERS + SCALED_OUT_CONSUMERS
    return SCALE_OUT_AT_SECOND + remaining / fleet

