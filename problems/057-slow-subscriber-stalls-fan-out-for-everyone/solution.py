"""Isolate subscribers so one slow consumer cannot stall a fan-out for everyone.

A live-scores service pushed each update to every connected subscriber in
one loop, waiting for each send to finish before the next. One subscriber on
a congested link took ten times longer per message, so every other
subscriber's updates were delayed behind it and the delay grew without
bound. This module gives each subscriber its own bounded queue and worker:
the publisher only enqueues, a fast subscriber is never blocked by a slow
one, and a subscriber whose queue overflows is either disconnected or has
its oldest updates dropped, so memory stays bounded.

Time is simulated in integer ticks so the tests are deterministic.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

DISCONNECT = "disconnect"   # for consumers that need every message (they must resync)
DROP_OLDEST = "drop_oldest"  # for "latest value wins" feeds such as live scores


@dataclass
class Subscriber:
    name: str
    ticks_per_message: int          # how long one delivery takes for this consumer
    queue_limit: int = 8
    overflow_policy: str = DISCONNECT
    delivered: list[tuple[int, int]] = field(default_factory=list)  # (message, tick)
    dropped: int = 0
    disconnected_at: int | None = None
    queue: deque[int] = field(default_factory=deque)
    busy_until: int = 0


def naive_broadcast(message_count: int, subscribers: list[Subscriber]) -> None:
    """Original loop: one message per tick, sent to each subscriber in turn, blocking."""
    clock = 0
    for message in range(message_count):
        # The publisher cannot start message N before it is produced at tick N.
        clock = max(clock, message)
        for subscriber in subscribers:
            # A blocking send: nothing else happens until this subscriber is done.
            clock += subscriber.ticks_per_message
            subscriber.delivered.append((message, clock))


def enqueue(subscriber: Subscriber, message: int, tick: int) -> None:
    """Hand a message to one subscriber's queue without ever waiting on it."""
    if subscriber.disconnected_at is not None:
        return
    if len(subscriber.queue) < subscriber.queue_limit:
        subscriber.queue.append(message)
        return
    if subscriber.overflow_policy == DROP_OLDEST:
        # Only the newest score matters; discarding stale ones keeps it current.
        subscriber.queue.popleft()
        subscriber.dropped += 1
        subscriber.queue.append(message)
    else:
        # Slow consumers that need every message are cut off and must
        # reconnect and resync, instead of growing the queue forever.
        subscriber.disconnected_at = tick
        subscriber.dropped += len(subscriber.queue) + 1
        subscriber.queue.clear()


def run_worker(subscriber: Subscriber, tick: int) -> None:
    """Start the next delivery for this subscriber if it is idle at this tick."""
    if subscriber.disconnected_at is not None:
        return
    if tick < subscriber.busy_until or not subscriber.queue:
        return
    message = subscriber.queue.popleft()
    subscriber.busy_until = tick + subscriber.ticks_per_message
    # Recorded at completion time so latency is comparable with the naive loop.
    subscriber.delivered.append((message, subscriber.busy_until))


def isolated_broadcast(message_count: int, subscribers: list[Subscriber],
                       extra_ticks: int = 0) -> None:
    """Publish one message per tick; every subscriber drains its own queue in parallel."""
    for subscriber in subscribers:
        if subscriber.overflow_policy not in (DISCONNECT, DROP_OLDEST):
            raise ValueError(f"unknown overflow policy {subscriber.overflow_policy!r}")
        if subscriber.queue_limit < 1 or subscriber.ticks_per_message < 1:
            raise ValueError("queue_limit and ticks_per_message must be at least 1")
    for tick in range(message_count + extra_ticks):
        if tick < message_count:
            for subscriber in subscribers:
                enqueue(subscriber, tick, tick)
        for subscriber in subscribers:
            run_worker(subscriber, tick)


def worst_latency(subscriber: Subscriber) -> int:
    """Largest delay between a message being produced and it reaching the subscriber."""
    worst = 0
    for message, delivered_tick in subscriber.delivered:
        worst = max(worst, delivered_tick - message)
    return worst
