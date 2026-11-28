"""Flush producer batches on age as well as size.

The event producer grouped messages into batches of 500 before sending them
to the broker, which worked at daytime volume. At night a region produced a
few messages a minute, so a batch took hours to fill: password-reset and
fraud-alert events sat in memory until morning, and a deploy restart threw
away whatever was waiting.

Fix: a batch is sent when it is full OR when its oldest message has waited
``linger`` seconds (Kafka's ``linger.ms`` idea). A periodic tick checks the
age even when no new messages arrive, and close() flushes what is left.
"""
from __future__ import annotations

from dataclasses import dataclass, field

BATCH_SIZE = 500
LINGER_SECONDS = 2.0


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds


@dataclass
class FakeBroker:
    """Records each delivered message with the time it arrived."""

    clock: FakeClock
    delivered: list[tuple[str, float]] = field(default_factory=list)
    batches_received: int = 0

    def send_batch(self, messages: list[str]) -> None:
        self.batches_received += 1
        for message in messages:
            self.delivered.append((message, self.clock.now))


class OriginalBatchingProducer:
    """Original: sends only when the batch is full."""

    def __init__(self, broker: FakeBroker, clock: FakeClock, batch_size: int = BATCH_SIZE) -> None:
        self.broker = broker
        self.clock = clock
        self.batch_size = batch_size
        self.pending: list[str] = []

    def send(self, message: str) -> None:
        self.pending.append(message)
        if len(self.pending) >= self.batch_size:
            self.broker.send_batch(self.pending)
            self.pending = []

    def tick(self) -> None:
        """The original had no timer; nothing happens."""

    def close(self) -> None:
        """The original dropped pending messages on shutdown."""
        self.pending = []


class BatchingProducer:
    """Fixed: sends when the batch is full or its oldest message is ``linger`` old."""

    def __init__(self, broker: FakeBroker, clock: FakeClock, batch_size: int = BATCH_SIZE,
                 linger_seconds: float = LINGER_SECONDS) -> None:
        self.broker = broker
        self.clock = clock
        self.batch_size = batch_size
        self.linger_seconds = linger_seconds
        self.pending: list[str] = []
        self.oldest_pending_at: float | None = None

    def send(self, message: str) -> None:
        """Queue a message; flush if that fills the batch or the batch is old."""
        if not self.pending:
            self.oldest_pending_at = self.clock.now
        self.pending.append(message)
        self._flush_if_due()

    def tick(self) -> None:
        """Called by a background timer, so a quiet producer still flushes."""
        self._flush_if_due()

    def close(self) -> None:
        """Flush whatever is waiting before the process exits."""
        self._flush()

    def _flush_if_due(self) -> None:
        if len(self.pending) >= self.batch_size:
            self._flush()
            return
        # WHY: age is measured from the OLDEST message; measuring from the
        # newest would let a slow trickle postpone the flush forever.
        if self.oldest_pending_at is not None:
            if self.clock.now - self.oldest_pending_at >= self.linger_seconds:
                self._flush()

    def _flush(self) -> None:
        if self.pending:
            self.broker.send_batch(self.pending)
        self.pending = []
        self.oldest_pending_at = None


def run_traffic(producer: OriginalBatchingProducer | BatchingProducer, clock: FakeClock,
                message_count: int, seconds_between: float, tick_seconds: float = 0.5) -> dict[str, float]:
    """Send messages at a steady rate with a timer tick; returns send time per message."""
    sent_at: dict[str, float] = {}
    for number in range(message_count):
        message = f"event-{number}"
        sent_at[message] = clock.now
        producer.send(message)
        _advance_with_ticks(producer, clock, seconds_between, tick_seconds)
    return sent_at


def _advance_with_ticks(producer: OriginalBatchingProducer | BatchingProducer, clock: FakeClock,
                        seconds: float, tick_seconds: float) -> None:
    """Move time forward, letting the producer's timer fire every ``tick_seconds``."""
    remaining = seconds
    while remaining > 0:
        step = min(tick_seconds, remaining)
        clock.advance(step)
        producer.tick()
        remaining -= step


def delivery_delays(sent_at: dict[str, float], broker: FakeBroker) -> list[float]:
    """Seconds from send() to broker arrival, for delivered messages."""
    delays = []
    for message, arrived_at in broker.delivered:
        delays.append(arrived_at - sent_at[message])
    return delays
