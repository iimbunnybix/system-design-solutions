"""A fast producer overwhelms a slower consumer through an unbounded buffer.

The incident: a relay service reads order events from a durable log and
pushes them to the fraud-scoring service, which keeps received events in an
in-memory queue. During a sale the log filled twice as fast as scoring could
keep up. The relay kept pushing, the in-memory queue grew until the scoring
service was OOM-killed, and every event in its queue was lost because the
relay had already marked them as delivered.

The fix is credit-based flow control: the consumer grants one credit per free
slot in a fixed-size buffer, and the producer sends an event only when it
holds a credit. Events that cannot be sent yet stay in the durable log, so
memory stays bounded and a burst becomes lag that drains afterwards. Sending
does not delete anything: the consumer acknowledges events only after scoring
them, and a restarted consumer is resent everything after the last
acknowledged offset, so events in a lost buffer are never lost for good.
"""
from __future__ import annotations

from collections import deque

TICK_SECONDS = 0.1
EVENT_BYTES = 4_096                  # one order event in memory, with overhead
MEMORY_LIMIT_BYTES = 64 * 1024 * 1024
CONSUMER_RATE_PER_TICK = 100         # fraud scoring: 1,000 events/s
BURST_RATE_PER_TICK = 200            # during the sale: 2,000 events/s
BURST_TICKS = 300                    # 30 s sale spike
NORMAL_RATE_PER_TICK = 50            # after the spike: 500 events/s
SIMULATED_TICKS = 1_200              # 2 minutes
BUFFER_SLOTS = 200                   # credit window: two ticks of work


def arrivals_at(tick: int) -> int:
    """Events written to the durable log during ``tick``."""
    if tick < BURST_TICKS:
        return BURST_RATE_PER_TICK
    return NORMAL_RATE_PER_TICK


class DurableLog:
    """The upstream log. Events here are on disk, not in anyone's memory.

    Events are never removed by reading. ``committed_offset`` marks how many
    events the consumer has acknowledged; everything after it may still need
    to be (re)sent.
    """

    def __init__(self) -> None:
        self.events: list[int] = []              # arrival tick of each event, by offset
        self.committed_offset = 0                # events before this are acknowledged

    def append(self, tick: int, count: int) -> None:
        """Upstream writes ``count`` events at ``tick``."""
        for _ in range(count):
            self.events.append(tick)

    @property
    def end_offset(self) -> int:
        """Offset one past the newest event."""
        return len(self.events)

    def read(self, start_offset: int, limit: int) -> list[int]:
        """Return up to ``limit`` events from ``start_offset`` without removing them."""
        return self.events[start_offset:start_offset + limit]

    def commit(self, count: int) -> None:
        """Acknowledge the next ``count`` events (they are processed in order)."""
        self.committed_offset = min(self.committed_offset + count, self.end_offset)

    def unacknowledged(self) -> int:
        """Events not yet confirmed as processed."""
        return self.end_offset - self.committed_offset


def process(buffer: deque[int], tick: int, latencies: list[float]) -> int:
    """Consumer scores up to its rate from the buffer; records each event's latency."""
    done = 0
    while buffer and done < CONSUMER_RATE_PER_TICK:
        arrived = buffer.popleft()
        latencies.append((tick - arrived) * TICK_SECONDS)
        done += 1
    return done


# ---------------------------------------------------------------- original
def simulate_unbounded_push() -> dict[str, float]:
    """Original design: the relay pushes everything it reads, immediately."""
    log = DurableLog()
    buffer: deque[int] = deque()                 # consumer memory, no limit
    latencies: list[float] = []
    for tick in range(SIMULATED_TICKS):
        log.append(tick, arrivals_at(tick))
        sent = log.read(log.committed_offset, log.unacknowledged())
        buffer.extend(sent)
        log.commit(len(sent))                    # BUG: "delivered" as soon as sent
        if len(buffer) * EVENT_BYTES > MEMORY_LIMIT_BYTES:
            # OOM kill: everything queued in memory is gone, and the log
            # already counts it as delivered, so nobody will resend it.
            return {"crashed_at_s": tick * TICK_SECONDS, "lost_events": len(buffer),
                    "max_latency_s": max(latencies)}
        process(buffer, tick, latencies)
    return {"crashed_at_s": -1.0, "lost_events": 0, "max_latency_s": max(latencies)}


# ---------------------------------------------------------------- the fix
class CreditedConsumer:
    """Consumer with a fixed buffer that hands out one credit per free slot."""

    def __init__(self, slots: int = BUFFER_SLOTS) -> None:
        self.slots = slots
        self.buffer: deque[int] = deque()
        self.outstanding_credits = 0             # granted, not yet used

    def grant_credits(self) -> int:
        """Grant credits for every slot that is neither full nor already promised."""
        free = self.slots - len(self.buffer) - self.outstanding_credits
        self.outstanding_credits += free
        return free

    def receive(self, event: int) -> None:
        """Accept one event; a sender without credit is a protocol violation."""
        if self.outstanding_credits == 0:
            raise RuntimeError("event sent without credit")
        self.outstanding_credits -= 1
        self.buffer.append(event)


class CreditedProducer:
    """Relay that sends only as many events as it holds credits for."""

    def __init__(self) -> None:
        self.credits = 0
        self.next_offset = 0                     # next log offset to send

    def add_credits(self, credits: int) -> None:
        """Store credits granted by the consumer."""
        self.credits += credits

    def send(self, log: DurableLog, consumer: CreditedConsumer) -> int:
        """Send at most ``credits`` events; the log keeps them until acknowledged."""
        events = log.read(self.next_offset, self.credits)
        for event in events:
            consumer.receive(event)
        self.next_offset += len(events)
        self.credits -= len(events)
        return len(events)

    def unsent(self, log: DurableLog) -> int:
        """Events in the log that have not been sent on this connection yet."""
        return log.end_offset - self.next_offset

    def reconnect(self, log: DurableLog) -> None:
        """A new consumer connected: resend everything it has not acknowledged.

        Old credits belonged to the dead consumer, so they are dropped too.
        """
        self.next_offset = log.committed_offset
        self.credits = 0


def simulate_credit_flow() -> dict[str, float]:
    """Same traffic with credit-based flow control."""
    log = DurableLog()
    consumer = CreditedConsumer()
    producer = CreditedProducer()
    latencies: list[float] = []
    peak_buffer = 0
    peak_log_backlog = 0
    caught_up_at_s = -1.0                        # first time after the burst the log is empty
    for tick in range(SIMULATED_TICKS):
        log.append(tick, arrivals_at(tick))
        producer.add_credits(consumer.grant_credits())
        producer.send(log, consumer)
        peak_buffer = max(peak_buffer, len(consumer.buffer))
        peak_log_backlog = max(peak_log_backlog, producer.unsent(log))
        scored = process(consumer.buffer, tick, latencies)
        # Acknowledge only after scoring, so a crash cannot lose these events.
        log.commit(scored)
        if tick >= BURST_TICKS and caught_up_at_s < 0 and producer.unsent(log) == 0:
            caught_up_at_s = tick * TICK_SECONDS
    return {"peak_memory_bytes": peak_buffer * EVENT_BYTES,
            "peak_log_backlog": peak_log_backlog,
            "delivered": len(latencies),
            "left_in_log": log.unacknowledged(),
            "caught_up_at_s": caught_up_at_s,
            "max_latency_s": max(latencies)}


def total_arrivals() -> int:
    """Events written to the log over the whole simulation."""
    total = 0
    for tick in range(SIMULATED_TICKS):
        total += arrivals_at(tick)
    return total
