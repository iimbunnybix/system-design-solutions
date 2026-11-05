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
memory stays bounded and a burst becomes lag that drains afterwards.
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
    """The upstream log. Events here are on disk, not in anyone's memory."""

    def __init__(self) -> None:
        self.pending: deque[int] = deque()       # arrival tick of each unread event

    def append(self, tick: int, count: int) -> None:
        """Upstream writes ``count`` events at ``tick``."""
        for _ in range(count):
            self.pending.append(tick)

    def take(self, limit: int) -> list[int]:
        """Read up to ``limit`` events; the reader now owns them."""
        taken = []
        while self.pending and len(taken) < limit:
            taken.append(self.pending.popleft())
        return taken


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
        buffer.extend(log.take(len(log.pending)))   # "delivered" as soon as sent
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

    def add_credits(self, credits: int) -> None:
        """Store credits granted by the consumer."""
        self.credits += credits

    def send(self, log: DurableLog, consumer: CreditedConsumer) -> int:
        """Read at most ``credits`` events from the log and send them."""
        events = log.take(self.credits)
        for event in events:
            consumer.receive(event)
        self.credits -= len(events)
        return len(events)


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
        peak_log_backlog = max(peak_log_backlog, len(log.pending))
        process(consumer.buffer, tick, latencies)
        if tick >= BURST_TICKS and caught_up_at_s < 0 and not log.pending:
            caught_up_at_s = tick * TICK_SECONDS
    return {"peak_memory_bytes": peak_buffer * EVENT_BYTES,
            "peak_log_backlog": peak_log_backlog,
            "delivered": len(latencies),
            "left_in_log": len(log.pending),
            "caught_up_at_s": caught_up_at_s,
            "max_latency_s": max(latencies)}


def total_arrivals() -> int:
    """Events written to the log over the whole simulation."""
    total = 0
    for tick in range(SIMULATED_TICKS):
        total += arrivals_at(tick)
    return total
