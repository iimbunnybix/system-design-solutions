"""Replayed queue messages ship orders twice because the dedup window expired.

A fulfilment consumer reads "order paid" messages from a partitioned log with
at-least-once delivery. It skipped duplicates by remembering message ids in a
store with a one-hour TTL. When its offset commits failed silently for three
hours and the consumer then restarted, the log replayed three hours of
messages; ids older than one hour had already expired, so those orders
shipped a second time.

The fix stores, in the same transaction as the shipment, the highest offset
already processed for each partition. A replayed message with an offset at or
below that mark is skipped, no matter how old it is, and the mark needs one
number per partition instead of one entry per message.
"""
from __future__ import annotations

from dataclasses import dataclass

SECONDS_PER_HOUR = 3600
DEDUP_TTL_SECONDS = 1 * SECONDS_PER_HOUR   # the original id-store retention
NO_OFFSET = -1                             # mark for a partition never processed


@dataclass(frozen=True)
class Message:
    partition: int
    offset: int        # position in the partition's log, increases by one
    message_id: str
    order_id: str


class FakeClock:
    """Manually advanced clock, in seconds."""

    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Warehouse:
    """Counts shipments per order; more than one is the incident."""

    def __init__(self) -> None:
        self.shipments: dict[str, int] = {}

    def ship(self, order_id: str) -> None:
        """Record one physical shipment of ``order_id``."""
        self.shipments[order_id] = self.shipments.get(order_id, 0) + 1

    def duplicate_shipments(self) -> int:
        """Shipments beyond the first for each order."""
        extra = 0
        for count in self.shipments.values():
            extra += count - 1
        return extra


# --- the original consumer ----------------------------------------------------
class TtlDedupConsumer:
    """Original: skip a message if its id was seen within the last TTL seconds."""

    def __init__(self, warehouse: Warehouse, clock: FakeClock, ttl_seconds: float = DEDUP_TTL_SECONDS) -> None:
        self.warehouse = warehouse
        self.clock = clock
        self.ttl_seconds = ttl_seconds
        self.seen_at: dict[str, float] = {}   # message id -> time it was processed

    def handle(self, message: Message) -> bool:
        """Ship unless the id is still in the store; return True if it shipped."""
        self._expire_old_ids()
        if message.message_id in self.seen_at:
            return False
        self.warehouse.ship(message.order_id)
        self.seen_at[message.message_id] = self.clock.now
        return True

    def _expire_old_ids(self) -> None:
        """Drop ids older than the TTL, like a cache with expiry."""
        cutoff = self.clock.now - self.ttl_seconds
        expired = [message_id for message_id, seen in self.seen_at.items() if seen <= cutoff]
        for message_id in expired:
            del self.seen_at[message_id]


# --- the fix --------------------------------------------------------------------
class OffsetMarkConsumer:
    """Skip any message at or below the highest offset processed in its partition.

    WHY this is safe: within a partition the log replays messages in offset
    order, so "already processed" is exactly "offset <= mark". The mark is
    stored with the shipment, so a crash can never leave one without the other.
    """

    def __init__(self, warehouse: Warehouse) -> None:
        self.warehouse = warehouse
        self.processed_up_to: dict[int, int] = {}

    def mark_for(self, partition: int) -> int:
        """Highest processed offset in ``partition``, or NO_OFFSET."""
        return self.processed_up_to.get(partition, NO_OFFSET)

    def handle(self, message: Message) -> bool:
        """Ship a message the first time its offset is seen; return True if it shipped."""
        if message.offset <= self.mark_for(message.partition):
            return False
        # In a real database these two writes are one transaction.
        self.warehouse.ship(message.order_id)
        self.processed_up_to[message.partition] = message.offset
        return True


# --- the incident replay ----------------------------------------------------------
def build_log(partition: int, count: int) -> list[Message]:
    """``count`` consecutive messages in one partition, one order each."""
    messages = []
    for offset in range(count):
        order_id = f"order-{partition}-{offset}"
        messages.append(Message(partition, offset, f"msg-{partition}-{offset}", order_id))
    return messages


def consume_then_replay(consumer: TtlDedupConsumer | OffsetMarkConsumer, clock: FakeClock,
                        log: list[Message], seconds_between_messages: float) -> None:
    """Process the whole log once, then replay it from offset 0.

    WHY from 0: the offset commits had been failing silently, so the last
    committed offset the broker knew about was the start of the log.
    """
    for message in log:
        consumer.handle(message)
        clock.advance(seconds_between_messages)
    for message in log:
        consumer.handle(message)
