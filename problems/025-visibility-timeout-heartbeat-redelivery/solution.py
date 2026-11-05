"""Stop a work queue from handing a long job to a second worker mid-run.

Video jobs are pulled from an SQS-style queue: ``receive`` hides a message
for a fixed *visibility timeout*, and the worker deletes it when done. The
timeout was 60 s, but large videos took several minutes. Every job longer
than 60 s became visible again while the first worker was still busy, so a
second (and third...) worker started the same job: duplicate transcodes,
duplicate "your video is ready" emails, and a queue that never drained.

The fix: workers send a heartbeat that extends the visibility timeout while
they are still working, and every receive gets a fresh receipt. A worker
that stalls past its lease finds its receipt rejected and abandons the job
instead of committing a duplicate result.
"""
from __future__ import annotations

from dataclasses import dataclass, field

VISIBILITY_TIMEOUT_S = 60
HEARTBEAT_EVERY_S = 20          # well under the timeout, so one late beat is survivable
STEP_S = 5                      # simulation tick


class LeaseLost(Exception):
    """The receipt is stale: the message was redelivered or deleted."""


class FakeClock:
    def __init__(self) -> None:
        self.now = 0

    def advance(self, seconds: int) -> None:
        self.now += seconds


@dataclass
class _StoredMessage:
    body: str
    visible_at: int = 0
    receipt: str = ""
    receive_count: int = 0


@dataclass(frozen=True)
class Delivery:
    message_id: str
    body: str
    receipt: str


class VisibilityQueue:
    """In-memory queue with SQS-like receive / extend / delete semantics."""

    def __init__(self, clock: FakeClock, timeout_s: int = VISIBILITY_TIMEOUT_S) -> None:
        self.clock = clock
        self.timeout_s = timeout_s
        self.messages: dict[str, _StoredMessage] = {}

    def send(self, message_id: str, body: str) -> None:
        self.messages[message_id] = _StoredMessage(body)

    def receive(self) -> Delivery | None:
        """Hide the first visible message and return it with a new receipt."""
        for message_id, message in self.messages.items():
            if message.visible_at <= self.clock.now:
                message.receive_count += 1
                message.visible_at = self.clock.now + self.timeout_s
                # A new receipt per delivery lets us tell the current holder
                # apart from a worker whose lease already ran out.
                message.receipt = f"{message_id}#{message.receive_count}"
                return Delivery(message_id, message.body, message.receipt)
        return None

    def _current(self, receipt: str) -> _StoredMessage:
        message_id = receipt.split("#", 1)[0]
        message = self.messages.get(message_id)
        if message is None or message.receipt != receipt:
            raise LeaseLost(receipt)
        return message

    def extend(self, receipt: str, seconds: int) -> None:
        """Push the visibility deadline out; fails if the lease was lost."""
        self._current(receipt).visible_at = self.clock.now + seconds

    def delete(self, receipt: str) -> None:
        """Acknowledge the message; fails if the lease was lost."""
        message_id = receipt.split("#", 1)[0]
        self._current(receipt)
        del self.messages[message_id]


@dataclass
class Worker:
    """Processes one job at a time in simulated ``STEP_S`` ticks."""
    name: str
    queue: VisibilityQueue
    job_seconds: int
    use_heartbeat: bool
    completed: list[str] = field(default_factory=list)
    abandoned: list[str] = field(default_factory=list)
    _delivery: Delivery | None = None
    _worked_s: int = 0

    def tick(self) -> None:
        """Do one step of work: pick up a job, keep working, or finish."""
        if self._delivery is None:
            self._delivery = self.queue.receive()
            self._worked_s = 0
            return
        self._worked_s += STEP_S
        try:
            if self._worked_s >= self.job_seconds:
                self._finish()
            elif self.use_heartbeat and self._worked_s % HEARTBEAT_EVERY_S == 0:
                self.queue.extend(self._delivery.receipt, VISIBILITY_TIMEOUT_S)
        except LeaseLost:
            # Someone else owns this job now; do not commit our result.
            self.abandoned.append(self._delivery.message_id)
            self._delivery = None

    def _finish(self) -> None:
        delivery = self._delivery
        if self.use_heartbeat:
            # Confirm we still hold the lease right before the side effect.
            self.queue.extend(delivery.receipt, VISIBILITY_TIMEOUT_S)
        self.completed.append(delivery.message_id)
        self._delivery = None
        try:
            self.queue.delete(delivery.receipt)
        except LeaseLost:
            pass        # naive worker: result already committed twice


def run(clock: FakeClock, workers: list[Worker], seconds: int,
        paused: dict[str, range] | None = None) -> None:
    """Advance time; ``paused`` maps a worker name to seconds it is frozen."""
    paused = paused or {}
    for _ in range(seconds // STEP_S):
        for worker in workers:
            if clock.now not in paused.get(worker.name, range(0)):
                worker.tick()
        clock.advance(STEP_S)
