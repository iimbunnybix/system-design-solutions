"""Keep serving useful requests during overload instead of a backlog of dead ones.

The pricing service answers in about 10 ms on one worker per shard, and its
callers give up after 1 second. Requests waited in an unbounded FIFO queue.
During a 10-second traffic spike above capacity the queue grew until every
request waited more than a second, so the service spent all its time
computing answers for callers that had already timed out. Useful
throughput ("goodput") fell to zero and stayed there long after the spike,
until the backlog drained.

The fix bounds the queue so that a request that is accepted can still be
answered in time (others get an immediate "busy" reply they can retry
elsewhere), and drops queued requests whose deadline can no longer be met
instead of doing work nobody will read.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass

SERVICE_MS = 10            # time to answer one request
CLIENT_TIMEOUT_MS = 1_000  # callers give up after this long


@dataclass
class Outcome:
    """What happened to every request in a simulation run."""
    answered_in_time: int = 0      # goodput: the caller got its answer
    answered_too_late: int = 0     # work done after the caller gave up
    rejected_busy: int = 0         # refused at once; caller can retry elsewhere
    dropped_expired: int = 0       # left the queue unanswered, no work spent


def max_queue_length(service_ms: int, timeout_ms: int) -> int:
    """Longest queue whose last request still finishes within the timeout.

    A request behind L others (plus one in progress) finishes after about
    (L + 2) * service_ms in the worst case.
    """
    return max(0, timeout_ms // service_ms - 2)


def naive_fifo(arrivals_ms: list[int], service_ms: int = SERVICE_MS,
               timeout_ms: int = CLIENT_TIMEOUT_MS) -> Outcome:
    """The original server: one worker, unbounded FIFO queue, serve everything."""
    outcome = Outcome()
    worker_free_at = 0
    for arrival in arrivals_ms:
        start = max(worker_free_at, arrival)
        worker_free_at = start + service_ms
        if worker_free_at - arrival <= timeout_ms:
            outcome.answered_in_time += 1
        else:
            outcome.answered_too_late += 1
    return outcome


class DeadlineAwareServer:
    """One worker with a bounded queue that skips requests it cannot finish in time."""

    def __init__(self, service_ms: int = SERVICE_MS, timeout_ms: int = CLIENT_TIMEOUT_MS,
                 queue_limit: int | None = None) -> None:
        self.service_ms = service_ms
        self.timeout_ms = timeout_ms
        # Callers may set a different limit; the deadline check below still
        # keeps a too-long queue from producing late answers.
        if queue_limit is None:
            queue_limit = max_queue_length(service_ms, timeout_ms)
        self.queue_limit = queue_limit
        self.waiting: deque[int] = deque()     # arrival times of queued requests
        self.worker_free_at = 0
        self.outcome = Outcome()

    def run_until(self, now_ms: int) -> None:
        """Let the worker take queued requests it can start before ``now_ms``."""
        while self.waiting and self.worker_free_at <= now_ms:
            self._start_next()

    def _start_next(self) -> None:
        """Serve the oldest queued request, or drop it if it would finish too late."""
        arrival = self.waiting.popleft()
        start = max(self.worker_free_at, arrival)
        deadline = arrival + self.timeout_ms
        # The caller is gone by the time we would answer: skip the work.
        if start + self.service_ms > deadline:
            self.outcome.dropped_expired += 1
            return
        self.worker_free_at = start + self.service_ms
        self.outcome.answered_in_time += 1

    def arrive(self, arrival_ms: int) -> None:
        """Accept a request into the queue, or reject it at once if the queue is full."""
        self.run_until(arrival_ms)
        if len(self.waiting) >= self.queue_limit and self.worker_free_at > arrival_ms:
            self.outcome.rejected_busy += 1
            return
        self.waiting.append(arrival_ms)
        self.run_until(arrival_ms)

    def finish(self) -> Outcome:
        """Drain the queue after the last arrival and return the totals."""
        while self.waiting:
            self._start_next()
        return self.outcome


def deadline_aware(arrivals_ms: list[int], service_ms: int = SERVICE_MS,
                   timeout_ms: int = CLIENT_TIMEOUT_MS, queue_limit: int | None = None) -> Outcome:
    """Run the fixed server over a list of arrival times."""
    server = DeadlineAwareServer(service_ms, timeout_ms, queue_limit)
    for arrival in arrivals_ms:
        server.arrive(arrival)
    return server.finish()


def steady_arrivals(start_ms: int, duration_ms: int, gap_ms: int) -> list[int]:
    """One request every ``gap_ms`` milliseconds."""
    return list(range(start_ms, start_ms + duration_ms, gap_ms))
