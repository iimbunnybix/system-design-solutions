"""Stop one tenant's bulk upload from starving everyone: deficit round robin.

A shared export service ran every tenant's jobs from one FIFO queue. When one
tenant submitted thousands of jobs at once, every other tenant's job queued
behind all of them and waited minutes, even though it needed 50 ms of work.

``FifoQueue`` is the original queue. ``DeficitRoundRobinQueue`` keeps one
queue per tenant and visits tenants in turn; each visit adds a fixed amount
of work time (the quantum) to the tenant's "deficit" budget, and the tenant
may run jobs while their cost fits in that budget. Every busy tenant gets an
equal share of worker time no matter how many jobs it queued or how big they
are, and an idle worker never waits while any job is queued.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Protocol

DEFAULT_QUANTUM_MS = 50


@dataclass(frozen=True)
class Job:
    tenant: str
    job_id: int
    cost_ms: int          # how long the job keeps the worker busy
    submitted_at_ms: int


class JobQueue(Protocol):
    def push(self, job: Job) -> None: ...
    def pop(self) -> Job: ...
    def __len__(self) -> int: ...


class FifoQueue:
    """Original design: one queue for everyone, served in arrival order."""

    def __init__(self) -> None:
        self._jobs: deque[Job] = deque()

    def push(self, job: Job) -> None:
        self._jobs.append(job)

    def pop(self) -> Job:
        return self._jobs.popleft()

    def __len__(self) -> int:
        return len(self._jobs)


class DeficitRoundRobinQueue:
    """Fair share of worker time per tenant (Shreedhar & Varghese's DRR)."""

    def __init__(self, quantum_ms: int = DEFAULT_QUANTUM_MS) -> None:
        if quantum_ms <= 0:
            raise ValueError("quantum_ms must be positive")
        self.quantum_ms = quantum_ms
        self._queues: dict[str, deque[Job]] = {}
        self._deficit_ms: dict[str, int] = {}
        # Tenants that have queued jobs, in the order they will be visited.
        self._active: deque[str] = deque()
        # True once the tenant at the front has received this visit's quantum,
        # so serving several small jobs in one visit adds the quantum only once.
        self._visit_started = False
        self._size = 0

    def push(self, job: Job) -> None:
        """Queue a job; a tenant with no queued jobs joins the back of the round."""
        if job.cost_ms <= 0:
            raise ValueError("cost_ms must be positive")
        if job.tenant not in self._queues:
            self._queues[job.tenant] = deque()
            self._deficit_ms[job.tenant] = 0
            self._active.append(job.tenant)
        self._queues[job.tenant].append(job)
        self._size += 1

    def pop(self) -> Job:
        """Return the next job to run. Raises IndexError when empty."""
        if self._size == 0:
            raise IndexError("pop from empty queue")
        while True:
            tenant = self._active[0]
            if not self._visit_started:
                self._deficit_ms[tenant] += self.quantum_ms
                self._visit_started = True
            queue = self._queues[tenant]
            if queue[0].cost_ms <= self._deficit_ms[tenant]:
                return self._serve(tenant)
            # Next job does not fit this visit's budget: keep the unused budget
            # (so a job bigger than one quantum runs after a few visits) and
            # move on to the next tenant.
            self._move_to_next_tenant()

    def _serve(self, tenant: str) -> Job:
        """Take the tenant's next job and charge its cost to the budget."""
        queue = self._queues[tenant]
        job = queue.popleft()
        self._deficit_ms[tenant] -= job.cost_ms
        self._size -= 1
        if not queue:
            # An emptied tenant leaves the round and loses its leftover budget;
            # otherwise a tenant could save up credit while idle and burst later.
            del self._queues[tenant]
            del self._deficit_ms[tenant]
            self._active.popleft()
            self._visit_started = False
        return job

    def _move_to_next_tenant(self) -> None:
        """End the current tenant's visit and send it to the back of the round."""
        self._active.rotate(-1)
        self._visit_started = False

    def __len__(self) -> int:
        return self._size


def run_single_worker(queue: JobQueue, jobs: list[Job]) -> dict[int, int]:
    """Simulate one worker with a fake clock. Returns job_id -> finish time (ms).

    Jobs arrive at ``submitted_at_ms``; the worker always takes the next job
    the queue gives it and never idles while a job is queued.
    """
    arrivals = deque(sorted(jobs, key=lambda job: (job.submitted_at_ms, job.job_id)))
    finished_at: dict[int, int] = {}
    clock_ms = 0
    while arrivals or len(queue):
        # Nothing to do: jump the clock to the next arrival.
        if not len(queue) and arrivals[0].submitted_at_ms > clock_ms:
            clock_ms = arrivals[0].submitted_at_ms
        while arrivals and arrivals[0].submitted_at_ms <= clock_ms:
            queue.push(arrivals.popleft())
        job = queue.pop()
        clock_ms += job.cost_ms
        finished_at[job.job_id] = clock_ms
    return finished_at


def wait_ms(job: Job, finished_at: dict[int, int]) -> int:
    """Time from submission until the job started running."""
    return finished_at[job.job_id] - job.cost_ms - job.submitted_at_ms
