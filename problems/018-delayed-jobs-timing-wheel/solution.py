"""Fire delayed jobs on time without scanning every pending job each second.

The original scheduler kept all pending jobs in one list and, once a second,
checked every job's due time. Work per tick grew with the number of pending
jobs, so at a few million queued reminders a tick took longer than a second
and jobs fired later and later. This module keeps that version and adds a
hierarchical timing wheel: jobs are filed into slots by due time, a tick only
looks at the slot for "now", and far-future jobs move down a level once per
minute or wait in a heap until they come within the wheel's one-hour horizon.
"""
from __future__ import annotations

import heapq
from dataclasses import dataclass, field

SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
WHEEL_HORIZON_SECONDS = SECONDS_PER_MINUTE * MINUTES_PER_HOUR   # beyond this -> overflow heap


@dataclass(order=True)
class Job:
    due_tick: int                                  # absolute second at which to run
    job_id: str = field(compare=True)


class NaiveScheduler:
    """Original design: one list, scanned in full on every tick."""

    def __init__(self) -> None:
        self.current_tick = 0
        self.pending: list[Job] = []
        self.jobs_examined = 0

    def schedule(self, job: Job) -> None:
        self.pending.append(job)

    def tick(self) -> list[Job]:
        """Advance one second and return the jobs that are now due."""
        self.current_tick += 1
        due: list[Job] = []
        still_waiting: list[Job] = []
        for job in self.pending:
            # This check runs for EVERY pending job, every second.
            self.jobs_examined += 1
            if job.due_tick <= self.current_tick:
                due.append(job)
            else:
                still_waiting.append(job)
        self.pending = still_waiting
        return due


class TimingWheelScheduler:
    """Two-level timing wheel (seconds, minutes) plus a heap for jobs over an hour away."""

    def __init__(self) -> None:
        self.current_tick = 0
        self.ready: list[Job] = []                 # due now or overdue at schedule time
        self.second_slots: list[list[Job]] = [[] for _ in range(SECONDS_PER_MINUTE)]
        self.minute_slots: list[list[Job]] = [[] for _ in range(MINUTES_PER_HOUR)]
        self.overflow: list[Job] = []              # min-heap ordered by due_tick
        self.jobs_examined = 0

    def schedule(self, job: Job) -> None:
        """File a job in the place that will be looked at when it is (nearly) due."""
        delay = job.due_tick - self.current_tick
        if delay <= 0:
            self.ready.append(job)
        elif delay < SECONDS_PER_MINUTE:
            # Unique slot within the next minute: it is visited exactly at due_tick.
            self.second_slots[job.due_tick % SECONDS_PER_MINUTE].append(job)
        elif delay < WHEEL_HORIZON_SECONDS:
            due_minute = job.due_tick // SECONDS_PER_MINUTE
            self.minute_slots[due_minute % MINUTES_PER_HOUR].append(job)
        else:
            heapq.heappush(self.overflow, job)

    def tick(self) -> list[Job]:
        """Advance one second and return the jobs that are now due."""
        self.current_tick += 1
        if self.current_tick % SECONDS_PER_MINUTE == 0:
            self._cascade_current_minute()
        self._pull_from_overflow()
        return self._take_due_jobs()

    def _cascade_current_minute(self) -> None:
        """At each minute boundary, move this minute's jobs down into the seconds wheel."""
        minute_index = (self.current_tick // SECONDS_PER_MINUTE) % MINUTES_PER_HOUR
        jobs = self.minute_slots[minute_index]
        self.minute_slots[minute_index] = []
        for job in jobs:
            self.jobs_examined += 1
            self.schedule(job)                     # now less than a minute away

    def _pull_from_overflow(self) -> None:
        """Move heap jobs that have come within the wheel's horizon into the wheel."""
        # Only the heap's top is checked when nothing is close: O(1) on a quiet tick.
        while self.overflow:
            soonest = self.overflow[0]
            if soonest.due_tick - self.current_tick >= WHEEL_HORIZON_SECONDS:
                break
            heapq.heappop(self.overflow)
            self.jobs_examined += 1
            self.schedule(soonest)

    def _take_due_jobs(self) -> list[Job]:
        """Empty the ready list and the slot for the current second."""
        slot_index = self.current_tick % SECONDS_PER_MINUTE
        due = self.ready + self.second_slots[slot_index]
        self.ready = []
        self.second_slots[slot_index] = []
        self.jobs_examined += len(due)
        return due

    def pending_count(self) -> int:
        """Jobs scheduled but not yet returned by tick()."""
        in_seconds = sum(len(slot) for slot in self.second_slots)
        in_minutes = sum(len(slot) for slot in self.minute_slots)
        return len(self.ready) + in_seconds + in_minutes + len(self.overflow)
