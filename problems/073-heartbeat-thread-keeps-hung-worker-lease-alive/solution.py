"""A background heartbeat kept a hung worker's job lease alive forever.

Workers lease a job for 30 s and a heartbeat thread renews the lease every
10 s. The heartbeat thread runs independently of the work, so when a worker
deadlocked halfway through a video transcode, the thread kept renewing and
the job was never handed to another worker. The fix makes each heartbeat
report a progress counter: the coordinator renews only while progress keeps
moving, lets a stalled lease expire, and hands the job to another worker
with a new fencing token, so the old worker cannot complete it later.
"""
from __future__ import annotations

from dataclasses import dataclass

LEASE_SECONDS = 30
HEARTBEAT_EVERY_SECONDS = 10
MAX_STALL_SECONDS = 60               # longest legitimate gap between progress updates
JOB_WORK_SECONDS = 50
HANG_AT_SECOND = 20
SIMULATED_SECONDS = 600


@dataclass
class Lease:
    worker: str
    token: int                        # fencing token: grows with every new lease
    expires_at: int
    progress: int = 0
    progress_changed_at: int = 0


class Coordinator:
    """Owns the lease for one job and decides who may work on it."""

    def __init__(self, check_progress: bool) -> None:
        self.check_progress = check_progress
        self.lease: Lease | None = None
        self.next_token = 1
        self.completed_by: str | None = None

    def acquire(self, worker: str, now: int) -> Lease | None:
        """Give the job to ``worker`` if nobody holds a live lease."""
        if self.completed_by is not None:
            return None
        if self.lease is not None and self.lease.expires_at > now:
            return None
        self.lease = Lease(worker, self.next_token, now + LEASE_SECONDS, 0, now)
        self.next_token += 1
        return self.lease

    def heartbeat(self, token: int, progress: int, now: int) -> bool:
        """Renew the lease; with progress checks, only if work is moving."""
        lease = self.lease
        if lease is None or lease.token != token or lease.expires_at <= now:
            return False
        if progress > lease.progress:
            lease.progress = progress
            lease.progress_changed_at = now
        # The original design skipped this check: alive was taken to mean working.
        if self.check_progress and now - lease.progress_changed_at > MAX_STALL_SECONDS:
            return False                   # refuse renewal; the lease runs out on its own
        lease.expires_at = now + LEASE_SECONDS
        return True

    def complete(self, token: int, now: int) -> bool:
        """Accept a result only from the current, unexpired lease holder."""
        lease = self.lease
        if lease is None or lease.token != token or lease.expires_at <= now:
            return False
        self.completed_by = lease.worker
        return True


@dataclass
class Worker:
    """A worker that makes one unit of progress per second until it hangs."""

    name: str
    hangs_at: int | None = None       # seconds into the job; None never hangs
    lease: Lease | None = None
    progress: int = 0
    stuck: bool = False               # main thread deadlocked; only heartbeats run

    def tick(self, coordinator: Coordinator, now: int) -> None:
        """One simulated second: try to get the job, work, heartbeat, finish."""
        if self.lease is None:
            if self.stuck:
                return                     # a deadlocked main thread asks for no work
            self.lease = coordinator.acquire(self.name, now)
            self.progress = 0
            return
        if self.hangs_at is not None and self.progress >= self.hangs_at:
            self.stuck = True
        if not self.stuck:
            self.progress += 1
        # The heartbeat thread keeps running even while the work is stuck.
        if now % HEARTBEAT_EVERY_SECONDS == 0:
            if not coordinator.heartbeat(self.lease.token, self.progress, now):
                self.lease = None          # lost the job; stop working on it
                return
        if self.progress >= JOB_WORK_SECONDS:
            coordinator.complete(self.lease.token, now)
            self.lease = None


def simulate(check_progress: bool) -> tuple[str | None, int | None]:
    """Worker A hangs 20 s in; worker B is idle and waiting for work.

    Returns who completed the job and when (None, None if nobody did).
    """
    coordinator = Coordinator(check_progress)
    hung_worker = Worker("worker-a", hangs_at=HANG_AT_SECOND)
    spare_worker = Worker("worker-b")
    for now in range(SIMULATED_SECONDS):
        hung_worker.tick(coordinator, now)
        spare_worker.tick(coordinator, now)
        if coordinator.completed_by is not None:
            return coordinator.completed_by, now
    return None, None
