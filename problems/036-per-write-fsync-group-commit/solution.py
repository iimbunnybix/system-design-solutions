"""Raise durable write throughput 10x by sharing one fsync across many writes.

An order-event log flushed every write to disk on its own (write + fsync)
before acknowledging it. One fsync takes about 2 ms on the volume, so the log
could never acknowledge more than ~500 writes per second, however fast the
CPU was; at 5,000 writes/s the queue grew without bound and latency rose to
seconds.

``GroupCommitLog`` keeps the same guarantee (a write is acknowledged only
after an fsync that includes it) but lets every write that arrived while the
previous fsync was running share the next one. ``simulate`` replays a
deterministic arrival schedule against either log with a fake clock.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

FSYNC_US = 2_000            # one fsync on the volume takes 2 ms
DEFAULT_MAX_BATCH = 256     # cap a batch so one fsync's payload stays bounded


class FakeDisk:
    """Counts fsyncs and remembers which records would survive a crash."""

    def __init__(self) -> None:
        self.buffered: list[str] = []   # written, still only in the page cache
        self.durable: list[str] = []    # covered by a completed fsync
        self.fsync_count = 0

    def write(self, record: str) -> None:
        self.buffered.append(record)

    def fsync(self) -> None:
        self.durable.extend(self.buffered)
        self.buffered.clear()
        self.fsync_count += 1


class NaiveLog:
    """Original log: every append pays for its own fsync."""

    max_batch = 1

    def __init__(self, disk: FakeDisk) -> None:
        self.disk = disk
        self.pending: list[str] = []

    def submit(self, record: str) -> None:
        self.pending.append(record)

    def flush(self) -> list[str]:
        """Make exactly one pending record durable and return it."""
        record = self.pending.pop(0)
        self.disk.write(record)
        self.disk.fsync()
        return [record]


class GroupCommitLog(NaiveLog):
    """Same durability rule, but one fsync covers every waiting record."""

    def __init__(self, disk: FakeDisk, max_batch: int = DEFAULT_MAX_BATCH) -> None:
        super().__init__(disk)
        if max_batch < 1:
            raise ValueError("max_batch must be at least 1")
        self.max_batch = max_batch

    def flush(self) -> list[str]:
        """Write up to ``max_batch`` waiting records, then fsync once for all of them."""
        batch = self.pending[:self.max_batch]
        del self.pending[:self.max_batch]
        for record in batch:
            self.disk.write(record)
        # The ack for every record in the batch waits for this single fsync,
        # so no record is acknowledged before it is durable.
        self.disk.fsync()
        return batch


@dataclass
class SimulationResult:
    ack_latency_us: list[int] = field(default_factory=list)
    fsyncs: int = 0
    last_ack_us: int = 0

    def percentile_us(self, percent: float) -> int:
        """Nearest-rank percentile of acknowledgement latency."""
        ordered = sorted(self.ack_latency_us)
        rank = max(1, math.ceil(percent / 100 * len(ordered)))
        return ordered[rank - 1]


def simulate(log: NaiveLog, arrivals_us: list[int], fsync_us: int = FSYNC_US) -> SimulationResult:
    """Replay writes arriving at ``arrivals_us`` (sorted) against one disk.

    The disk does one flush at a time. When it becomes free, the log flushes
    whatever has arrived by then; if nothing has, it waits for the next write
    (no artificial delay is added to build bigger batches).
    """
    result = SimulationResult()
    arrival_of: dict[str, int] = {}
    next_index = 0
    disk_free_at_us = 0
    while next_index < len(arrivals_us) or log.pending:
        if not log.pending:
            disk_free_at_us = max(disk_free_at_us, arrivals_us[next_index])
        # Every write that arrived before the disk became free joins the queue.
        while next_index < len(arrivals_us) and arrivals_us[next_index] <= disk_free_at_us:
            record = f"order-event-{next_index}"
            arrival_of[record] = arrivals_us[next_index]
            log.submit(record)
            next_index += 1
        flushed = log.flush()
        disk_free_at_us += fsync_us
        for record in flushed:
            result.ack_latency_us.append(disk_free_at_us - arrival_of[record])
    result.fsyncs = log.disk.fsync_count
    result.last_ack_us = disk_free_at_us
    return result


def steady_arrivals(writes_per_second: int, duration_seconds: int) -> list[int]:
    """Evenly spaced arrival times in microseconds (deterministic load)."""
    gap_us = 1_000_000 // writes_per_second
    return [index * gap_us for index in range(writes_per_second * duration_seconds)]
