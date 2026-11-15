"""Give every ID-generating pod a worker ID that no other live pod holds.

Order IDs are Snowflake-style 64-bit numbers: timestamp, worker ID, sequence.
They are unique only if no two running pods share a worker ID. The service
derived the worker ID by hashing the pod name into the 10-bit space, and with
dozens of pods two of them landed on the same ID and issued identical order
IDs. This module keeps that scheme and adds a lease registry: a pod borrows a
free worker ID for a limited time, renews it while alive, and stops issuing
IDs the moment its lease lapses, so an ID is never held by two pods at once.
"""
from __future__ import annotations

import math
import zlib
from dataclasses import dataclass
from typing import Callable

WORKER_ID_BITS = 10
SEQUENCE_BITS = 12
MAX_WORKER_IDS = 1 << WORKER_ID_BITS        # 1,024 possible worker IDs
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1     # 4,095 IDs per millisecond per worker
LEASE_TTL_MS = 30_000                       # a worker ID is borrowed for 30 s at a time
RENEW_SAFETY_MARGIN_MS = 5_000              # stop issuing 5 s before the lease ends


class NoWorkerIdAvailable(Exception):
    """All worker IDs are leased by live pods."""


class LeaseExpired(Exception):
    """The pod no longer owns its worker ID, so issuing IDs could duplicate."""


def compose_id(timestamp_ms: int, worker_id: int, sequence: int) -> int:
    """Pack the three fields into one 64-bit ID."""
    return (timestamp_ms << (WORKER_ID_BITS + SEQUENCE_BITS)) | (worker_id << SEQUENCE_BITS) | sequence


def naive_worker_id(pod_name: str) -> int:
    """Original scheme: hash the pod name into the worker-ID space.

    Different names can map to the same ID; nothing checks for that.
    """
    return zlib.crc32(pod_name.encode("utf-8")) % MAX_WORKER_IDS


def chance_of_any_collision(pod_count: int, slot_count: int = MAX_WORKER_IDS) -> float:
    """Birthday-problem probability that at least two pods hash to one slot."""
    if pod_count > slot_count:
        return 1.0
    log_all_distinct = 0.0
    for already_placed in range(pod_count):
        log_all_distinct += math.log((slot_count - already_placed) / slot_count)
    return 1.0 - math.exp(log_all_distinct)


@dataclass
class Lease:
    owner: str
    worker_id: int
    expires_at_ms: int


class WorkerIdRegistry:
    """Shared store (etcd, ZooKeeper, a DB table) handing out worker IDs as leases."""

    def __init__(self, ttl_ms: int = LEASE_TTL_MS) -> None:
        self.ttl_ms = ttl_ms
        self.leases: dict[int, Lease] = {}      # worker_id -> current lease

    def acquire(self, owner: str, now_ms: int) -> Lease:
        """Lease the lowest worker ID that is free or whose lease has expired."""
        for worker_id in range(MAX_WORKER_IDS):
            current = self.leases.get(worker_id)
            if current is None or current.expires_at_ms <= now_ms:
                lease = Lease(owner, worker_id, now_ms + self.ttl_ms)
                self.leases[worker_id] = lease
                return lease
        raise NoWorkerIdAvailable(f"all {MAX_WORKER_IDS} worker IDs are leased")

    def renew(self, lease: Lease, now_ms: int) -> bool:
        """Extend a lease we still own; False if it expired or was taken over."""
        current = self.leases.get(lease.worker_id)
        if current is None or current.owner != lease.owner or current.expires_at_ms <= now_ms:
            return False
        current.expires_at_ms = now_ms + self.ttl_ms
        lease.expires_at_ms = current.expires_at_ms
        return True


class IdGenerator:
    """Snowflake generator that only issues IDs while its worker ID is leased."""

    def __init__(self, worker_id: int, clock_ms: Callable[[], int],
                 lease: Lease | None = None) -> None:
        self.worker_id = worker_id
        self.clock_ms = clock_ms
        self.lease = lease
        self.last_timestamp_ms = -1
        self.sequence = 0

    def _check_lease(self, now_ms: int) -> None:
        """Refuse before the lease ends: another pod may get this ID right after."""
        if self.lease is None:
            return
        # The margin covers clock skew between this pod and the registry.
        if now_ms >= self.lease.expires_at_ms - RENEW_SAFETY_MARGIN_MS:
            raise LeaseExpired(f"worker ID {self.worker_id} lease is ending; renew first")

    def next_id(self) -> int:
        """Return a new ID; the sequence separates IDs within one millisecond."""
        now_ms = self.clock_ms()
        self._check_lease(now_ms)
        if now_ms == self.last_timestamp_ms:
            if self.sequence == MAX_SEQUENCE:
                raise OverflowError("sequence exhausted for this millisecond")
            self.sequence += 1
        else:
            self.sequence = 0
            self.last_timestamp_ms = now_ms
        return compose_id(now_ms, self.worker_id, self.sequence)


def leased_generator(registry: WorkerIdRegistry, pod_name: str,
                     clock_ms: Callable[[], int]) -> IdGenerator:
    """Fixed start-up: borrow a unique worker ID instead of hashing the pod name."""
    lease = registry.acquire(pod_name, clock_ms())
    return IdGenerator(lease.worker_id, clock_ms, lease)
