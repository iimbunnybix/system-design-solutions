"""One big tenant's queries all land on one worker under consistent hashing.

A multi-tenant query service routes every query to a worker by consistent
hashing on the tenant id, so each tenant's data stays warm in one worker's
local cache. That spreads *tenants* evenly, not *load*: when one tenant
sends 40% of all concurrent queries, every one of them goes to the same
worker, which overflows while the others sit half idle.

The fix is consistent hashing with bounded loads (Mirrokni, Thorup and
Zadimoghaddam, 2016): no worker may hold more than ``c`` times the average
load. A query goes to its tenant's home worker if that worker is under the
bound, otherwise to the next worker clockwise on the ring that is. Most
tenants keep their home worker; only the overflow moves.
"""
from __future__ import annotations

import hashlib
import math
import random
from bisect import bisect_right
from typing import Iterator

WORKERS = [f"worker-{i}" for i in range(10)]
VNODES_PER_WORKER = 100
WORKER_SLOTS = 150                # concurrent queries a worker can run before queueing
LOAD_FACTOR = 1.25                # c: max load as a multiple of the average
TENANTS = 200
CONCURRENT_QUERIES = 1_000
HOT_TENANT = "tenant-0"
HOT_SHARE = 0.40                  # month-end dashboard refresh of one big tenant
SEED = 34


def ring_point(label: str) -> int:
    """Stable 64-bit position on the ring (Python's hash() changes per process)."""
    return int.from_bytes(hashlib.md5(label.encode()).digest()[:8], "big")


class HashRing:
    """Consistent-hash ring with virtual nodes."""

    def __init__(self, workers: list[str], vnodes: int = VNODES_PER_WORKER) -> None:
        self.workers = list(workers)
        points = []
        for worker in workers:
            for v in range(vnodes):
                points.append((ring_point(f"{worker}#{v}"), worker))
        points.sort()
        self._positions = [p for p, _ in points]
        self._owners = [w for _, w in points]

    def home(self, key: str) -> str:
        """The worker that owns ``key`` in plain consistent hashing."""
        return next(self.walk(key))

    def walk(self, key: str) -> Iterator[str]:
        """Each worker once, in clockwise order starting at ``key``'s position."""
        start = bisect_right(self._positions, ring_point(key))
        seen: set[str] = set()
        for step in range(len(self._owners)):
            worker = self._owners[(start + step) % len(self._owners)]
            if worker not in seen:
                seen.add(worker)
                yield worker
            if len(seen) == len(self.workers):
                return


class NaiveRouter:
    """Original: every query goes to its tenant's home worker."""

    def __init__(self, ring: HashRing) -> None:
        self.ring = ring
        self.load = {w: 0 for w in ring.workers}

    def assign(self, tenant: str) -> str:
        worker = self.ring.home(tenant)
        self.load[worker] += 1
        return worker

    def release(self, worker: str) -> None:
        """A query finished on ``worker``."""
        self.load[worker] -= 1


class BoundedLoadRouter:
    """Consistent hashing with bounded loads: no worker above c x average."""

    def __init__(self, ring: HashRing, load_factor: float = LOAD_FACTOR) -> None:
        if load_factor <= 1.0:
            # With c <= 1 the bound can be below the average and a query may
            # find no worker at all.
            raise ValueError("load_factor must be greater than 1")
        self.ring = ring
        self.load_factor = load_factor
        self.load = {w: 0 for w in ring.workers}

    def capacity(self) -> int:
        """Per-worker bound counting the query being placed.

        ceil(c * (in_flight + 1) / workers): the workers together have room
        for at least c * (in_flight + 1) > in_flight + 1 queries, so the walk
        always finds a worker under the bound.
        """
        in_flight = sum(self.load.values())
        return math.ceil(self.load_factor * (in_flight + 1) / len(self.ring.workers))

    def assign(self, tenant: str) -> str:
        """Home worker if it is under the bound, else the next one clockwise that is."""
        bound = self.capacity()
        for worker in self.ring.walk(tenant):
            if self.load[worker] < bound:
                self.load[worker] += 1
                return worker
        raise RuntimeError("unreachable: total capacity exceeds in-flight queries")

    def release(self, worker: str) -> None:
        """A query finished on ``worker``; frees one unit of its bound."""
        self.load[worker] -= 1


def workload(seed: int = SEED) -> list[str]:
    """Tenant ids of the queries in flight at the peak, in arrival order."""
    rng = random.Random(seed)
    hot = round(CONCURRENT_QUERIES * HOT_SHARE)
    queries = [HOT_TENANT] * hot
    for _ in range(CONCURRENT_QUERIES - hot):
        queries.append(f"tenant-{rng.randrange(1, TENANTS)}")
    rng.shuffle(queries)
    return queries


def simulate(router_class: type, seed: int = SEED) -> dict[str, float]:
    """Place the peak's queries; report load and how many keep their home worker."""
    ring = HashRing(WORKERS)
    router = router_class(ring)
    other_total = 0
    other_at_home = 0
    hot_workers: set[str] = set()
    for tenant in workload(seed):
        worker = router.assign(tenant)
        if tenant == HOT_TENANT:
            hot_workers.add(worker)
        else:
            other_total += 1
            other_at_home += worker == ring.home(tenant)
    loads = router.load.values()
    over_capacity = sum(max(0, load - WORKER_SLOTS) for load in loads)
    return {"max_load": max(loads), "min_load": min(loads),
            "over_capacity": over_capacity, "hot_tenant_workers": len(hot_workers),
            "other_tenants_at_home": other_at_home / other_total}
