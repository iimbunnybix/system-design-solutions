"""Limit the blast radius of a poison tenant with shuffle sharding.

A document-rendering service spread every tenant's requests over all eight
workers. One tenant uploaded a file that crashed the renderer; its client
retried, each retry landed on the next worker, and within seconds all eight
were down for every tenant. Shuffle sharding gives each tenant its own small,
stable combination of workers (2 of 8 here). The poison tenant can only crash
its own two, and another tenant loses service only if it was assigned exactly
the same pair, 1 combination in 28, instead of every tenant losing service.
"""
from __future__ import annotations

import hashlib
import itertools
import random

WORKER_COUNT = 8
WORKERS_PER_TENANT = 2


def worker_names(count: int = WORKER_COUNT) -> list[str]:
    return [f"worker-{number}" for number in range(count)]


def naive_route(request_number: int, workers: list[str]) -> str:
    """Original load balancer: round-robin every request over every worker."""
    return workers[request_number % len(workers)]


def shuffle_shard(tenant: str, workers: list[str],
                  workers_per_tenant: int = WORKERS_PER_TENANT) -> list[str]:
    """Pick a stable, pseudo-random set of workers for one tenant.

    Seeding from a hash of the tenant name (not Python's hash(), which changes
    per process) means every load balancer computes the same shard.
    """
    if not 1 <= workers_per_tenant <= len(workers):
        raise ValueError("workers_per_tenant must be between 1 and the worker count")
    digest = hashlib.sha256(tenant.encode("utf-8")).digest()
    generator = random.Random(int.from_bytes(digest[:8], "big"))
    return sorted(generator.sample(workers, workers_per_tenant))


class Fleet:
    """Workers that crash permanently when they process a poison request."""

    def __init__(self, workers: list[str]) -> None:
        self.alive = set(workers)

    def handle(self, worker: str, is_poison: bool) -> bool:
        """Process one request on worker; returns True if it was served."""
        if worker not in self.alive:
            return False
        if is_poison:
            self.alive.discard(worker)
            return False
        return True


def poison_attack_naive(fleet: Fleet, workers: list[str], retries: int) -> None:
    """The poison tenant's client retries; round-robin spreads it to every worker."""
    for request_number in range(retries):
        fleet.handle(naive_route(request_number, workers), is_poison=True)


def poison_attack_sharded(fleet: Fleet, tenant: str, workers: list[str], retries: int) -> None:
    """The same retries, but they can only ever reach the tenant's own shard."""
    shard = shuffle_shard(tenant, workers)
    for request_number in range(retries):
        fleet.handle(shard[request_number % len(shard)], is_poison=True)


def tenant_is_served(fleet: Fleet, tenant: str, workers: list[str]) -> bool:
    """A healthy tenant's client tries each worker in its shard until one answers."""
    for worker in shuffle_shard(tenant, workers):
        if fleet.handle(worker, is_poison=False):
            return True
    return False


def share_of_tenants_down(fleet: Fleet, tenants: list[str], workers: list[str],
                          use_shards: bool) -> float:
    """Fraction of tenants that cannot get any request served."""
    down = 0
    for tenant in tenants:
        if use_shards:
            served = tenant_is_served(fleet, tenant, workers)
        else:
            served = bool(fleet.alive)
        if not served:
            down += 1
    return down / len(tenants)


def possible_shards(worker_count: int = WORKER_COUNT,
                    workers_per_tenant: int = WORKERS_PER_TENANT) -> int:
    """How many distinct shards exist: worker_count choose workers_per_tenant."""
    return len(list(itertools.combinations(range(worker_count), workers_per_tenant)))
