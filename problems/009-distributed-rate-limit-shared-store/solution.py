"""Enforce one API quota across a fleet of gateway nodes.

Each gateway node originally kept its own in-memory counter per API key, so a
"100 requests/minute" plan really allowed 100 x (number of nodes), and the
limit silently grew every time the fleet autoscaled. Dividing the limit by the
node count was tried next; it blocks well-behaved clients whenever load
balancing is uneven (sticky sessions, consistent hashing, few connections).

The fix keeps the rate-limit state in a shared store (Redis in production) and makes
the check-and-record one atomic operation (a Lua script in Redis), using a
**sliding log**: the timestamps of the admitted requests in the last 60 seconds
(a sorted set in Redis). A request is admitted only if fewer than ``limit``
timestamps are in the window, so no rolling 60 s window ever holds more than
the quota, even for a client that bursts across a minute boundary.
If the store is unreachable, nodes fall back to a conservative local limit.
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Optional


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class StoreUnavailable(Exception):
    pass


class SharedStore:
    """Stand-in for Redis: one log of admitted timestamps per key, behind an atomic script hook."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self._logs: dict[str, deque[float]] = {}
        self._lock = threading.Lock()
        self.available = True
        self.calls = 0

    def sliding_log_acquire(self, key: str, limit: int, window: float) -> tuple[bool, int]:
        """Atomically check and record one request. Returns (allowed, count in window).

        Equivalent to a single Redis Lua script on a sorted set:
        ZREMRANGEBYSCORE (drop entries older than the window), ZCARD, then ZADD
        if allowed. Atomicity is what prevents two nodes from both admitting
        the 100th request. Only admitted requests are logged, so a key never
        stores more than ``limit`` timestamps.
        """
        if not self.available:
            raise StoreUnavailable("redis: connection refused")
        with self._lock:
            self.calls += 1
            now = self.clock.now()
            log = self._logs.setdefault(key, deque())
            # The window is (now - window, now]: a request exactly `window`
            # seconds old no longer counts. Timestamps arrive in order, so the
            # oldest ones are always at the left end.
            while log and log[0] <= now - window:
                log.popleft()
            if len(log) >= limit:
                return False, len(log)
            log.append(now)
            return True, len(log)


class LocalFixedWindow:
    """Per-node, in-memory fixed-window counter (the original design)."""

    def __init__(self, clock: FakeClock, limit: int, window: float = 60.0) -> None:
        self.clock = clock
        self.limit = limit
        self.window = window
        self._counts: dict[tuple[str, int], int] = {}

    def allow(self, key: str) -> bool:
        idx = int(self.clock.now() // self.window)
        n = self._counts.get((key, idx), 0)
        if n >= self.limit:
            return False
        self._counts[(key, idx)] = n + 1
        return True


def original_fallback_share(limit: int, fleet_size: int) -> int:
    """First outage policy: floor each node's share at 1 request.

    Wrong for small plans: 5/min on 20 nodes gives every node 1, so the fleet
    admits 20/min, the "quota x fleet size" bug again.
    """
    return max(1, limit // max(1, fleet_size))


def fallback_share(limit: int, fleet_size: int, node_index: int) -> int:
    """Whole requests per window this node may admit while the store is down.

    The quota is dealt out like cards: every node gets ``limit // fleet_size``
    and the first ``limit % fleet_size`` nodes get one extra. The shares add up
    to exactly ``limit``, so the fleet total never exceeds the quota, even when
    the plan is smaller than the fleet (5/min on 20 nodes: nodes 0-4 admit 1
    each, nodes 5-19 fail closed).
    """
    fleet_size = max(1, fleet_size)
    base, extra = divmod(limit, fleet_size)
    if node_index % fleet_size < extra:
        return base + 1
    return base


@dataclass
class GatewayNode:
    """A gateway node using the shared limiter, with a local fallback."""

    name: str
    store: SharedStore
    clock: FakeClock
    limit: int
    window: float = 60.0
    fleet_size_hint: int = 1
    node_index: int = 0
    fallback: Optional[LocalFixedWindow] = None
    degraded_decisions: int = 0

    def __post_init__(self) -> None:
        if self.fallback is None:
            share = fallback_share(self.limit, self.fleet_size_hint, self.node_index)
            self.fallback = LocalFixedWindow(self.clock, share, self.window)

    def allow(self, api_key: str) -> bool:
        try:
            allowed, _ = self.store.sliding_log_acquire(f"rl:{api_key}", self.limit, self.window)
            return allowed
        except StoreUnavailable:
            self.degraded_decisions += 1
            return self.fallback.allow(api_key)


def simulate(nodes: list, clock: FakeClock, rate_per_sec: float, seconds: float,
             route=None, key: str = "client-1") -> list[float]:
    """Send evenly spaced requests; return timestamps of admitted requests.

    ``nodes`` are objects with ``allow(key)``; ``route(i)`` picks the node index
    for request i (round-robin by default).
    """
    gap = 1.0 / rate_per_sec
    admitted = []
    for i in range(int(rate_per_sec * seconds)):
        node = nodes[route(i) if route else i % len(nodes)]
        if node.allow(key):
            admitted.append(clock.now())
        clock.advance(gap)
    return admitted


def max_in_any_window(timestamps: list[float], window: float = 60.0) -> int:
    """Largest number of admitted requests in any rolling window (two-pointer scan)."""
    best, lo = 0, 0
    for hi, t in enumerate(timestamps):
        while timestamps[lo] <= t - window:
            lo += 1
        best = max(best, hi - lo + 1)
    return best
