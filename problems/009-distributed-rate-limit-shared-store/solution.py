"""Enforce one API quota across a fleet of gateway nodes.

Each gateway node originally kept its own in-memory counter per API key, so a
"100 requests/minute" plan really allowed 100 x (number of nodes), and the
limit silently grew every time the fleet autoscaled. Dividing the limit by the
node count was tried next; it blocks well-behaved clients whenever load
balancing is uneven (sticky sessions, consistent hashing, few connections).

The fix keeps the counters in a shared store (Redis in production) and makes
the check-and-increment one atomic operation (a Lua script in Redis), using a
**sliding window counter**: the current fixed window's count plus the previous
window's count weighted by how much of it still overlaps the last 60 seconds.
If the store is unreachable, nodes fall back to a conservative local limit.
"""
from __future__ import annotations

import math
import threading
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
    """Stand-in for Redis: integer counters with expiry and an atomic script hook."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self._data: dict[str, tuple[int, float]] = {}
        self._lock = threading.Lock()
        self.available = True
        self.calls = 0

    def _get(self, key: str) -> int:
        value, expires = self._data.get(key, (0, math.inf))
        return value if self.clock.now() < expires else 0

    def sliding_window_acquire(self, key: str, limit: int, window: float) -> tuple[bool, float]:
        """Atomically check and record one request. Returns (allowed, estimated count).

        Equivalent to a single Redis Lua script: GET prev, GET curr, compute,
        INCR + EXPIRE curr if allowed. Atomicity is what prevents two nodes from
        both admitting the 100th request.
        """
        if not self.available:
            raise StoreUnavailable("redis: connection refused")
        with self._lock:
            self.calls += 1
            now = self.clock.now()
            idx = int(now // window)
            elapsed = (now - idx * window) / window
            prev = self._get(f"{key}:{idx - 1}")
            curr = self._get(f"{key}:{idx}")
            estimate = prev * (1 - elapsed) + curr
            if estimate + 1 > limit:
                return False, estimate
            self._data[f"{key}:{idx}"] = (curr + 1, (idx + 2) * window)  # keep for next window
            return True, estimate + 1


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


@dataclass
class GatewayNode:
    """A gateway node using the shared limiter, with a local fallback."""

    name: str
    store: SharedStore
    clock: FakeClock
    limit: int
    window: float = 60.0
    fleet_size_hint: int = 1
    fallback: Optional[LocalFixedWindow] = None
    degraded_decisions: int = 0

    def __post_init__(self) -> None:
        if self.fallback is None:
            # Fail "mostly closed": while the store is down each node allows only
            # its share of the quota, so the fleet total stays near the limit.
            share = max(1, self.limit // max(1, self.fleet_size_hint))
            self.fallback = LocalFixedWindow(self.clock, share, self.window)

    def allow(self, api_key: str) -> bool:
        try:
            allowed, _ = self.store.sliding_window_acquire(f"rl:{api_key}", self.limit, self.window)
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
