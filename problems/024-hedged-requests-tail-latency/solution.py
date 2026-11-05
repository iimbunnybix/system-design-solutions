"""Hedged requests: cut fan-out tail latency without doubling backend load.

A search page fans every query out to 20 index shards and must wait for the
slowest one. Each shard call is usually ~10 ms, but about 2% of calls hit a
replica in a GC pause and take hundreds of milliseconds. With 20 calls per
page, a third of pages include at least one slow call, so p99 page latency
is set by the rare slow replica. The fix sends a backup ("hedge") call to
another replica when the first has not answered within the recent p95
latency, and caps hedges with a token-bucket budget so a fleet-wide slowdown
cannot turn into double the traffic.
"""
from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass

FAST_MIN_MS = 8.0
FAST_MAX_MS = 12.0
SLOW_MIN_MS = 200.0
SLOW_MAX_MS = 500.0
SLOW_PROBABILITY = 0.02          # share of calls that hit a paused replica
FANOUT = 20                      # shards a single page must wait for
HEDGE_PERCENTILE = 95.0          # hedge only calls slower than 95% of recent ones
LATENCY_WINDOW = 1000            # recent calls used to estimate that percentile
MIN_SAMPLES_FOR_DELAY = 100      # before this, use the configured initial delay
RECOMPUTE_EVERY = 50             # refresh the cached delay every N recorded calls
INITIAL_HEDGE_DELAY_MS = 50.0
HEDGE_BUDGET_FRACTION = 0.10     # extra calls allowed: 2x the ~5% steady-state need
HEDGE_BUDGET_BURST = 10.0        # hedges that may be spent at once


class ReplicaLatency:
    """Latency of one call to a randomly chosen replica (seeded, deterministic)."""

    def __init__(self, rng: random.Random, slow_probability: float = SLOW_PROBABILITY) -> None:
        self.rng = rng
        self.slow_probability = slow_probability

    def sample_ms(self) -> float:
        if self.rng.random() < self.slow_probability:
            return self.rng.uniform(SLOW_MIN_MS, SLOW_MAX_MS)
        return self.rng.uniform(FAST_MIN_MS, FAST_MAX_MS)


def percentile(values: list[float], percent: float) -> float:
    """Nearest-rank percentile: the smallest value >= percent% of the values."""
    if not values:
        raise ValueError("no values")
    ordered = sorted(values)
    rank = max(1, -(-len(ordered) * percent // 100))     # ceiling division
    return ordered[int(rank) - 1]


class HedgeDelay:
    """Tracks recent first-call latencies and returns the current hedge delay."""

    def __init__(self) -> None:
        self.recent: deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.recorded = 0
        self.cached_delay_ms = INITIAL_HEDGE_DELAY_MS

    def record(self, latency_ms: float) -> None:
        self.recent.append(latency_ms)
        self.recorded += 1
        # Sorting the window on every call is too expensive on a hot path,
        # so the percentile is recomputed periodically and cached.
        if self.recorded % RECOMPUTE_EVERY == 0:
            self.refresh()

    def refresh(self) -> None:
        # Too few samples gives a noisy percentile, so keep the initial value.
        if len(self.recent) >= MIN_SAMPLES_FOR_DELAY:
            self.cached_delay_ms = percentile(list(self.recent), HEDGE_PERCENTILE)

    def current_ms(self) -> float:
        return self.cached_delay_ms


class HedgeBudget:
    """Token bucket: each first call earns a fraction of a hedge token."""

    def __init__(self, fraction: float = HEDGE_BUDGET_FRACTION,
                 burst: float = HEDGE_BUDGET_BURST) -> None:
        self.fraction = fraction
        self.burst = burst
        self.tokens = burst

    def earn(self) -> None:
        self.tokens = min(self.burst, self.tokens + self.fraction)

    def try_spend(self) -> bool:
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class NaiveCaller:
    """The original client: one call per shard, wait however long it takes."""

    def __init__(self, latency: ReplicaLatency) -> None:
        self.latency = latency
        self.calls_sent = 0

    def call_shard(self) -> float:
        self.calls_sent += 1
        return self.latency.sample_ms()


class HedgedCaller:
    """Sends a backup call after the hedge delay, if the budget allows."""

    def __init__(self, latency: ReplicaLatency, budget: HedgeBudget | None = None) -> None:
        self.latency = latency
        self.budget = budget
        self.delay = HedgeDelay()
        self.calls_sent = 0
        self.hedges_sent = 0

    def call_shard(self) -> float:
        """Latency seen by the caller: whichever of the two calls answers first."""
        hedge_after_ms = self.delay.current_ms()
        first_ms = self.latency.sample_ms()
        self.calls_sent += 1
        self.delay.record(first_ms)
        if self.budget is not None:
            self.budget.earn()
        if first_ms <= hedge_after_ms or not self.may_hedge():
            return first_ms
        # The backup starts only once the delay has passed, so its answer
        # arrives at delay + its own latency; the caller takes the earlier one.
        self.calls_sent += 1
        self.hedges_sent += 1
        backup_ms = hedge_after_ms + self.latency.sample_ms()
        return min(first_ms, backup_ms)

    def may_hedge(self) -> bool:
        return self.budget is None or self.budget.try_spend()


@dataclass
class SimulationResult:
    p50_ms: float
    p99_ms: float
    calls_per_shard_request: float


def page_latency_ms(caller: NaiveCaller | HedgedCaller, fanout: int = FANOUT) -> float:
    """A page is done only when its slowest shard answers."""
    slowest = 0.0
    for _ in range(fanout):
        slowest = max(slowest, caller.call_shard())
    return slowest


def simulate(caller: NaiveCaller | HedgedCaller, pages: int, fanout: int = FANOUT) -> SimulationResult:
    """Serve ``pages`` page requests and summarise page latency and load."""
    latencies = []
    for _ in range(pages):
        latencies.append(page_latency_ms(caller, fanout))
    return SimulationResult(
        p50_ms=percentile(latencies, 50),
        p99_ms=percentile(latencies, 99),
        calls_per_shard_request=caller.calls_sent / (pages * fanout),
    )
