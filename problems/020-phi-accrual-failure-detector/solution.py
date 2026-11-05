"""Detect dead nodes without failing over every time a healthy node pauses.

The cluster declared a node dead when no heartbeat arrived for a fixed 3
seconds. Nodes whose heartbeats were sometimes late (garbage-collection pauses,
a noisy network) crossed that line many times a day and triggered needless
failovers. This module keeps that detector and adds a phi accrual detector: it
learns each node's own heartbeat rhythm and reports how unlikely the current
silence is, so a node that is often a bit late gets more slack and a
metronome-regular node is still caught quickly.
"""
from __future__ import annotations

import math
from collections import deque

FIXED_TIMEOUT_SECONDS = 3.0
PHI_THRESHOLD = 8.0                 # suspect when silence is a 1-in-10^8 event
HISTORY_SIZE = 100                  # inter-arrival samples remembered per node
MIN_SAMPLES = 3                     # below this, history is too thin to trust
MIN_STD_DEV_SECONDS = 0.5           # floor so a perfectly steady node isn't hair-trigger
SMALLEST_PROBABILITY = 1e-300       # keeps log10 finite far in the tail


class FixedTimeoutDetector:
    """Original detector: dead if silent for longer than one global timeout."""

    def __init__(self, timeout_seconds: float = FIXED_TIMEOUT_SECONDS) -> None:
        self.timeout_seconds = timeout_seconds
        self.last_heartbeat: float | None = None

    def heartbeat(self, now: float) -> None:
        self.last_heartbeat = now

    def is_suspected(self, now: float) -> bool:
        if self.last_heartbeat is None:
            return False
        return now - self.last_heartbeat > self.timeout_seconds


def probability_of_longer_gap(elapsed: float, mean: float, std_dev: float) -> float:
    """P(next heartbeat arrives later than `elapsed`), assuming normal inter-arrival times."""
    z_score = (elapsed - mean) / (std_dev * math.sqrt(2.0))
    return 0.5 * math.erfc(z_score)


class PhiAccrualDetector:
    """Phi accrual failure detector (Hayashibara et al., 2004), as used by Cassandra and Akka.

    phi = -log10(P(a gap at least this long)). phi 1 means a 10% chance the node is
    merely late, phi 8 means a 0.000001% chance.
    """

    def __init__(self, threshold: float = PHI_THRESHOLD,
                 bootstrap_timeout_seconds: float = FIXED_TIMEOUT_SECONDS) -> None:
        self.threshold = threshold
        self.bootstrap_timeout_seconds = bootstrap_timeout_seconds
        self.intervals: deque[float] = deque(maxlen=HISTORY_SIZE)
        self.last_heartbeat: float | None = None

    def heartbeat(self, now: float) -> None:
        """Record a heartbeat and the gap since the previous one."""
        if self.last_heartbeat is not None:
            self.intervals.append(now - self.last_heartbeat)
        self.last_heartbeat = now

    def _mean_and_std_dev(self) -> tuple[float, float]:
        """Mean and (floored) standard deviation of the remembered gaps."""
        mean = sum(self.intervals) / len(self.intervals)
        variance = sum((gap - mean) ** 2 for gap in self.intervals) / len(self.intervals)
        return mean, max(math.sqrt(variance), MIN_STD_DEV_SECONDS)

    def phi(self, now: float) -> float:
        """Suspicion level for a node last heard from at `last_heartbeat`."""
        if self.last_heartbeat is None or len(self.intervals) < MIN_SAMPLES:
            return 0.0
        mean, std_dev = self._mean_and_std_dev()
        elapsed = now - self.last_heartbeat
        probability = probability_of_longer_gap(elapsed, mean, std_dev)
        return -math.log10(max(probability, SMALLEST_PROBABILITY))

    def is_suspected(self, now: float) -> bool:
        if self.last_heartbeat is None:
            return False
        # Until we have learned the node's rhythm, fall back to the old timeout
        # rather than guessing from one or two samples.
        if len(self.intervals) < MIN_SAMPLES:
            return now - self.last_heartbeat > self.bootstrap_timeout_seconds
        return self.phi(now) >= self.threshold
