"""Coordination-free unique IDs (Snowflake layout) + Base62 short codes."""
from __future__ import annotations

import math
import threading
import time
from typing import Callable

ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
EPOCH_MS = 1_767_225_600_000          # 2026-01-01T00:00:00Z
WORKER_BITS, SEQ_BITS = 10, 12
MAX_WORKER, MAX_SEQ = (1 << WORKER_BITS) - 1, (1 << SEQ_BITS) - 1


class ClockMovedBackwards(Exception):
    pass


class ClockBeforeEpoch(Exception):
    """The host clock reads earlier than EPOCH_MS (e.g. booted at 1970 before NTP sync)."""


def base62_encode(n: int) -> str:
    if n < 0:
        raise ValueError("negative")
    if n == 0:
        return ALPHABET[0]
    out = []
    while n:
        n, r = divmod(n, 62)
        out.append(ALPHABET[r])
    return "".join(reversed(out))


def base62_decode(s: str) -> int:
    n = 0
    for ch in s:
        idx = ALPHABET.find(ch)
        if idx < 0:
            raise ValueError(f"invalid character {ch!r}")
        n = n * 62 + idx
    return n


class SnowflakeGenerator:
    def __init__(self, worker_id: int, clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
                 max_backwards_wait_ms: int = 5,
                 monotonic_ms: Callable[[], int] = lambda: time.monotonic_ns() // 1_000_000):
        if not 0 <= worker_id <= MAX_WORKER:
            raise ValueError(f"worker_id must be 0..{MAX_WORKER}")
        self.worker_id = worker_id
        self._clock = clock_ms
        self._max_wait = max_backwards_wait_ms
        # The wall clock is the thing that went wrong, so time the wait with a monotonic clock.
        self._monotonic = monotonic_ms
        self._last_ms = -1
        self._seq = 0
        self._lock = threading.Lock()

    def _wait_until_after(self, ms: int, deadline_ms: int | None = None) -> int:
        """Spin until the wall clock passes `ms`, or until the monotonic deadline expires.

        Returns the last reading; the caller checks whether it actually got past `ms`.
        """
        now = self._clock()
        while now <= ms:
            if deadline_ms is not None and self._monotonic() >= deadline_ms:
                break
            now = self._clock()
        return now

    def next_id(self) -> int:
        with self._lock:
            now = self._clock()
            # A clock before our epoch would produce negative IDs that sort before
            # every real one, so fail loudly instead of minting them.
            if now < EPOCH_MS:
                raise ClockBeforeEpoch(f"clock {now} ms is before epoch {EPOCH_MS} ms")
            if now < self._last_ms:
                if self._last_ms - now > self._max_wait:
                    raise ClockMovedBackwards(f"clock moved back {self._last_ms - now} ms")
                # Bound the wait: a frozen clock must not block every caller forever under the lock.
                deadline_ms = self._monotonic() + self._max_wait
                now = self._wait_until_after(self._last_ms - 1, deadline_ms)
                if now < self._last_ms:
                    raise ClockMovedBackwards("clock did not recover")
            if now == self._last_ms:
                self._seq = (self._seq + 1) & MAX_SEQ
                if self._seq == 0:                       # 4096 ids used this ms
                    now = self._wait_until_after(self._last_ms)
            else:
                self._seq = 0
            self._last_ms = now
            return ((now - EPOCH_MS) << (WORKER_BITS + SEQ_BITS)) | (self.worker_id << SEQ_BITS) | self._seq

    def next_code(self) -> str:
        return base62_encode(self.next_id())


def decompose(id_: int) -> dict:
    return {"timestamp_ms": (id_ >> (WORKER_BITS + SEQ_BITS)) + EPOCH_MS,
            "worker_id": (id_ >> SEQ_BITS) & MAX_WORKER,
            "sequence": id_ & MAX_SEQ}


def random_code_collision_probability(existing: int, length: int = 6) -> float:
    """Chance that ANY collision has happened after `existing` random codes (birthday bound)."""
    space = 62 ** length
    return 1 - math.exp(-existing * (existing - 1) / (2 * space))
