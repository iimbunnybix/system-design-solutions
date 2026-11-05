"""Cache stampede (dog-pile) protection.

When a hot key expires, every concurrent request misses at the same moment
and all of them recompute it against the database. Three combined fixes:

1. **Single-flight**: per key, only one caller runs the loader; concurrent
   callers wait for its result (or get an error if it fails).
2. **Stale-while-revalidate**: after the soft TTL, serve the stale value
   immediately and refresh in the background (one refresher per key).
3. **Probabilistic early refresh (XFetch, Vattani et al., VLDB 2015)**:
   each read refreshes early with a probability that rises as expiry nears,
   so expirations of hot keys spread out instead of lining up.
"""
from __future__ import annotations

import math
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Hashable

Loader = Callable[[Hashable], Any]


@dataclass
class Entry:
    value: Any
    fresh_until: float     # soft TTL: after this, serve stale + refresh
    expires_at: float      # hard TTL: after this, must load synchronously
    compute_s: float       # how long the last load took (XFetch "delta")


class NaiveCache:
    """The original: check, miss, load, set. No coordination between callers."""

    def __init__(self, loader: Loader, ttl_s: float, clock: Callable[[], float] = time.monotonic):
        self.loader, self.ttl, self.clock = loader, ttl_s, clock
        self._data: dict[Hashable, tuple[Any, float]] = {}

    def get(self, key: Hashable) -> Any:
        hit = self._data.get(key)
        if hit and hit[1] > self.clock():
            return hit[0]
        value = self.loader(key)
        self._data[key] = (value, self.clock() + self.ttl)
        return value


class _Flight:
    def __init__(self):
        self.done = threading.Event()
        self.value: Any = None
        self.error: BaseException | None = None


class StampedeProtectedCache:
    def __init__(self, loader: Loader, ttl_s: float, stale_s: float = 0.0, beta: float = 1.0,
                 clock: Callable[[], float] = time.monotonic, rng: random.Random | None = None,
                 background: Callable[[Callable[[], None]], None] | None = None):
        """
        ttl_s:   soft TTL (fresh period)
        stale_s: extra period during which a stale value may be served while refreshing
        beta:    XFetch aggressiveness (>1 refreshes earlier, 0 disables)
        background: how to run refreshes; default is a daemon thread
        """
        self.loader, self.ttl, self.stale, self.beta = loader, ttl_s, stale_s, beta
        self.clock = clock
        self.rng = rng or random.Random()
        self.background = background or (lambda fn: threading.Thread(target=fn, daemon=True).start())
        self._data: dict[Hashable, Entry] = {}
        self._flights: dict[Hashable, _Flight] = {}
        self._lock = threading.Lock()

    # -- single flight -----------------------------------------------------------
    def _load(self, key: Hashable) -> Any:
        with self._lock:
            # Re-check under the lock: a flight may have finished between the
            # caller's lock-free read and now (otherwise we'd load twice).
            e = self._data.get(key)
            if e is not None and self.clock() < e.expires_at:
                return e.value
            flight = self._flights.get(key)
            leader = flight is None
            if leader:
                flight = self._flights[key] = _Flight()
        if not leader:
            flight.done.wait()
            if flight.error:
                raise flight.error
            return flight.value
        return self._run_flight(key, flight)

    def _run_flight(self, key: Hashable, flight: _Flight) -> Any:
        """Run the loader as the leader of an already-registered flight."""
        start = self.clock()
        try:
            value = self.loader(key)
            now = self.clock()
            with self._lock:
                self._data[key] = Entry(value, now + self.ttl, now + self.ttl + self.stale,
                                        max(now - start, 0.0))
            flight.value = value
            return value
        except BaseException as e:   # propagate to waiters, don't cache failures
            flight.error = e
            raise
        finally:
            with self._lock:
                self._flights.pop(key, None)
            flight.done.set()

    def _refresh_async(self, key: Hashable) -> None:
        # Claim the refresh when it is *scheduled*, not when it runs: with a
        # queueing executor, every stale read before the job starts would
        # otherwise schedule its own refresh and stampede the database again.
        with self._lock:
            if key in self._flights:
                return              # someone is already refreshing
            flight = self._flights[key] = _Flight()

        def run():
            try:
                self._run_flight(key, flight)
            except Exception:       # keep serving stale; next read will retry
                pass

        try:
            self.background(run)
        except BaseException:
            # The executor refused the job: release the claim so the key is
            # not stuck "refreshing" forever.
            with self._lock:
                self._flights.pop(key, None)
            flight.done.set()
            raise

    # -- public API ------------------------------------------------------------------
    def should_refresh_early(self, e: Entry, now: float) -> bool:
        """XFetch: refresh if now - delta*beta*ln(U) >= fresh_until, U~(0,1]."""
        if self.beta <= 0 or e.compute_s <= 0:
            return False
        u = 1.0 - self.rng.random()            # (0, 1]
        return now - e.compute_s * self.beta * math.log(u) >= e.fresh_until

    def get(self, key: Hashable) -> Any:
        now = self.clock()
        e = self._data.get(key)
        if e is None or now >= e.expires_at:
            return self._load(key)                       # cold or hard-expired: wait
        if now >= e.fresh_until or self.should_refresh_early(e, now):
            self._refresh_async(key)                     # stale or early: serve + refresh
        return e.value
