"""Keep the hot working set cached while a nightly job scans every product once.

The product cache was a plain LRU. A nightly analytics job reads millions of
product ids exactly once each through the same cache; every one of them
becomes "most recently used" and pushes out the products shoppers actually
look at, so the user hit rate collapsed every night and the database took the
full load.

The fix is a segmented LRU (the core idea of 2Q): a new key enters a small
probation segment and is promoted to the protected segment only when it is
hit a second time. Scan keys are never hit twice, so they churn through
probation and evict each other instead of the hot set.
"""
from __future__ import annotations

import random
from collections import OrderedDict
from dataclasses import dataclass
from typing import Hashable, Protocol

DEFAULT_PROTECTED_SHARE = 0.8   # share of capacity for keys seen at least twice
CACHE_CAPACITY = 10_000
CATALOG_SIZE = 1_000_000        # the nightly job reads every product once
HOT_PRODUCTS = 5_000            # products shoppers actually browse
ZIPF_EXPONENT = 0.6             # popularity skew among the hot products
DAY_REQUESTS = 50_000
SCAN_KEYS_PER_USER_REQUEST = 100   # the batch job reads far faster than shoppers
MORNING_REQUESTS = 5_000


class Cache(Protocol):
    """What the read path needs from a cache."""

    def get(self, key: Hashable) -> object | None: ...
    def put(self, key: Hashable, value: object) -> None: ...


class LRUCache:
    """The original cache: one LRU list; any access makes a key the newest."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self.capacity = capacity
        self.entries: OrderedDict[Hashable, object] = OrderedDict()

    def get(self, key: Hashable) -> object | None:
        """Return the cached value (marking it newest) or None."""
        if key not in self.entries:
            return None
        self.entries.move_to_end(key)
        return self.entries[key]

    def put(self, key: Hashable, value: object) -> None:
        """Insert or update a key, evicting the least recently used one."""
        self.entries[key] = value
        self.entries.move_to_end(key)
        if len(self.entries) > self.capacity:
            self.entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self.entries)


class SegmentedLRUCache:
    """Scan-resistant cache: probation segment for new keys, protected for reused ones."""

    def __init__(self, capacity: int, protected_share: float = DEFAULT_PROTECTED_SHARE) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        if not 0 <= protected_share < 1:
            raise ValueError("protected_share must be in [0, 1)")
        self.capacity = capacity
        self.protected_capacity = int(capacity * protected_share)
        self.probation: OrderedDict[Hashable, object] = OrderedDict()
        self.protected: OrderedDict[Hashable, object] = OrderedDict()

    def get(self, key: Hashable) -> object | None:
        """Return the cached value or None; a probation hit earns promotion."""
        if key in self.protected:
            self.protected.move_to_end(key)
            return self.protected[key]
        if key in self.probation:
            value = self.probation.pop(key)
            self._promote(key, value)
            return value
        return None

    def put(self, key: Hashable, value: object) -> None:
        """Insert a new key into probation, or update a key where it already is."""
        if key in self.protected:
            self.protected[key] = value
            self.protected.move_to_end(key)
            return
        self.probation[key] = value
        self.probation.move_to_end(key)
        self._evict_if_full()

    def _promote(self, key: Hashable, value: object) -> None:
        """Move a key hit for the second time into the protected segment."""
        if self.protected_capacity == 0:
            self.probation[key] = value     # no protected room: behave like LRU
            return
        self.protected[key] = value
        if len(self.protected) > self.protected_capacity:
            # Demote rather than drop: the oldest protected key gets one more
            # chance in probation before it can leave the cache.
            demoted_key, demoted_value = self.protected.popitem(last=False)
            self.probation[demoted_key] = demoted_value

    def _evict_if_full(self) -> None:
        """Evict from the old end of probation until the cache fits again."""
        while len(self) > self.capacity:
            self.probation.popitem(last=False)

    def __len__(self) -> int:
        return len(self.probation) + len(self.protected)


class FakeDatabase:
    """Counts the queries that cache misses send to the database."""

    def __init__(self) -> None:
        self.queries = 0

    def load_product(self, product_id: int) -> str:
        """Return a product row; every call is one database query."""
        self.queries += 1
        return f"product-{product_id}"


def read_through(cache: Cache, database: FakeDatabase, product_id: int) -> bool:
    """Serve one read; return True on a cache hit, else load and fill the cache."""
    if cache.get(product_id) is not None:
        return True
    cache.put(product_id, database.load_product(product_id))
    return False


@dataclass(frozen=True)
class NightReport:
    """User-traffic hit rate in each phase of one simulated night."""
    day_hit_rate: float
    scan_hit_rate: float
    morning_hit_rate: float
    scan_database_queries: int  # queries caused by user misses during the scan


class UserTraffic:
    """Seeded shopper reads: a skewed (Zipf-like) choice among the hot products."""

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.hot_ids = self.rng.sample(range(CATALOG_SIZE), HOT_PRODUCTS)
        weights = []
        for rank in range(1, HOT_PRODUCTS + 1):
            weights.append(1 / rank ** ZIPF_EXPONENT)
        self.cumulative = []
        total = 0.0
        for weight in weights:
            total += weight
            self.cumulative.append(total)

    def next_product(self) -> int:
        """Pick the next product a shopper opens."""
        return self.rng.choices(self.hot_ids, cum_weights=self.cumulative)[0]


def measure_user_hits(cache: Cache, database: FakeDatabase, traffic: UserTraffic,
                      requests: int) -> float:
    """Serve user requests and return their hit rate."""
    hits = 0
    for _ in range(requests):
        if read_through(cache, database, traffic.next_product()):
            hits += 1
    return hits / requests


def run_scan(cache: Cache, database: FakeDatabase, traffic: UserTraffic) -> tuple[float, int]:
    """Nightly job reads every product once, with user traffic in between.

    Returns the user hit rate during the scan and the database queries the
    user misses caused.
    """
    user_hits = 0
    user_requests = 0
    for product_id in range(CATALOG_SIZE):
        read_through(cache, database, product_id)
        if product_id % SCAN_KEYS_PER_USER_REQUEST == 0:
            user_requests += 1
            if read_through(cache, database, traffic.next_product()):
                user_hits += 1
    return user_hits / user_requests, user_requests - user_hits


def simulate_night(cache: Cache, seed: int = 1) -> NightReport:
    """Warm the cache with daytime traffic, run the scan, then measure the morning."""
    database = FakeDatabase()
    traffic = UserTraffic(seed)
    measure_user_hits(cache, database, traffic, DAY_REQUESTS)          # warm-up
    day = measure_user_hits(cache, database, traffic, DAY_REQUESTS)
    scan, scan_user_queries = run_scan(cache, database, traffic)
    morning = measure_user_hits(cache, database, traffic, MORNING_REQUESTS)
    return NightReport(day, scan, morning, scan_user_queries)
