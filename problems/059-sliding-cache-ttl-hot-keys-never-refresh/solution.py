"""Stop hot cache entries from serving stale data forever under sliding expiration.

A product-catalogue cache reset each entry's 5-minute TTL on every read
("sliding expiration"), so that idle entries would be evicted but busy ones
would stay warm. A popular product is read every few seconds, so its entry
never went 5 minutes without a read and never expired: a price change in the
database stayed invisible on the product page for days. The fix keeps the
sliding idle timeout for eviction but adds an absolute maximum age measured
from when the value was loaded, which reads cannot extend.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

IDLE_TTL_SECONDS = 300.0      # evict an entry nobody has read for 5 minutes
MAX_AGE_SECONDS = 300.0       # never serve a value loaded more than 5 minutes ago

Loader = Callable[[str], object]


class FakeClock:
    """A clock the tests move by hand, so expiry is deterministic."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@dataclass
class Entry:
    value: object
    loaded_at: float        # when the value was read from the database
    last_read_at: float     # when a caller last got this value from the cache


class NaiveSlidingCache:
    """Original cache: every read pushes the expiry another TTL into the future."""

    def __init__(self, clock: FakeClock, ttl_seconds: float = IDLE_TTL_SECONDS) -> None:
        self.clock = clock
        self.ttl_seconds = ttl_seconds
        self.entries: dict[str, Entry] = {}
        self.loads = 0

    def get(self, key: str, loader: Loader) -> object:
        """Return the cached value, loading it on a miss or after expiry."""
        now = self.clock.time()
        entry = self.entries.get(key)
        if entry is not None and now - entry.last_read_at < self.ttl_seconds:
            # The bug: a read refreshes the only timer, so a key that is read
            # more often than once per TTL is never reloaded.
            entry.last_read_at = now
            return entry.value
        return self._load(key, loader, now)

    def _load(self, key: str, loader: Loader, now: float) -> object:
        self.loads += 1
        value = loader(key)
        self.entries[key] = Entry(value, loaded_at=now, last_read_at=now)
        return value


class BoundedAgeCache(NaiveSlidingCache):
    """The fix: an idle timeout for eviction plus a hard age limit for freshness."""

    def __init__(self, clock: FakeClock, idle_ttl_seconds: float = IDLE_TTL_SECONDS,
                 max_age_seconds: float = MAX_AGE_SECONDS) -> None:
        super().__init__(clock, idle_ttl_seconds)
        self.max_age_seconds = max_age_seconds

    def get(self, key: str, loader: Loader) -> object:
        """Serve from cache only while the entry is both recently read and young."""
        now = self.clock.time()
        entry = self.entries.get(key)
        if entry is not None and self._is_usable(entry, now):
            entry.last_read_at = now
            return entry.value
        return self._load(key, loader, now)

    def _is_usable(self, entry: Entry, now: float) -> bool:
        idle_for = now - entry.last_read_at
        age = now - entry.loaded_at
        # Age is measured from the load, which no read can move, so staleness
        # is bounded by max_age no matter how hot the key is.
        return idle_for < self.ttl_seconds and age < self.max_age_seconds


class PriceDatabase:
    """The source of truth; set_price simulates a merchandiser's change."""

    def __init__(self, prices: dict[str, int]) -> None:
        self.prices = dict(prices)

    def set_price(self, product: str, price: int) -> None:
        self.prices[product] = price

    def load(self, product: str) -> object:
        return self.prices[product]


def seconds_stale(cache: NaiveSlidingCache, database: PriceDatabase, clock: FakeClock,
                  product: str, new_price: int, read_every_seconds: float,
                  duration_seconds: float) -> float:
    """Change a price, keep reading it, and return how long the old price was served.

    Returns duration_seconds if the new price never appeared.
    """
    cache.get(product, database.load)          # warm the cache with the old price
    clock.advance(read_every_seconds)
    database.set_price(product, new_price)
    changed_at = clock.time()
    while clock.time() - changed_at <= duration_seconds:
        if cache.get(product, database.load) == new_price:
            return clock.time() - changed_at
        clock.advance(read_every_seconds)
    return duration_seconds
