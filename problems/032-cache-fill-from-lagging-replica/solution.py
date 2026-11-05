"""A cache miss refilled from a lagging read replica pins the old value.

The incident: a product service uses cache-aside with a 10-minute TTL and
reads from a read replica on a miss. When a merchant changes a price, the
service updates the primary and deletes the cache key. With ~100 reads per
second on a popular product, the next read arrives within milliseconds,
misses, and reads the replica, which has not applied the update yet
(replication lag ~250 ms). It caches the old price, and every shopper sees
that old price until the TTL expires 10 minutes later.

The fix is a versioned fill. Every write returns its commit LSN (log
sequence number), and the invalidation leaves a short-lived marker holding
that LSN instead of just deleting the key. A fill must come from a source
that has applied at least that LSN: the replica if it has caught up,
otherwise the primary. The cache also refuses to overwrite an entry with an
older version, so no late fill can bring an old value back.
"""
from __future__ import annotations

from dataclasses import dataclass

CACHE_TTL_MS = 600_000            # 10 minutes
MARKER_TTL_MS = 30_000            # must be longer than the worst replication lag
REPLICATION_LAG_MS = 250
READ_INTERVAL_MS = 10             # ~100 reads per second on one hot product
UPDATE_AT_MS = 1_000
SIMULATION_MS = 700_000
OLD_PRICE = 4_999                 # cents
NEW_PRICE = 3_999


class FakeClock:
    """Deterministic time in milliseconds for tests and the simulation."""

    def __init__(self) -> None:
        self.now_ms = 0

    def advance(self, ms: int) -> None:
        """Move time forward by ``ms`` milliseconds."""
        self.now_ms += ms


@dataclass(frozen=True)
class WriteRecord:
    lsn: int
    commit_ms: int
    key: str
    value: int


class Database:
    """Primary plus one asynchronous replica that applies writes ``lag_ms`` late."""

    def __init__(self, clock: FakeClock, lag_ms: int) -> None:
        self.clock = clock
        self.lag_ms = lag_ms
        self.log: list[WriteRecord] = []
        self.primary_reads = 0
        self.replica_reads = 0

    def write(self, key: str, value: int) -> int:
        """Commit on the primary; returns the commit LSN."""
        lsn = len(self.log) + 1
        self.log.append(WriteRecord(lsn, self.clock.now_ms, key, value))
        return lsn

    def _state_as_of(self, cutoff_ms: int, key: str) -> tuple[int | None, int]:
        """(latest value of ``key``, highest applied LSN) for writes up to ``cutoff_ms``."""
        value = None
        applied_lsn = 0
        for record in self.log:
            if record.commit_ms > cutoff_ms:
                break
            applied_lsn = record.lsn
            if record.key == key:
                value = record.value
        return value, applied_lsn

    def read_primary(self, key: str) -> tuple[int | None, int]:
        """Always current; this is the load the replicas exist to absorb."""
        self.primary_reads += 1
        return self._state_as_of(self.clock.now_ms, key)

    def read_replica(self, key: str) -> tuple[int | None, int]:
        """Returns the replica's view and its applied LSN (pg_last_wal_replay_lsn)."""
        self.replica_reads += 1
        return self._state_as_of(self.clock.now_ms - self.lag_ms, key)


@dataclass
class CacheEntry:
    value: int | None
    version: int                  # LSN the value is known to be current as of
    expires_ms: int
    is_marker: bool = False       # "invalidated at LSN version", no value


class Cache:
    """Stand-in for Redis with per-key TTL and version-checked writes."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.entries: dict[str, CacheEntry] = {}

    def get(self, key: str) -> CacheEntry | None:
        """Live entry for ``key`` (value or marker), or None."""
        entry = self.entries.get(key)
        if entry is None or entry.expires_ms <= self.clock.now_ms:
            return None
        return entry

    def set(self, key: str, value: int | None, ttl_ms: int) -> None:
        """Unconditional SET ... EX, as the original code used."""
        self.entries[key] = CacheEntry(value, 0, self.clock.now_ms + ttl_ms)

    def delete(self, key: str) -> None:
        """DEL key: the original invalidation."""
        self.entries.pop(key, None)

    def set_if_newer(self, key: str, entry: CacheEntry) -> bool:
        """Store ``entry`` unless the live entry has a higher version (a Lua script in Redis)."""
        current = self.get(key)
        if current is not None and current.version > entry.version:
            return False
        self.entries[key] = entry
        return True


# ---------------------------------------------------------------- original
class NaiveProductService:
    """Cache-aside: delete on write, fill from the replica on a miss."""

    def __init__(self, db: Database, cache: Cache) -> None:
        self.db = db
        self.cache = cache

    def update_price(self, product: str, price: int) -> None:
        """Write to the primary, then delete the cached copy."""
        self.db.write(product, price)
        self.cache.delete(product)

    def get_price(self, product: str) -> int | None:
        """Serve from cache; on a miss read the replica and cache whatever it says."""
        entry = self.cache.get(product)
        if entry is not None:
            return entry.value
        value, _ = self.db.read_replica(product)     # may not have the write yet
        self.cache.set(product, value, CACHE_TTL_MS)
        return value


# --------------------------------------------------------------------- fix
class VersionedProductService:
    """Cache-aside where an invalidation records the LSN every fill must reach."""

    def __init__(self, db: Database, cache: Cache, marker_ttl_ms: int = MARKER_TTL_MS) -> None:
        self.db = db
        self.cache = cache
        self.marker_ttl_ms = marker_ttl_ms

    def update_price(self, product: str, price: int) -> None:
        """Write, then replace the cached value with an "invalidated at LSN" marker."""
        lsn = self.db.write(product, price)
        expires = self.db.clock.now_ms + self.marker_ttl_ms
        self.cache.set_if_newer(product, CacheEntry(None, lsn, expires, is_marker=True))

    def get_price(self, product: str) -> int | None:
        """Serve a cached value; on a miss or marker, fill from a source new enough."""
        entry = self.cache.get(product)
        if entry is not None and not entry.is_marker:
            return entry.value
        required_lsn = entry.version if entry is not None else 0
        value, as_of_lsn = self._read_at_least(product, required_lsn)
        expires = self.db.clock.now_ms + CACHE_TTL_MS
        self.cache.set_if_newer(product, CacheEntry(value, as_of_lsn, expires))
        return value

    def _read_at_least(self, product: str, required_lsn: int) -> tuple[int | None, int]:
        """Read from the replica if it has applied ``required_lsn``, else from the primary.

        Most misses have no marker (required_lsn 0) or come after the replica
        caught up, so the primary only sees reads inside the lag window.
        """
        value, applied_lsn = self.db.read_replica(product)
        if applied_lsn >= required_lsn:
            return value, applied_lsn
        return self.db.read_primary(product)


# ---------------------------------------------------------------- workload
def simulate(service_class: type, lag_ms: int = REPLICATION_LAG_MS) -> dict[str, int]:
    """One hot product read every 10 ms; its price changes at t = 1 s.

    Returns how many reads after the update showed the old price, when the
    last one happened, and the database reads by kind.
    """
    clock = FakeClock()
    db = Database(clock, lag_ms)
    service = service_class(db, Cache(clock))
    db.write("sku-1", OLD_PRICE)
    clock.advance(lag_ms + READ_INTERVAL_MS)        # replica has the initial row
    stale_reads = 0
    last_stale_ms = -1
    updated = False
    while clock.now_ms < SIMULATION_MS:
        if not updated and clock.now_ms >= UPDATE_AT_MS:
            service.update_price("sku-1", NEW_PRICE)
            updated = True
        price = service.get_price("sku-1")
        if updated and price != NEW_PRICE:
            stale_reads += 1
            last_stale_ms = clock.now_ms
        clock.advance(READ_INTERVAL_MS)
    return {"stale_reads": stale_reads, "last_stale_ms": last_stale_ms,
            "primary_reads": db.primary_reads, "replica_reads": db.replica_reads}
