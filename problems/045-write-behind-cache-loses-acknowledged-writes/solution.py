"""Write-behind cache that does not lose acknowledged writes on a crash.

A cart service put a write-behind cache in front of its database: writes were
stored in memory, acknowledged at once, and flushed to the database in one
batch every few seconds. When the cache process crashed, every write made
since the last flush was gone even though clients had been told "saved".

The fix appends each write to a durable journal (an append-only file that is
fsynced) before acknowledging it. On restart the journal is replayed into the
database, and after each successful flush the journal is truncated so it
stays small.
"""
from __future__ import annotations

FLUSH_INTERVAL_S = 5.0
UPDATE_EVERY = 10


class FakeDisk:
    """A file on disk with page-cache semantics.

    ``append`` only reaches the page cache; ``fsync`` makes it durable. A
    crash keeps durable records and drops everything still in the page cache.
    """

    def __init__(self) -> None:
        self.durable: list[tuple[str, str]] = []
        self.page_cache: list[tuple[str, str]] = []
        self.fsync_count = 0

    def append(self, record: tuple[str, str]) -> None:
        """Write a record into the OS page cache (not yet safe)."""
        self.page_cache.append(record)

    def fsync(self) -> None:
        """Force buffered records to stable storage."""
        self.durable.extend(self.page_cache)
        self.page_cache = []
        self.fsync_count += 1

    def truncate(self) -> None:
        """Empty the file (durably) once its records are in the database."""
        self.durable = []
        self.page_cache = []

    def crash(self) -> None:
        """Power loss / process kill: unsynced data disappears."""
        self.page_cache = []


class Database:
    """The system of record. Survives cache crashes."""

    def __init__(self) -> None:
        self.rows: dict[str, str] = {}

    def write_batch(self, writes: dict[str, str]) -> None:
        """Apply a batch of key -> value writes."""
        self.rows.update(writes)


class NaiveWriteBehindCache:
    """Original design: ack once the write is in memory."""

    def __init__(self, database: Database) -> None:
        self.database = database
        self.dirty: dict[str, str] = {}
        self.last_flush_at = 0.0

    def put(self, key: str, value: str, now: float) -> bool:
        """Store in memory and acknowledge immediately."""
        self.dirty[key] = value
        self.maybe_flush(now)
        return True

    def maybe_flush(self, now: float) -> None:
        """Flush dirty keys to the database every FLUSH_INTERVAL_S seconds."""
        if now - self.last_flush_at < FLUSH_INTERVAL_S:
            return
        self.database.write_batch(self.dirty)
        self.dirty = {}
        self.last_flush_at = now

    def crash(self) -> None:
        """The process dies: memory is lost."""
        self.dirty = {}


class JournaledWriteBehindCache:
    """Fixed design: journal + fsync before ack, replay on restart."""

    def __init__(self, database: Database, journal: FakeDisk) -> None:
        self.database = database
        self.journal = journal
        self.dirty: dict[str, str] = {}
        self.last_flush_at = 0.0

    def put(self, key: str, value: str, now: float) -> bool:
        """Make the write durable in the journal, then acknowledge it."""
        self.journal.append((key, value))
        # Without fsync the record sits in the page cache and a crash still
        # loses it, so the ack would be the same lie as before.
        self.journal.fsync()
        self.dirty[key] = value
        self.maybe_flush(now)
        return True

    def maybe_flush(self, now: float) -> None:
        """Flush to the database and only then drop the journal entries."""
        if now - self.last_flush_at < FLUSH_INTERVAL_S:
            return
        self.database.write_batch(self.dirty)
        # Truncate after the database write succeeded: truncating first would
        # open a window where a crash loses the batch.
        self.journal.truncate()
        self.dirty = {}
        self.last_flush_at = now

    def crash(self) -> None:
        """The process dies: memory and unsynced journal data are lost."""
        self.dirty = {}
        self.journal.crash()

    def recover(self, now: float) -> int:
        """Replay the journal into the database after a restart.

        Records are applied in journal order so the last write to a key wins,
        exactly as it did before the crash. Returns the number of records
        replayed.
        """
        latest_values: dict[str, str] = {}
        record_count = len(self.journal.durable)
        for key, value in self.journal.durable:
            latest_values[key] = value
        self.database.write_batch(latest_values)
        self.journal.truncate()
        self.last_flush_at = now
        return record_count


def run_until_crash(cache: NaiveWriteBehindCache | JournaledWriteBehindCache,
                    seconds_between_writes: float, crash_after: int) -> dict[str, str]:
    """Send writes on a fake clock, then crash after ``crash_after`` of them.

    Returns the writes that were acknowledged before the crash.
    """
    acknowledged: dict[str, str] = {}
    for index in range(crash_after):
        # Multiply instead of summing so the clock has no float drift.
        now = (index + 1) * seconds_between_writes
        # Every tenth write updates an earlier cart, so last-write-wins matters.
        if index % UPDATE_EVERY == UPDATE_EVERY - 1:
            key = f"cart:{index + 1 - UPDATE_EVERY}"
        else:
            key = f"cart:{index}"
        value = f"v{index}"
        if cache.put(key, value, now):
            acknowledged[key] = value
    cache.crash()
    return acknowledged


def lost_writes(acknowledged: dict[str, str], database: Database) -> list[str]:
    """Keys whose acknowledged value is not what the database holds."""
    lost = []
    for key, value in acknowledged.items():
        if database.rows.get(key) != value:
            lost.append(key)
    return lost
