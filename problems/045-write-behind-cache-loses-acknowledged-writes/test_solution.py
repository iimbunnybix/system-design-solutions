import unittest

from solution import (FLUSH_INTERVAL_S, Database, FakeDisk, JournaledWriteBehindCache,
                      NaiveWriteBehindCache, lost_writes, run_until_crash)

SECONDS_BETWEEN_WRITES = 0.01
# 730 writes = 7.3 s: one flush at 5.0 s, then 2.3 s of unflushed writes.
CRASH_AFTER = 730
RESTART_AT = 60.0


def journaled_cache() -> tuple[JournaledWriteBehindCache, Database, FakeDisk]:
    """A fixed cache with its own database and journal file."""
    database = Database()
    journal = FakeDisk()
    return JournaledWriteBehindCache(database, journal), database, journal


class ReproduceProblem(unittest.TestCase):
    def test_crash_loses_writes_acknowledged_since_last_flush(self):
        database = Database()
        acknowledged = run_until_crash(NaiveWriteBehindCache(database),
                                       SECONDS_BETWEEN_WRITES, CRASH_AFTER)
        self.assertEqual(len(lost_writes(acknowledged, database)), 207)

    def test_crash_before_first_flush_loses_everything(self):
        database = Database()
        acknowledged = run_until_crash(NaiveWriteBehindCache(database),
                                       SECONDS_BETWEEN_WRITES, 100)
        self.assertEqual(database.rows, {})
        self.assertEqual(len(lost_writes(acknowledged, database)), len(acknowledged))


class FixWithDurableJournal(unittest.TestCase):
    def test_recovery_restores_every_acknowledged_write(self):
        cache, database, _ = journaled_cache()
        acknowledged = run_until_crash(cache, SECONDS_BETWEEN_WRITES, CRASH_AFTER)
        cache.recover(RESTART_AT)
        self.assertEqual(lost_writes(acknowledged, database), [])

    def test_only_writes_since_last_flush_are_replayed(self):
        cache, _, _ = journaled_cache()
        run_until_crash(cache, SECONDS_BETWEEN_WRITES, CRASH_AFTER)
        self.assertEqual(cache.recover(RESTART_AT), 230)

    def test_every_write_is_fsynced_before_ack(self):
        cache, _, journal = journaled_cache()
        run_until_crash(cache, SECONDS_BETWEEN_WRITES, CRASH_AFTER)
        self.assertEqual(journal.fsync_count, CRASH_AFTER)

    def test_last_write_to_a_key_wins_after_replay(self):
        cache, database, _ = journaled_cache()
        cache.put("cart:1", "old", now=1.0)
        cache.put("cart:1", "new", now=2.0)
        cache.crash()
        cache.recover(RESTART_AT)
        self.assertEqual(database.rows["cart:1"], "new")

    def test_journal_is_truncated_after_a_flush(self):
        cache, database, journal = journaled_cache()
        cache.put("cart:1", "a", now=1.0)
        cache.put("cart:2", "b", now=FLUSH_INTERVAL_S)
        self.assertEqual(journal.durable, [])
        self.assertEqual(database.rows, {"cart:1": "a", "cart:2": "b"})

    def test_recovering_an_empty_journal_changes_nothing(self):
        cache, database, _ = journaled_cache()
        self.assertEqual(cache.recover(RESTART_AT), 0)
        self.assertEqual(database.rows, {})

    def test_second_crash_after_recovery_loses_nothing(self):
        cache, database, _ = journaled_cache()
        cache.put("cart:1", "a", now=1.0)
        cache.crash()
        cache.recover(RESTART_AT)
        cache.crash()
        cache.recover(RESTART_AT + 1)
        self.assertEqual(database.rows, {"cart:1": "a"})


class FakeDiskSemantics(unittest.TestCase):
    def test_unsynced_append_is_lost_on_crash(self):
        disk = FakeDisk()
        disk.append(("cart:1", "a"))
        disk.crash()
        self.assertEqual(disk.durable, [])

    def test_synced_append_survives_crash(self):
        disk = FakeDisk()
        disk.append(("cart:1", "a"))
        disk.fsync()
        disk.crash()
        self.assertEqual(disk.durable, [("cart:1", "a")])


if __name__ == "__main__":
    unittest.main()
