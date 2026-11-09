import unittest

from solution import (AUDITS, STARTING_BALANCE, Ledger, Shard, naive_read_latest,
                      run_audits, snapshot_read)


def read_two_shards_with_transfer_between(read_shard) -> int:
    """Read shard 0, commit one shard-1 -> shard-0 transfer, then read shard 1."""
    ledger = Ledger(seed=0)
    snapshot_ts = ledger.clock
    first = read_shard(ledger, 0, snapshot_ts)
    ledger.transfer("s1-a0", "s0-a0", 300)
    second = read_shard(ledger, 1, snapshot_ts)
    return first + second


TWO_SHARD_TOTAL = 2 * 250 * STARTING_BALANCE


class ReproduceProblem(unittest.TestCase):
    def test_transfer_between_shard_reads_loses_the_money(self):
        # Shard 0 was read before it received 300; shard 1 after it lost 300.
        total = read_two_shards_with_transfer_between(naive_read_latest)
        self.assertEqual(total, TWO_SHARD_TOTAL - 300)

    def test_every_audit_under_live_traffic_is_wrong(self):
        report = run_audits(naive_read_latest)
        self.assertEqual(report.wrong_totals, AUDITS)
        self.assertEqual(report.worst_error, 8_687)


class FixSnapshotReads(unittest.TestCase):
    def test_transfer_between_shard_reads_is_not_seen_at_all(self):
        total = read_two_shards_with_transfer_between(snapshot_read)
        self.assertEqual(total, TWO_SHARD_TOTAL)

    def test_every_audit_under_live_traffic_is_exact(self):
        for seed in range(5):
            report = run_audits(snapshot_read, seed=seed)
            self.assertEqual(report.wrong_totals, 0, seed)

    def test_real_total_never_changes(self):
        ledger = Ledger(seed=3)
        ledger.random_transfers(1_000)
        total = sum(ledger.shard_of(a).latest(a) for a in ledger.accounts)
        self.assertEqual(total, ledger.expected_total)


class ShardVersions(unittest.TestCase):
    def setUp(self) -> None:
        self.shard = Shard()
        self.shard.write("a", 0, 100)
        self.shard.write("a", 5, 70)

    def test_as_of_returns_version_committed_at_or_before_snapshot(self):
        self.assertEqual(self.shard.as_of("a", 4), 100)
        self.assertEqual(self.shard.as_of("a", 5), 70)    # boundary is inclusive
        self.assertEqual(self.shard.as_of("a", 99), 70)

    def test_snapshot_before_account_existed_is_an_error(self):
        self.shard.write("b", 3, 10)
        with self.assertRaises(LookupError):
            self.shard.as_of("b", 2)

    def test_commit_timestamps_must_increase(self):
        with self.assertRaises(ValueError):
            self.shard.write("a", 5, 1)

    def test_transfer_cannot_overdraw_the_source(self):
        ledger = Ledger(seed=0)
        ledger.transfer("s0-a0", "s1-a0", STARTING_BALANCE + 999)
        self.assertEqual(ledger.shard_of("s0-a0").latest("s0-a0"), 0)
        self.assertEqual(ledger.shard_of("s1-a0").latest("s1-a0"), 2 * STARTING_BALANCE)


if __name__ == "__main__":
    unittest.main()
