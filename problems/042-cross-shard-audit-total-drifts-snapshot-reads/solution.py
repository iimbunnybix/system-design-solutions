"""Sum balances across shards without seeing half of a transfer.

Wallet balances are sharded by account. A transfer between two shards commits
both sides at the same commit timestamp, so the total amount of money never
changes. The hourly audit, however, read each shard's *latest* balances one
shard after another while transfers kept committing. A transfer that landed
between two shard reads was counted on one side only, so the audit total was
randomly too high or too low and raised false "money created / lost" alarms.

The fix is a snapshot read: every shard keeps a few old versions of each
balance (multi-version concurrency control), the audit picks one timestamp
when it starts, and reads every shard "as of" that timestamp. Each transfer
is either fully before it or fully after it.
"""
from __future__ import annotations

import bisect
import random
from dataclasses import dataclass, field
from typing import Callable

SHARD_COUNT = 4
ACCOUNTS_PER_SHARD = 250
STARTING_BALANCE = 1_000
MAX_TRANSFER = 500
TICKS_PER_SHARD_READ = 50     # transfers that commit while one shard is read
AUDITS = 200


@dataclass
class Shard:
    """One shard: every balance keeps its history as (commit_ts, balance)."""
    history: dict[str, list[tuple[int, int]]] = field(default_factory=dict)

    def write(self, account: str, commit_ts: int, balance: int) -> None:
        """Append a new version; commit timestamps only move forward."""
        versions = self.history.setdefault(account, [])
        if versions and commit_ts <= versions[-1][0]:
            raise ValueError("commit timestamps must increase per account")
        versions.append((commit_ts, balance))

    def latest(self, account: str) -> int:
        """The newest committed balance (what the original audit read)."""
        return self.history[account][-1][1]

    def as_of(self, account: str, snapshot_ts: int) -> int:
        """The balance committed at or before ``snapshot_ts``."""
        versions = self.history[account]
        # Find the last version whose commit_ts <= snapshot_ts.
        position = bisect.bisect_right(versions, (snapshot_ts, float("inf")))
        if position == 0:
            raise LookupError(f"{account} did not exist at ts {snapshot_ts}")
        return versions[position - 1][1]


class Ledger:
    """Accounts spread over shards; cross-shard transfers commit atomically."""

    def __init__(self, seed: int = 0) -> None:
        self.shards = [Shard() for _ in range(SHARD_COUNT)]
        self.rng = random.Random(seed)
        self.clock = 0
        self.accounts: list[str] = []
        for shard_id in range(SHARD_COUNT):
            for n in range(ACCOUNTS_PER_SHARD):
                account = f"s{shard_id}-a{n}"
                self.accounts.append(account)
                self.shards[shard_id].write(account, self.clock, STARTING_BALANCE)
        self.expected_total = len(self.accounts) * STARTING_BALANCE

    def shard_of(self, account: str) -> Shard:
        """Accounts are named after their shard, e.g. 's2-a17'."""
        return self.shards[int(account[1:account.index("-")])]

    def transfer(self, source: str, target: str, amount: int) -> None:
        """Move money; both sides get the same commit timestamp (as 2PC gives)."""
        self.clock += 1
        source_shard, target_shard = self.shard_of(source), self.shard_of(target)
        amount = min(amount, source_shard.latest(source))   # no overdrafts
        source_shard.write(source, self.clock, source_shard.latest(source) - amount)
        target_shard.write(target, self.clock, target_shard.latest(target) + amount)

    def random_transfers(self, count: int) -> None:
        """Background traffic between random accounts, usually on two shards."""
        for _ in range(count):
            source, target = self.rng.sample(self.accounts, 2)
            self.transfer(source, target, self.rng.randint(1, MAX_TRANSFER))


ShardReader = Callable[[Ledger, int, int], int]


def naive_read_latest(ledger: Ledger, shard_id: int, snapshot_ts: int) -> int:
    """Original audit: sum the shard's newest balances, ignoring the snapshot."""
    shard = ledger.shards[shard_id]
    return sum(shard.latest(account) for account in shard.history)


def snapshot_read(ledger: Ledger, shard_id: int, snapshot_ts: int) -> int:
    """Fixed audit: sum the shard's balances as of the audit's start timestamp."""
    shard = ledger.shards[shard_id]
    return sum(shard.as_of(account, snapshot_ts) for account in shard.history)


def audit_total(ledger: Ledger, read_shard: ShardReader) -> int:
    """Read the shards one after another while transfers keep committing."""
    snapshot_ts = ledger.clock     # chosen once, when the audit starts
    total = 0
    for shard_id in range(SHARD_COUNT):
        total += read_shard(ledger, shard_id, snapshot_ts)
        ledger.random_transfers(TICKS_PER_SHARD_READ)
    return total


@dataclass
class AuditReport:
    audits: int
    wrong_totals: int
    worst_error: int     # largest |audit total - real total|


def run_audits(read_shard: ShardReader, audits: int = AUDITS, seed: int = 0) -> AuditReport:
    """Run many audits back to back on one ledger with live traffic."""
    ledger = Ledger(seed)
    wrong = 0
    worst = 0
    for _ in range(audits):
        error = abs(audit_total(ledger, read_shard) - ledger.expected_total)
        if error:
            wrong += 1
        worst = max(worst, error)
    return AuditReport(audits, wrong, worst)
