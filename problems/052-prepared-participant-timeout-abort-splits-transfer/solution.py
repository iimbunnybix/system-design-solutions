"""Two-phase commit where a coordinator crash can no longer split a transfer in half.

Cross-shard money transfers use two-phase commit: both ledger shards prepare
(lock the account and promise they can apply the change), then the coordinator
tells each to commit. In the original version a prepared shard that heard
nothing for a while aborted on its own to free the lock, and the coordinator
never recorded its decision. When the coordinator crashed after telling one
shard "commit", the other shard timed out and aborted: money left one account
and never arrived. This version writes the decision durably before sending it,
and a prepared shard never decides alone: it waits (in doubt) until it can read
the decision, and a recovering coordinator re-sends it.
"""
from __future__ import annotations

COMMIT = "commit"
ABORT = "abort"
PREPARED = "prepared"      # promised to commit if told to; must not decide alone
COMMITTED = "committed"
ABORTED = "aborted"


class CoordinatorCrashed(Exception):
    """Simulates the coordinator process dying at a chosen point."""


class DecisionLog:
    """The coordinator's durable record of commit/abort decisions (a stand-in for an fsynced file)."""

    def __init__(self) -> None:
        self.decisions: dict[str, str] = {}

    def record(self, transaction_id: str, decision: str) -> None:
        self.decisions[transaction_id] = decision

    def lookup(self, transaction_id: str) -> str | None:
        return self.decisions.get(transaction_id)


class LedgerShard:
    """One participant: a shard holding one account balance."""

    def __init__(self, name: str, balance: int) -> None:
        self.name = name
        self.balance = balance
        self.states: dict[str, str] = {}
        self.pending_changes: dict[str, int] = {}
        # A prepared transaction keeps the account locked until it is resolved.
        self.locked_by: str | None = None

    def prepare(self, transaction_id: str, change: int) -> bool:
        """Phase 1: vote yes only if the change can be applied, and lock the account."""
        if self.locked_by is not None or self.balance + change < 0:
            self.states[transaction_id] = ABORTED
            return False
        self.locked_by = transaction_id
        self.pending_changes[transaction_id] = change
        self.states[transaction_id] = PREPARED
        return True

    def finish(self, transaction_id: str, decision: str) -> None:
        """Phase 2: apply or drop the change. Repeats are ignored, so re-sending is safe."""
        if self.states.get(transaction_id) != PREPARED:
            return
        change = self.pending_changes.pop(transaction_id)
        if decision == COMMIT:
            self.balance += change
            self.states[transaction_id] = COMMITTED
        else:
            self.states[transaction_id] = ABORTED
        self.locked_by = None

    def naive_on_timeout(self, transaction_id: str) -> None:
        """Original timeout handler: free the lock by aborting, even after voting yes."""
        if self.states.get(transaction_id) == PREPARED:
            self.finish(transaction_id, ABORT)

    def on_timeout(self, transaction_id: str, log: DecisionLog, coordinator_reachable: bool) -> str:
        """Fixed timeout handler: a prepared shard asks for the decision instead of guessing.

        Once it voted yes, the coordinator may already have told another shard
        to commit, so aborting alone could split the transfer. If the decision
        cannot be read yet, the shard stays prepared (blocked) and retries later.
        """
        if self.states.get(transaction_id) == PREPARED and coordinator_reachable:
            decision = log.lookup(transaction_id)
            if decision is not None:
                self.finish(transaction_id, decision)
        return self.states[transaction_id]


def collect_votes(transaction_id: str, changes: list[tuple[LedgerShard, int]]) -> str:
    """Phase 1 on every shard; commit only if every shard voted yes."""
    decision = COMMIT
    for shard, change in changes:
        if not shard.prepare(transaction_id, change):
            decision = ABORT
    return decision


def run_transaction(transaction_id: str, changes: list[tuple[LedgerShard, int]],
                    log: DecisionLog | None, crash_after_messages: int | None = None) -> str:
    """Run two-phase commit; `log=None` is the original, unlogged coordinator.

    `crash_after_messages` makes the coordinator die after sending the
    decision to that many shards, the moment that split real transfers.
    """
    decision = collect_votes(transaction_id, changes)
    if log is not None:
        # The fix: the decision is durable before any shard can act on it.
        log.record(transaction_id, decision)
    for index, (shard, _change) in enumerate(changes):
        if crash_after_messages is not None and index == crash_after_messages:
            raise CoordinatorCrashed(f"coordinator died before telling {shard.name}")
        shard.finish(transaction_id, decision)
    return decision


def recover_coordinator(transaction_id: str, shards: list[LedgerShard], log: DecisionLog) -> str:
    """After a restart, finish a transaction from the log.

    No logged decision means the crash happened before deciding, so no shard
    can have been told to commit and aborting ("presumed abort") is safe.
    """
    decision = log.lookup(transaction_id)
    if decision is None:
        decision = ABORT
        log.record(transaction_id, decision)
    for shard in shards:
        shard.finish(transaction_id, decision)
    return decision
