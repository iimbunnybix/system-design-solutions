import unittest

from solution import (ABORT, ABORTED, COMMIT, COMMITTED, PREPARED, CoordinatorCrashed,
                      DecisionLog, LedgerShard, recover_coordinator, run_transaction)

STARTING_BALANCE = 1000
TRANSFER_AMOUNT = 500
TRANSFER_ID = "transfer-1"


def two_shards() -> tuple[LedgerShard, LedgerShard]:
    return LedgerShard("ledger-a", STARTING_BALANCE), LedgerShard("ledger-b", STARTING_BALANCE)


def transfer(sender: LedgerShard, receiver: LedgerShard) -> list[tuple[LedgerShard, int]]:
    return [(sender, -TRANSFER_AMOUNT), (receiver, TRANSFER_AMOUNT)]


def total_money(*shards: LedgerShard) -> int:
    return sum(shard.balance for shard in shards)


def crash_after_first_commit(sender, receiver, log) -> None:
    """Coordinator tells ledger-a "commit" and dies before reaching ledger-b."""
    try:
        run_transaction(TRANSFER_ID, transfer(sender, receiver), log, crash_after_messages=1)
    except CoordinatorCrashed:
        pass


class ReproduceProblem(unittest.TestCase):
    def test_timeout_abort_after_partial_commit_loses_money(self):
        sender, receiver = two_shards()
        crash_after_first_commit(sender, receiver, log=None)
        receiver.naive_on_timeout(TRANSFER_ID)
        self.assertEqual(sender.states[TRANSFER_ID], COMMITTED)
        self.assertEqual(receiver.states[TRANSFER_ID], ABORTED)
        self.assertEqual(total_money(sender, receiver), 2 * STARTING_BALANCE - TRANSFER_AMOUNT)


class FixDecisionLogAndNoUnilateralAbort(unittest.TestCase):
    def setUp(self) -> None:
        self.sender, self.receiver = two_shards()
        self.log = DecisionLog()

    def test_prepared_shard_waits_while_coordinator_is_down(self):
        crash_after_first_commit(self.sender, self.receiver, self.log)
        state = self.receiver.on_timeout(TRANSFER_ID, self.log, coordinator_reachable=False)
        self.assertEqual(state, PREPARED)
        self.assertEqual(self.receiver.locked_by, TRANSFER_ID)

    def test_prepared_shard_reads_logged_decision_when_reachable(self):
        crash_after_first_commit(self.sender, self.receiver, self.log)
        state = self.receiver.on_timeout(TRANSFER_ID, self.log, coordinator_reachable=True)
        self.assertEqual(state, COMMITTED)
        self.assertEqual(total_money(self.sender, self.receiver), 2 * STARTING_BALANCE)

    def test_recovered_coordinator_resends_commit(self):
        crash_after_first_commit(self.sender, self.receiver, self.log)
        self.receiver.on_timeout(TRANSFER_ID, self.log, coordinator_reachable=False)
        recover_coordinator(TRANSFER_ID, [self.sender, self.receiver], self.log)
        self.assertEqual(self.sender.balance, STARTING_BALANCE - TRANSFER_AMOUNT)
        self.assertEqual(self.receiver.balance, STARTING_BALANCE + TRANSFER_AMOUNT)
        self.assertIsNone(self.receiver.locked_by)

    def test_recovery_twice_does_not_apply_the_transfer_twice(self):
        crash_after_first_commit(self.sender, self.receiver, self.log)
        recover_coordinator(TRANSFER_ID, [self.sender, self.receiver], self.log)
        recover_coordinator(TRANSFER_ID, [self.sender, self.receiver], self.log)
        self.assertEqual(self.receiver.balance, STARTING_BALANCE + TRANSFER_AMOUNT)

    def test_crash_before_decision_is_presumed_abort(self):
        self.sender.prepare(TRANSFER_ID, -TRANSFER_AMOUNT)
        self.receiver.prepare(TRANSFER_ID, TRANSFER_AMOUNT)
        decision = recover_coordinator(TRANSFER_ID, [self.sender, self.receiver], self.log)
        self.assertEqual(decision, ABORT)
        self.assertEqual(self.sender.balance, STARTING_BALANCE)
        self.assertEqual(self.receiver.balance, STARTING_BALANCE)

    def test_decision_is_logged_before_any_shard_commits(self):
        crash_after_first_commit(self.sender, self.receiver, self.log)
        self.assertEqual(self.log.lookup(TRANSFER_ID), COMMIT)

    def test_successful_transfer_commits_on_both_shards(self):
        decision = run_transaction(TRANSFER_ID, transfer(self.sender, self.receiver), self.log)
        self.assertEqual(decision, COMMIT)
        self.assertEqual(total_money(self.sender, self.receiver), 2 * STARTING_BALANCE)


class FixVoting(unittest.TestCase):
    def test_insufficient_funds_vote_aborts_everywhere(self):
        sender = LedgerShard("ledger-a", TRANSFER_AMOUNT - 1)
        receiver = LedgerShard("ledger-b", STARTING_BALANCE)
        decision = run_transaction(TRANSFER_ID, transfer(sender, receiver), DecisionLog())
        self.assertEqual(decision, ABORT)
        self.assertEqual(receiver.balance, STARTING_BALANCE)
        self.assertIsNone(receiver.locked_by)

    def test_in_doubt_shard_refuses_other_transactions(self):
        sender, receiver = two_shards()
        log = DecisionLog()
        crash_after_first_commit(sender, receiver, log)
        self.assertFalse(receiver.prepare("transfer-2", 100))


if __name__ == "__main__":
    unittest.main()
