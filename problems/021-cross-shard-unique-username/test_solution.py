import threading
import unittest

from solution import (CONFIRMED, RESERVATION_TTL_SECONDS, UserShard, UsernameIndex,
                      UsernameTaken, naive_register, normalize_username, register)

SHARD_COUNT = 4
PARTITION_COUNT = 8
START_TIME = 1_000.0
# user_ids 1 and 2 land on different shards (1 % 4 != 2 % 4)
FIRST_USER = 1
SECOND_USER = 2


def make_shards() -> list[UserShard]:
    return [UserShard(shard_id) for shard_id in range(SHARD_COUNT)]


def all_usernames(shards: list[UserShard]) -> list[str]:
    names = []
    for shard in shards:
        names.extend(shard.users.values())
    return names


class ReproduceProblem(unittest.TestCase):
    def test_same_username_on_two_shards_is_accepted_twice(self):
        shards = make_shards()
        naive_register(shards, FIRST_USER, "alice")
        naive_register(shards, SECOND_USER, "alice")
        self.assertEqual(all_usernames(shards), ["alice", "alice"])

    def test_local_index_only_catches_duplicates_on_the_same_shard(self):
        shards = make_shards()
        naive_register(shards, FIRST_USER, "alice")
        with self.assertRaises(UsernameTaken):
            naive_register(shards, FIRST_USER + SHARD_COUNT, "alice")


class FixGlobalUsernameIndex(unittest.TestCase):
    def setUp(self):
        self.shards = make_shards()
        self.index = UsernameIndex(PARTITION_COUNT)

    def test_second_user_on_another_shard_is_rejected(self):
        register(self.index, self.shards, FIRST_USER, "alice", START_TIME)
        with self.assertRaises(UsernameTaken):
            register(self.index, self.shards, SECOND_USER, "alice", START_TIME)
        self.assertEqual(all_usernames(self.shards), ["alice"])

    def test_names_differing_only_in_case_and_spaces_collide(self):
        register(self.index, self.shards, FIRST_USER, "Alice", START_TIME)
        with self.assertRaises(UsernameTaken):
            register(self.index, self.shards, SECOND_USER, "  ALICE ", START_TIME)

    def test_different_names_both_succeed(self):
        register(self.index, self.shards, FIRST_USER, "alice", START_TIME)
        register(self.index, self.shards, SECOND_USER, "bob", START_TIME)
        self.assertCountEqual(all_usernames(self.shards), ["alice", "bob"])

    def test_failed_shard_insert_releases_the_name(self):
        self.shards[FIRST_USER % SHARD_COUNT].fail_next_insert = True
        with self.assertRaises(ConnectionError):
            register(self.index, self.shards, FIRST_USER, "alice", START_TIME)
        register(self.index, self.shards, SECOND_USER, "alice", START_TIME)
        self.assertEqual(all_usernames(self.shards), ["alice"])

    def test_confirmed_claim_never_expires(self):
        register(self.index, self.shards, FIRST_USER, "alice", START_TIME)
        later = START_TIME + RESERVATION_TTL_SECONDS * 100
        with self.assertRaises(UsernameTaken):
            register(self.index, self.shards, SECOND_USER, "alice", later)


class FixCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.shards = make_shards()
        self.index = UsernameIndex(PARTITION_COUNT)
        self.partition = self.index.partition_for("alice")

    def reserve_only(self, user_id: int, now: float) -> None:
        """Simulate a sign-up that crashed right after reserving."""
        self.partition.reserve("alice", user_id, now, lambda owner, name: False)

    def test_fresh_pending_claim_blocks_other_users(self):
        self.reserve_only(FIRST_USER, START_TIME)
        with self.assertRaises(UsernameTaken):
            register(self.index, self.shards, SECOND_USER, "alice", START_TIME + 1)

    def test_abandoned_pending_claim_is_reclaimed_after_ttl(self):
        self.reserve_only(FIRST_USER, START_TIME)
        register(self.index, self.shards, SECOND_USER, "alice",
                 START_TIME + RESERVATION_TTL_SECONDS)
        self.assertEqual(self.partition.claims["alice"].user_id, SECOND_USER)

    def test_claim_whose_user_was_created_is_not_stolen_after_ttl(self):
        # Crash between "create user" and "confirm".
        self.reserve_only(FIRST_USER, START_TIME)
        self.shards[FIRST_USER % SHARD_COUNT].insert_user(FIRST_USER, "alice")
        with self.assertRaises(UsernameTaken):
            register(self.index, self.shards, SECOND_USER, "alice",
                     START_TIME + RESERVATION_TTL_SECONDS * 2)
        self.assertEqual(self.partition.claims["alice"].state, CONFIRMED)

    def test_same_user_retrying_after_crash_succeeds(self):
        self.reserve_only(FIRST_USER, START_TIME)
        register(self.index, self.shards, FIRST_USER, "alice", START_TIME + 1)
        self.assertEqual(all_usernames(self.shards), ["alice"])


class FixConcurrency(unittest.TestCase):
    def test_fifty_concurrent_sign_ups_for_one_name_yield_one_user(self):
        shards, index = make_shards(), UsernameIndex(PARTITION_COUNT)
        thread_count = 50
        barrier = threading.Barrier(thread_count)
        winners, losers = [], []

        def sign_up(user_id: int) -> None:
            barrier.wait()
            try:
                register(index, shards, user_id, "alice", START_TIME)
                winners.append(user_id)
            except UsernameTaken:
                losers.append(user_id)

        threads = [threading.Thread(target=sign_up, args=(user_id,))
                   for user_id in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), thread_count - 1)
        self.assertEqual(all_usernames(shards), ["alice"])


class NormalizationTests(unittest.TestCase):
    def test_normalization_lowercases_and_strips(self):
        self.assertEqual(normalize_username("  AlIcE "), "alice")


if __name__ == "__main__":
    unittest.main()
