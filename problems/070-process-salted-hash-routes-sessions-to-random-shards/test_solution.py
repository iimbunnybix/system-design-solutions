import unittest

from solution import (SHARD_COUNT, ShardedSessionStore, agreement_rate, login_then_next_request,
                      naive_shard_for, routes_in_new_process, stable_shard_for)

SESSIONS = 2_000
# Two gunicorn-style workers: each interpreter gets its own hash seed.
WORKER_A_SEED, WORKER_B_SEED = "1", "2"


def session_ids(count=SESSIONS):
    return [f"sess-{number:06d}" for number in range(count)]


def two_workers(function_name):
    ids = session_ids()
    worker_a = routes_in_new_process(ids, function_name, WORKER_A_SEED)
    worker_b = routes_in_new_process(ids, function_name, WORKER_B_SEED)
    return ids, worker_a, worker_b


class ReproduceProblem(unittest.TestCase):
    def test_naive_routing_is_consistent_inside_one_process(self):
        self.assertEqual(naive_shard_for("sess-000001"), naive_shard_for("sess-000001"))

    def test_two_workers_agree_on_only_about_one_in_eight_sessions(self):
        _, worker_a, worker_b = two_workers("naive_shard_for")
        self.assertAlmostEqual(agreement_rate(worker_a, worker_b), 1 / SHARD_COUNT, delta=0.03)

    def test_most_users_are_logged_out_on_their_next_request(self):
        ids, worker_a, worker_b = two_workers("naive_shard_for")
        logged_out = login_then_next_request(ShardedSessionStore(), ids, worker_a, worker_b)
        self.assertEqual(logged_out, 1_727)


class FixStableHash(unittest.TestCase):
    def test_two_workers_route_every_session_to_the_same_shard(self):
        _, worker_a, worker_b = two_workers("stable_shard_for")
        self.assertEqual(agreement_rate(worker_a, worker_b), 1.0)

    def test_no_user_is_logged_out(self):
        ids, worker_a, worker_b = two_workers("stable_shard_for")
        self.assertEqual(login_then_next_request(ShardedSessionStore(), ids, worker_a, worker_b), 0)

    def test_known_key_maps_to_a_fixed_shard(self):
        # Pinned so a change to the routing function (which would move every session) fails loudly.
        self.assertEqual(stable_shard_for("sess-000001"), 4)

    def test_sessions_spread_over_all_shards(self):
        counts = [0] * SHARD_COUNT
        for session_id in session_ids():
            counts[stable_shard_for(session_id)] += 1
        self.assertGreater(min(counts), SESSIONS / SHARD_COUNT * 0.8)

    def test_non_ascii_session_ids_are_routed(self):
        self.assertIn(stable_shard_for("sess-ü-日本"), range(SHARD_COUNT))

    def test_zero_shards_is_rejected(self):
        with self.assertRaises(ValueError):
            stable_shard_for("sess-000001", 0)

    def test_empty_key_list_counts_as_full_agreement(self):
        self.assertEqual(agreement_rate([], []), 1.0)


if __name__ == "__main__":
    unittest.main()
