import random
import unittest

from solution import (REFRESHES_PER_SESSION, SEED, SESSIONS, ReadResult, Server,
                      SessionToken, choose_server, count_vanishings, make_primary, make_replicas, monotonic_read,
                      naive_read, read_comments, simulate)


def fresh_stale_fresh_servers() -> list[Server]:
    """Fresh, stale, fresh: the order of servers a reader saw during the incident."""
    replicas = {server.name: server for server in make_replicas()}
    return [replicas["replica-a"], replicas["replica-c"], replicas["replica-a"]]


def results_for(servers: list[Server]) -> list[ReadResult]:
    """Read the thread from each server in turn."""
    return [read_comments(server) for server in servers]


class ReproduceProblem(unittest.TestCase):
    def test_comment_disappears_then_reappears_across_refreshes(self):
        first, second, third = results_for(fresh_stale_fresh_servers())
        self.assertIn("c4", first.comment_ids)
        self.assertNotIn("c4", second.comment_ids)
        self.assertIn("c4", third.comment_ids)

    def test_random_router_flickers_for_almost_every_reader(self):
        report = simulate(use_token=False)
        self.assertGreater(report["flicker_session_fraction"], 0.99)
        self.assertEqual(report["vanishings"], 2997)

    def test_naive_router_ignores_replica_lag(self):
        rng = random.Random(SEED)
        served_by = {naive_read(make_replicas(), rng).server_name for _ in range(50)}
        self.assertIn("replica-c", served_by)


class FixSessionToken(unittest.TestCase):
    def setUp(self):
        self.replicas = make_replicas()
        self.primary = make_primary()
        self.rng = random.Random(SEED)

    def test_no_reader_ever_sees_a_comment_vanish(self):
        report = simulate(use_token=True)
        self.assertEqual(report["vanishings"], 0)
        self.assertEqual(report["flicker_session_fraction"], 0.0)

    def test_new_reader_may_use_any_replica(self):
        chosen = {choose_server(self.replicas, self.primary, SessionToken(), self.rng).name
                  for _ in range(50)}
        self.assertEqual(chosen, {"replica-a", "replica-b", "replica-c"})

    def test_reader_who_saw_lsn_960_never_reads_replica_c(self):
        token = SessionToken(highest_seen_lsn=960)
        for _ in range(50):
            result = monotonic_read(self.replicas, self.primary, token, self.rng)
            self.assertNotEqual(result.server_name, "replica-c")

    def test_token_rises_to_the_position_of_the_server_that_answered(self):
        token = SessionToken()
        result = monotonic_read(self.replicas, self.primary, token, self.rng)
        self.assertEqual(token.highest_seen_lsn, result.applied_lsn)

    def test_token_never_moves_backwards(self):
        token = SessionToken(highest_seen_lsn=995)
        stale_fleet = [Server("replica-b", 960)]
        monotonic_read(stale_fleet, self.primary, token, self.rng)
        self.assertEqual(token.highest_seen_lsn, 1_000)

    def test_falls_back_to_primary_when_every_replica_is_behind(self):
        token = SessionToken(highest_seen_lsn=1_000)
        fleet_without_fresh_replica = [Server("replica-b", 960), Server("replica-c", 900)]
        result = monotonic_read(fleet_without_fresh_replica, self.primary, token, self.rng)
        self.assertEqual(result.server_name, "primary")
        self.assertIn("c4", result.comment_ids)

    def test_falls_back_to_primary_when_there_are_no_replicas(self):
        token = SessionToken(highest_seen_lsn=900)
        result = monotonic_read([], self.primary, token, self.rng)
        self.assertEqual(result.server_name, "primary")

    def test_replica_becomes_eligible_again_once_it_catches_up(self):
        token = SessionToken(highest_seen_lsn=1_000)
        caught_up_fleet = [Server("replica-c", 1_000)]
        result = monotonic_read(caught_up_fleet, self.primary, token, self.rng)
        self.assertEqual(result.server_name, "replica-c")

    def test_fix_still_serves_every_read_from_replicas_in_the_model(self):
        report = simulate(use_token=True)
        self.assertNotIn("primary", report["reads_per_server"])
        self.assertEqual(sum(report["reads_per_server"].values()), SESSIONS * REFRESHES_PER_SESSION)


class CountVanishings(unittest.TestCase):
    def test_growing_comment_list_is_not_a_vanishing(self):
        servers = sorted(make_replicas(), key=lambda server: server.applied_lsn)
        self.assertEqual(count_vanishings(results_for(servers)), 0)

    def test_each_refresh_that_loses_a_comment_counts_once(self):
        self.assertEqual(count_vanishings(results_for(fresh_stale_fresh_servers())), 1)


if __name__ == "__main__":
    unittest.main()
