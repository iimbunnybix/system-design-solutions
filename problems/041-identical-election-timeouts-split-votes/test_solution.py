import random
import unittest

from solution import (GIVE_UP_AFTER_MS, RANDOM_TIMEOUT_MAX_MS, RANDOM_TIMEOUT_MIN_MS,
                      Election, naive_fixed_timeout, randomized_timeout,
                      simulate_failovers)

FOLLOWERS = 4


def fresh_election(policy=randomized_timeout) -> Election:
    """An election whose event loop has not started, for poking single rules."""
    return Election(policy, seed=0)


class ReproduceProblem(unittest.TestCase):
    def test_fixed_timeouts_never_elect_a_leader_within_ten_seconds(self):
        report = simulate_failovers(naive_fixed_timeout, trials=200)
        self.assertEqual(report.no_leader, 200)

    def test_every_follower_becomes_a_candidate_in_every_term(self):
        result = Election(naive_fixed_timeout, seed=0).run()
        self.assertIsNone(result.leader)
        self.assertEqual(result.highest_term, 66)
        self.assertEqual(result.elections_started, FOLLOWERS * result.highest_term)


class FixRandomizedTimeouts(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.report = simulate_failovers(randomized_timeout, trials=1_000)

    def test_every_failover_elects_a_leader(self):
        self.assertEqual(self.report.no_leader, 0)

    def test_median_and_worst_failover_times(self):
        self.assertEqual(self.report.median_ms, 187)
        self.assertEqual(self.report.worst_ms, 691)

    def test_most_failovers_need_only_one_election(self):
        single = sum(1 for seed in range(1_000)
                     if Election(randomized_timeout, seed).run().elections_started == 1)
        self.assertEqual(single, 863)

    def test_three_node_cluster_also_elects_a_leader(self):
        result = Election(randomized_timeout, seed=0, cluster_size=3).run()
        self.assertIsNotNone(result.leader)
        self.assertLess(result.elected_at_ms, GIVE_UP_AFTER_MS)

    def test_random_timeouts_stay_within_bounds(self):
        rng = random.Random(7)
        for _ in range(1_000):
            timeout = randomized_timeout(rng)
            self.assertGreaterEqual(timeout, RANDOM_TIMEOUT_MIN_MS)
            self.assertLessEqual(timeout, RANDOM_TIMEOUT_MAX_MS)


class VotingRules(unittest.TestCase):
    def test_a_node_votes_at_most_once_per_term(self):
        election = fresh_election()
        election.on_request_vote(0, voter_id=1, candidate_id=2, term=1)
        election.on_request_vote(0, voter_id=1, candidate_id=3, term=1)
        self.assertEqual(election.nodes[1].voted_for, 2)

    def test_a_candidate_steps_down_for_a_newer_term(self):
        election = fresh_election()
        election.on_timeout(0, node_id=1, generation=0)
        self.assertTrue(election.nodes[1].is_candidate)
        election.on_request_vote(5, voter_id=1, candidate_id=2, term=2)
        self.assertFalse(election.nodes[1].is_candidate)
        self.assertEqual(election.nodes[1].voted_for, 2)

    def test_old_timer_is_ignored_after_a_reset(self):
        election = fresh_election()
        election.reset_timer(election.nodes[1], 0)    # generation 1 is current
        election.on_timeout(10, node_id=1, generation=0)
        self.assertEqual(election.nodes[1].term, 0)

    def test_votes_from_an_old_term_are_not_counted(self):
        election = fresh_election()
        election.on_timeout(0, node_id=1, generation=0)   # term 1
        election.on_timeout(1, node_id=1, generation=election.nodes[1].timer_generation)
        election.on_vote_reply(2, candidate_id=1, voter_id=2, term=1, granted=True)
        election.on_vote_reply(2, candidate_id=1, voter_id=3, term=1, granted=True)
        self.assertIsNone(election.leader)


if __name__ == "__main__":
    unittest.main()
