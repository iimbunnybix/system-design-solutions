import random
import unittest

from solution import (BACKEND_SAFE_IN_FLIGHT, CLIENT_COUNT, NEW_NODE, REQUEST_DURATION_TICKS,
                      TwoChoicesClient, build_cluster_with_new_node, fair_share, naive_pick,
                      simulate_naive, simulate_two_choices)

SEEDS = range(5)


class ReproduceProblem(unittest.TestCase):
    def test_every_client_picks_the_new_node_from_the_report(self):
        report = build_cluster_with_new_node().load_report()
        self.assertEqual(naive_pick(report), NEW_NODE)

    def test_new_node_ends_up_holding_every_in_flight_request(self):
        stats = simulate_naive()
        self.assertEqual(stats.peak_on_new_node, CLIENT_COUNT * REQUEST_DURATION_TICKS)

    def test_herding_overloads_backends_far_beyond_fair_share(self):
        stats = simulate_naive()
        self.assertGreater(stats.peak_in_flight, 10 * fair_share())
        self.assertGreater(stats.overloaded_backend_ticks, 0)


class FixWithTwoRandomChoices(unittest.TestCase):
    def test_no_backend_is_ever_overloaded(self):
        for seed in SEEDS:
            with self.subTest(seed=seed):
                stats = simulate_two_choices(seed)
                self.assertEqual(stats.overloaded_backend_ticks, 0)
                self.assertLessEqual(stats.peak_in_flight, BACKEND_SAFE_IN_FLIGHT)

    def test_new_node_still_receives_its_share_of_traffic(self):
        stats = simulate_two_choices(seed=0)
        self.assertGreaterEqual(stats.peak_on_new_node, fair_share())

    def test_same_seed_gives_same_result(self):
        first = simulate_two_choices(seed=3)
        second = simulate_two_choices(seed=3)
        self.assertEqual(first.peak_in_flight, second.peak_in_flight)

    def test_client_prefers_backend_it_has_loaded_less(self):
        client = TwoChoicesClient(["busy", "idle"], random.Random(1))
        for _ in range(3):
            client.my_finishing_ticks["busy"].append(100)
        self.assertEqual(client.pick(now=0), "idle")

    def test_finished_requests_no_longer_count_as_local_load(self):
        client = TwoChoicesClient(["a", "b"], random.Random(1))
        client.my_finishing_ticks["a"].append(5)
        client.forget_finished(now=5)
        self.assertEqual(client.my_finishing_ticks["a"], [])

    def test_single_backend_is_rejected(self):
        with self.assertRaises(ValueError):
            TwoChoicesClient(["only"], random.Random(1))


if __name__ == "__main__":
    unittest.main()
