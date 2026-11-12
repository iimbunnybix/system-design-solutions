import random
import unittest

from solution import (ENTERPRISE_USERS, REQUESTS_PER_USER, HashRing, Request,
                      SessionAffinityBalancer, backend_names, fair_share, incident_users,
                      load_with_naive, load_with_sessions, naive_route_by_source_ip,
                      share_of_sessions_moved_by_modulo, share_of_sessions_moved_by_ring)

SEED = 48
SEEDS = range(5)
BACKENDS = backend_names()
NAT_IP = "203.0.113.10"


def naive_load(seed: int = SEED) -> dict[str, int]:
    """Per-backend requests for the incident traffic under source-IP affinity."""
    return load_with_naive(incident_users(random.Random(seed)), BACKENDS)


def session_load(seed: int = SEED) -> dict[str, int]:
    """Per-backend requests for the same traffic under cookie affinity."""
    balancer = SessionAffinityBalancer(BACKENDS, random.Random(seed))
    return load_with_sessions(incident_users(random.Random(seed)), balancer, BACKENDS)


def random_session_ids(count: int) -> list[str]:
    """Seeded fake session ids."""
    rng = random.Random(SEED)
    return [format(rng.getrandbits(64), "016x") for _ in range(count)]


class ReproduceProblem(unittest.TestCase):
    def test_every_request_from_the_nat_goes_to_one_backend(self):
        backends_seen = set()
        for _ in range(50):
            backends_seen.add(naive_route_by_source_ip(Request(NAT_IP, None), BACKENDS))
        self.assertEqual(len(backends_seen), 1)

    def test_one_backend_gets_over_six_times_its_fair_share(self):
        load = naive_load()
        self.assertEqual(max(load.values()), 31_300)
        self.assertGreater(max(load.values()), 6 * fair_share(load))

    def test_other_backends_sit_mostly_idle(self):
        load = naive_load()
        self.assertLess(sorted(load.values())[-2], 0.3 * fair_share(load))

    def test_cookie_is_ignored_by_source_ip_routing(self):
        first = naive_route_by_source_ip(Request(NAT_IP, "aaaa"), BACKENDS)
        second = naive_route_by_source_ip(Request(NAT_IP, "bbbb"), BACKENDS)
        self.assertEqual(first, second)


class FixWithSessionCookieAffinity(unittest.TestCase):
    def test_no_backend_exceeds_fair_share_by_more_than_20_percent(self):
        for seed in SEEDS:
            with self.subTest(seed=seed):
                load = session_load(seed)
                self.assertLess(max(load.values()), 1.2 * fair_share(load))

    def test_every_backend_gets_at_least_80_percent_of_fair_share(self):
        for seed in SEEDS:
            with self.subTest(seed=seed):
                load = session_load(seed)
                self.assertGreater(min(load.values()), 0.8 * fair_share(load))

    def test_same_cookie_always_reaches_same_backend(self):
        balancer = SessionAffinityBalancer(BACKENDS, random.Random(SEED))
        first, _ = balancer.route(Request(NAT_IP, "session-1"))
        later, _ = balancer.route(Request("192.0.2.99", "session-1"))
        self.assertEqual(first, later)

    def test_users_behind_one_nat_spread_across_all_backends(self):
        balancer = SessionAffinityBalancer(BACKENDS, random.Random(SEED))
        backends_seen = set()
        for index in range(ENTERPRISE_USERS):
            backend, _ = balancer.route(Request(NAT_IP, f"user-{index}"))
            backends_seen.add(backend)
        self.assertEqual(backends_seen, set(BACKENDS))

    def test_total_requests_are_unchanged(self):
        users = ENTERPRISE_USERS + 1_000
        self.assertEqual(sum(session_load().values()), users * REQUESTS_PER_USER)


class CookielessFallback(unittest.TestCase):
    def test_request_without_cookie_gets_a_new_session_cookie(self):
        balancer = SessionAffinityBalancer(BACKENDS, random.Random(SEED))
        backend, cookie = balancer.route(Request(NAT_IP, None))
        self.assertIsNotNone(cookie)
        self.assertEqual(balancer.route(Request(NAT_IP, cookie)), (backend, None))

    def test_clients_that_drop_cookies_are_spread_not_pinned(self):
        balancer = SessionAffinityBalancer(BACKENDS, random.Random(SEED))
        backends_seen = set()
        for _ in range(100):
            backend, _ = balancer.route(Request(NAT_IP, None))
            backends_seen.add(backend)
        self.assertEqual(backends_seen, set(BACKENDS))

    def test_empty_cookie_is_treated_as_missing(self):
        balancer = SessionAffinityBalancer(BACKENDS, random.Random(SEED))
        _, cookie = balancer.route(Request(NAT_IP, ""))
        self.assertIsNotNone(cookie)


class HashRingBehaviour(unittest.TestCase):
    def test_removing_a_backend_moves_about_one_eighth_of_sessions(self):
        moved = share_of_sessions_moved_by_ring(random_session_ids(10_000), BACKENDS, "backend-3")
        self.assertLess(moved, 0.2)

    def test_modulo_hashing_would_move_most_sessions(self):
        moved = share_of_sessions_moved_by_modulo(random_session_ids(10_000), BACKENDS, "backend-3")
        self.assertGreater(moved, 0.8)

    def test_removed_backend_receives_no_sessions(self):
        ring = HashRing(BACKENDS)
        ring.remove("backend-3")
        for session in random_session_ids(2_000):
            self.assertNotEqual(ring.lookup(session), "backend-3")

    def test_empty_ring_is_rejected(self):
        with self.assertRaises(ValueError):
            HashRing([])


if __name__ == "__main__":
    unittest.main()
