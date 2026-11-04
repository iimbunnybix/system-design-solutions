import threading
import unittest

from solution import (FakeClock, GatewayNode, LocalFixedWindow, SharedStore, max_in_any_window,
                      simulate)

LIMIT = 100            # plan: 100 requests per minute per API key
ABUSIVE_RATE = 50      # requests/second from a misbehaving client (3,000/min)


def shared_fleet(clock, n, store=None):
    store = store or SharedStore(clock)
    return store, [GatewayNode(f"gw-{i}", store, clock, LIMIT, fleet_size_hint=n) for i in range(n)]


class ReproducePerNodeLimits(unittest.TestCase):
    def test_quota_multiplied_by_fleet_size(self):
        for nodes, expected_per_min in ((10, 1_000), (20, 2_000)):
            clock = FakeClock()
            fleet = [LocalFixedWindow(clock, LIMIT) for _ in range(nodes)]
            admitted = simulate(fleet, clock, ABUSIVE_RATE, 300)
            self.assertEqual(len(admitted) / 5, expected_per_min)

    def test_dividing_the_limit_blocks_sticky_clients(self):
        clock = FakeClock()
        fleet = [LocalFixedWindow(clock, LIMIT // 10) for _ in range(10)]
        # A well-behaved client: 80 req/min, but all of its traffic lands on one node.
        admitted = simulate(fleet, clock, 80 / 60, 300, route=lambda i: 0)
        self.assertEqual(len(admitted), 50)          # 10/min instead of 80/min


class SharedSlidingWindow(unittest.TestCase):
    def test_fleet_enforces_one_quota(self):
        for nodes in (10, 20):
            clock = FakeClock()
            _, fleet = shared_fleet(clock, nodes)
            admitted = simulate(fleet, clock, ABUSIVE_RATE, 300)
            self.assertEqual(max_in_any_window(admitted), LIMIT)
            self.assertEqual(len(admitted), 496)     # ~100/min over 5 minutes, any fleet size

    def test_sticky_client_within_quota_is_not_blocked(self):
        clock = FakeClock()
        _, fleet = shared_fleet(clock, 10)
        admitted = simulate(fleet, clock, 80 / 60, 300, route=lambda i: 0)
        self.assertEqual(len(admitted), 400)         # every request allowed

    def test_keys_are_isolated(self):
        clock = FakeClock()
        _, fleet = shared_fleet(clock, 3)
        for _ in range(LIMIT):
            self.assertTrue(fleet[0].allow("a"))
        self.assertFalse(fleet[1].allow("a"))
        self.assertTrue(fleet[2].allow("b"))

    def test_window_slides_rather_than_resets(self):
        clock = FakeClock()
        store, fleet = shared_fleet(clock, 2)
        for _ in range(LIMIT):                       # burst at t=0
            fleet[0].allow("k")
        clock.advance(60)                            # start of next window: prev still counts fully
        self.assertFalse(fleet[1].allow("k"))
        clock.advance(30)                            # half of prev window still overlaps -> 50 used
        allowed = sum(fleet[i % 2].allow("k") for i in range(100))
        self.assertEqual(allowed, 50)

    def test_concurrent_nodes_cannot_overshoot(self):
        clock = FakeClock()
        store, fleet = shared_fleet(clock, 10)
        results = []
        lock = threading.Lock()

        def worker(node):
            local = [node.allow("k") for _ in range(50)]
            with lock:
                results.extend(local)

        threads = [threading.Thread(target=worker, args=(n,)) for n in fleet]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sum(results), LIMIT)        # 500 attempts, exactly 100 admitted


class StoreOutage(unittest.TestCase):
    def test_falls_back_to_per_node_share(self):
        clock = FakeClock()
        store, fleet = shared_fleet(clock, 10)
        store.available = False
        admitted = simulate(fleet, clock, ABUSIVE_RATE, 60)
        self.assertEqual(len(admitted), LIMIT)       # 10 nodes x 10 each, round-robin
        self.assertTrue(all(n.degraded_decisions > 0 for n in fleet))

    def test_recovers_when_store_returns(self):
        clock = FakeClock()
        store, fleet = shared_fleet(clock, 10)
        store.available = False
        simulate(fleet, clock, ABUSIVE_RATE, 60)
        store.available = True
        calls = store.calls
        simulate(fleet, clock, ABUSIVE_RATE, 60)
        self.assertGreater(store.calls, calls)


if __name__ == "__main__":
    unittest.main()
