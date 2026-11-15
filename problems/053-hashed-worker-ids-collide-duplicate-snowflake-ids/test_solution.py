import unittest

from solution import (LEASE_TTL_MS, MAX_WORKER_IDS, RENEW_SAFETY_MARGIN_MS, IdGenerator,
                      LeaseExpired, NoWorkerIdAvailable, WorkerIdRegistry,
                      chance_of_any_collision, leased_generator, naive_worker_id)

START_MS = 1_700_000_000_000
# These two pod names of one Deployment hash to the same worker ID (226).
FIRST_POD = "orders-api-4"
CLASHING_POD = "orders-api-70"


class FakeClock:
    """Millisecond clock the test moves by hand."""

    def __init__(self, now_ms: int = START_MS) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, milliseconds: int) -> None:
        self.now_ms += milliseconds


def ids_from(generator: IdGenerator, count: int) -> list[int]:
    return [generator.next_id() for _ in range(count)]


class ReproduceProblem(unittest.TestCase):
    def test_two_pod_names_hash_to_the_same_worker_id(self):
        self.assertEqual(naive_worker_id(FIRST_POD), naive_worker_id(CLASHING_POD))

    def test_pods_sharing_a_worker_id_issue_identical_ids(self):
        clock = FakeClock()
        first = IdGenerator(naive_worker_id(FIRST_POD), clock)
        second = IdGenerator(naive_worker_id(CLASHING_POD), clock)
        duplicates = set(ids_from(first, 3)) & set(ids_from(second, 3))
        self.assertEqual(len(duplicates), 3)

    def test_first_collision_appears_at_seventy_one_pods(self):
        seen: set[int] = set()
        for index in range(MAX_WORKER_IDS):
            worker_id = naive_worker_id(f"orders-api-{index}")
            if worker_id in seen:
                break
            seen.add(worker_id)
        self.assertEqual(index + 1, 71)

    def test_hundred_random_pods_almost_certainly_collide(self):
        self.assertAlmostEqual(chance_of_any_collision(100), 0.993, places=3)
        self.assertAlmostEqual(chance_of_any_collision(40), 0.538, places=3)


class FixLeasedWorkerIds(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.registry = WorkerIdRegistry()

    def test_clashing_pod_names_get_different_worker_ids(self):
        first = leased_generator(self.registry, FIRST_POD, self.clock)
        second = leased_generator(self.registry, CLASHING_POD, self.clock)
        self.assertNotEqual(first.worker_id, second.worker_id)
        self.assertFalse(set(ids_from(first, 3)) & set(ids_from(second, 3)))

    def test_every_one_of_1024_pods_gets_a_unique_worker_id(self):
        worker_ids = set()
        for index in range(MAX_WORKER_IDS):
            worker_ids.add(self.registry.acquire(f"pod-{index}", self.clock()).worker_id)
        self.assertEqual(len(worker_ids), MAX_WORKER_IDS)

    def test_pod_beyond_capacity_is_refused_instead_of_sharing(self):
        for index in range(MAX_WORKER_IDS):
            self.registry.acquire(f"pod-{index}", self.clock())
        with self.assertRaises(NoWorkerIdAvailable):
            self.registry.acquire("pod-extra", self.clock())

    def test_expired_lease_of_a_dead_pod_is_reused(self):
        dead = self.registry.acquire("dead-pod", self.clock())
        self.clock.advance(LEASE_TTL_MS)
        replacement = self.registry.acquire("new-pod", self.clock())
        self.assertEqual(replacement.worker_id, dead.worker_id)

    def test_renewed_lease_keeps_its_worker_id(self):
        lease = self.registry.acquire("pod-a", self.clock())
        self.clock.advance(LEASE_TTL_MS // 2)
        self.assertTrue(self.registry.renew(lease, self.clock()))
        self.clock.advance(LEASE_TTL_MS // 2)
        other = self.registry.acquire("pod-b", self.clock())
        self.assertNotEqual(other.worker_id, lease.worker_id)

    def test_renewal_fails_once_another_pod_took_the_id(self):
        lease = self.registry.acquire("paused-pod", self.clock())
        self.clock.advance(LEASE_TTL_MS)
        self.registry.acquire("new-pod", self.clock())
        self.assertFalse(self.registry.renew(lease, self.clock()))


class FixGeneratorStopsWithoutLease(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.registry = WorkerIdRegistry()
        self.generator = leased_generator(self.registry, "pod-a", self.clock)

    def test_ids_are_issued_while_the_lease_is_valid(self):
        self.clock.advance(LEASE_TTL_MS - RENEW_SAFETY_MARGIN_MS - 1)
        self.assertGreater(self.generator.next_id(), 0)

    def test_generator_refuses_inside_the_safety_margin(self):
        self.clock.advance(LEASE_TTL_MS - RENEW_SAFETY_MARGIN_MS)
        with self.assertRaises(LeaseExpired):
            self.generator.next_id()

    def test_generator_resumes_after_renewal(self):
        self.clock.advance(LEASE_TTL_MS - RENEW_SAFETY_MARGIN_MS)
        self.assertTrue(self.registry.renew(self.generator.lease, self.clock()))
        self.assertGreater(self.generator.next_id(), 0)

    def test_ids_in_one_millisecond_differ_by_sequence(self):
        ids = ids_from(self.generator, 3)
        self.assertEqual(len(set(ids)), 3)
        self.assertEqual(ids, sorted(ids))


if __name__ == "__main__":
    unittest.main()
