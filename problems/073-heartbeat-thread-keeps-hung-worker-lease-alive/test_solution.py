import unittest

from solution import LEASE_SECONDS, MAX_STALL_SECONDS, Coordinator, simulate


def leased_coordinator(check_progress: bool = True) -> tuple[Coordinator, int]:
    """A coordinator whose job is leased to worker-a at second 0."""
    coordinator = Coordinator(check_progress)
    lease = coordinator.acquire("worker-a", now=0)
    return coordinator, lease.token


class ReproduceProblem(unittest.TestCase):
    def test_job_held_by_hung_worker_is_never_completed(self):
        self.assertEqual(simulate(check_progress=False), (None, None))

    def test_heartbeat_without_progress_renews_forever(self):
        coordinator, token = leased_coordinator(check_progress=False)
        for now in range(10, 600, 10):
            self.assertTrue(coordinator.heartbeat(token, progress=0, now=now))


class FixProgressAwareLease(unittest.TestCase):
    def test_spare_worker_completes_the_job_after_the_stall(self):
        self.assertEqual(simulate(check_progress=True), ("worker-b", 160))

    def test_renewal_is_refused_once_progress_stalls_too_long(self):
        coordinator, token = leased_coordinator()
        for now in range(10, MAX_STALL_SECONDS + 1, 10):
            self.assertTrue(coordinator.heartbeat(token, progress=0, now=now))
        self.assertFalse(coordinator.heartbeat(token, progress=0, now=MAX_STALL_SECONDS + 10))

    def test_slow_worker_that_keeps_progressing_keeps_its_lease(self):
        coordinator, token = leased_coordinator()
        for step, now in enumerate(range(10, 600, 10), start=1):
            self.assertTrue(coordinator.heartbeat(token, progress=step, now=now))

    def test_stalled_lease_becomes_available_after_it_expires(self):
        coordinator, token = leased_coordinator()
        for now in range(10, 51, 10):
            coordinator.heartbeat(token, progress=0, now=now)  # last renewal at 50: expires at 80
        self.assertFalse(coordinator.heartbeat(token, progress=0, now=70))
        self.assertIsNone(coordinator.acquire("worker-b", now=69))
        self.assertIsNotNone(coordinator.acquire("worker-b", now=50 + LEASE_SECONDS))

    def test_new_lease_gets_a_larger_fencing_token(self):
        coordinator, old_token = leased_coordinator()
        new_lease = coordinator.acquire("worker-b", now=LEASE_SECONDS)
        self.assertGreater(new_lease.token, old_token)


class FixFencing(unittest.TestCase):
    def test_old_worker_cannot_complete_after_reassignment(self):
        coordinator, old_token = leased_coordinator()
        coordinator.acquire("worker-b", now=LEASE_SECONDS)
        self.assertFalse(coordinator.complete(old_token, now=LEASE_SECONDS + 1))
        self.assertIsNone(coordinator.completed_by)

    def test_old_worker_cannot_renew_after_reassignment(self):
        coordinator, old_token = leased_coordinator()
        coordinator.acquire("worker-b", now=LEASE_SECONDS)
        self.assertFalse(coordinator.heartbeat(old_token, progress=99, now=LEASE_SECONDS + 1))

    def test_expired_lease_cannot_complete_even_if_not_reassigned(self):
        coordinator, token = leased_coordinator()
        self.assertFalse(coordinator.complete(token, now=LEASE_SECONDS))

    def test_completed_job_is_not_handed_out_again(self):
        coordinator, token = leased_coordinator()
        coordinator.complete(token, now=5)
        self.assertIsNone(coordinator.acquire("worker-b", now=LEASE_SECONDS + 1))


if __name__ == "__main__":
    unittest.main()
