import unittest

from solution import VISIBILITY_TIMEOUT_S, FakeClock, LeaseLost, VisibilityQueue, Worker, run

LONG_JOB_S = 180


def setup(use_heartbeat, job_seconds=LONG_JOB_S, worker_count=3):
    clock = FakeClock()
    queue = VisibilityQueue(clock)
    queue.send("video-1", "transcode 4k.mp4")
    workers = [Worker(f"w{index}", queue, job_seconds, use_heartbeat)
               for index in range(worker_count)]
    return clock, queue, workers


def all_completed(workers):
    completed = []
    for worker in workers:
        completed.extend(worker.completed)
    return completed


class ReproduceProblem(unittest.TestCase):
    def test_long_job_is_completed_by_several_workers(self):
        clock, queue, workers = setup(use_heartbeat=False)
        run(clock, workers, 600)
        self.assertEqual(all_completed(workers), ["video-1"] * 7)

    def test_queue_never_drains_because_every_delete_uses_a_stale_receipt(self):
        clock, queue, workers = setup(use_heartbeat=False)
        run(clock, workers, 600)
        self.assertEqual(queue.messages["video-1"].receive_count, 10)

    def test_short_job_is_fine_without_heartbeat(self):
        clock, queue, workers = setup(use_heartbeat=False, job_seconds=30)
        run(clock, workers, 600)
        self.assertEqual(all_completed(workers), ["video-1"])


class FixHeartbeatExtendsTheLease(unittest.TestCase):
    def test_long_job_is_completed_exactly_once(self):
        clock, queue, workers = setup(use_heartbeat=True)
        run(clock, workers, 600)
        self.assertEqual(all_completed(workers), ["video-1"])
        self.assertEqual(queue.messages, {})

    def test_job_is_only_received_once_while_heartbeating(self):
        clock, queue, workers = setup(use_heartbeat=True)
        run(clock, workers, LONG_JOB_S - 5)
        self.assertEqual(queue.messages["video-1"].receive_count, 1)

    def test_crashed_worker_job_is_picked_up_after_the_timeout(self):
        clock, queue, workers = setup(use_heartbeat=True, worker_count=2)
        crashed, survivor = workers
        run(clock, workers, 600, paused={"w0": range(10, 10_000)})
        self.assertEqual(crashed.completed, [])
        self.assertEqual(survivor.completed, ["video-1"])

    def test_stalled_worker_abandons_instead_of_committing_a_duplicate(self):
        clock, queue, workers = setup(use_heartbeat=True, worker_count=2)
        # w0 freezes (long GC pause) for longer than the visibility timeout.
        run(clock, workers, 900, paused={"w0": range(30, 30 + 2 * VISIBILITY_TIMEOUT_S)})
        self.assertEqual(all_completed(workers), ["video-1"])
        self.assertEqual(workers[0].abandoned, ["video-1"])


class FixReceiptsAreSingleUse(unittest.TestCase):
    def test_stale_receipt_cannot_delete_a_redelivered_message(self):
        clock = FakeClock()
        queue = VisibilityQueue(clock)
        queue.send("m", "body")
        first = queue.receive()
        clock.advance(VISIBILITY_TIMEOUT_S)
        queue.receive()
        with self.assertRaises(LeaseLost):
            queue.delete(first.receipt)

    def test_extend_after_delete_raises_lease_lost(self):
        clock = FakeClock()
        queue = VisibilityQueue(clock)
        queue.send("m", "body")
        delivery = queue.receive()
        queue.delete(delivery.receipt)
        with self.assertRaises(LeaseLost):
            queue.extend(delivery.receipt, 30)

    def test_hidden_message_is_not_received_again_before_timeout(self):
        clock = FakeClock()
        queue = VisibilityQueue(clock)
        queue.send("m", "body")
        queue.receive()
        clock.advance(VISIBILITY_TIMEOUT_S - 1)
        self.assertIsNone(queue.receive())


if __name__ == "__main__":
    unittest.main()
