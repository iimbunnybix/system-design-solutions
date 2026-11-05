import random
import threading
import time
import unittest

from solution import NaiveCache, StampedeProtectedCache


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class CountingDB:
    def __init__(self, delay=0.0, fail=False):
        self.calls = 0
        self.delay = delay
        self.fail = fail
        self.lock = threading.Lock()

    def __call__(self, key):
        with self.lock:
            self.calls += 1
            n = self.calls
        if self.delay:
            time.sleep(self.delay)
        if self.fail:
            raise ConnectionError("db down")
        return f"{key}-v{n}"


def hammer(cache, n=50, key="home"):
    barrier = threading.Barrier(n)
    results, errors = [], []

    def worker():
        barrier.wait()
        try:
            results.append(cache.get(key))
        except Exception as e:      # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results, errors


class StampedeTests(unittest.TestCase):
    def test_naive_cache_stampedes(self):
        db = CountingDB(delay=0.2)
        results, _ = hammer(NaiveCache(db, ttl_s=60))
        self.assertEqual(db.calls, 50)          # every request hit the DB

    def test_single_flight_one_load(self):
        db = CountingDB(delay=0.2)
        results, errors = hammer(StampedeProtectedCache(db, ttl_s=60, beta=0))
        self.assertEqual(db.calls, 1)
        self.assertEqual(set(results), {"home-v1"})
        self.assertEqual(errors, [])

    def test_loader_failure_propagates_and_is_not_cached(self):
        db = CountingDB(delay=0.1, fail=True)
        cache = StampedeProtectedCache(db, ttl_s=60, beta=0)
        results, errors = hammer(cache, n=20)
        self.assertEqual(db.calls, 1)
        self.assertEqual(len(errors), 20)
        db.fail = False
        self.assertEqual(cache.get("home"), "home-v2")    # retried, not negative-cached

    def test_stale_while_revalidate_serves_old_value_and_refreshes_once(self):
        clock, db, jobs = FakeClock(), CountingDB(), []
        cache = StampedeProtectedCache(db, ttl_s=60, stale_s=30, beta=0, clock=clock,
                                       background=jobs.append)
        self.assertEqual(cache.get("k"), "k-v1")
        clock.t += 61                                     # soft-expired
        self.assertEqual([cache.get("k") for _ in range(10)], ["k-v1"] * 10)
        self.assertEqual(db.calls, 1)                     # nobody waited on the DB
        jobs[0]()                                         # run the background refresh
        self.assertEqual(cache.get("k"), "k-v2")
        self.assertEqual(db.calls, 2)

    def test_many_stale_reads_schedule_exactly_one_background_refresh(self):
        clock, db, jobs = FakeClock(), CountingDB(), []
        cache = StampedeProtectedCache(db, ttl_s=60, stale_s=30, beta=0, clock=clock,
                                       background=jobs.append)
        cache.get("k")
        clock.t += 61                                     # soft-expired
        for _ in range(10):
            cache.get("k")
        self.assertEqual(len(jobs), 1)
        for job in jobs:
            job()
        self.assertEqual(db.calls, 2)                     # one initial load + one refresh

    def test_failed_background_refresh_allows_a_later_retry(self):
        clock, db, jobs = FakeClock(), CountingDB(), []
        cache = StampedeProtectedCache(db, ttl_s=60, stale_s=30, beta=0, clock=clock,
                                       background=jobs.append)
        cache.get("k")
        clock.t += 61
        db.fail = True
        cache.get("k")
        jobs[0]()                                         # refresh fails, stale kept
        self.assertEqual(cache.get("k"), "k-v1")
        self.assertEqual(len(jobs), 2)                    # the claim was released

    def test_rejected_background_job_does_not_leave_key_stuck(self):
        clock, db = FakeClock(), CountingDB()

        def rejecting_executor(job):
            raise RuntimeError("queue full")

        cache = StampedeProtectedCache(db, ttl_s=60, stale_s=30, beta=0, clock=clock,
                                       background=rejecting_executor)
        cache.get("k")
        clock.t += 61
        with self.assertRaises(RuntimeError):
            cache.get("k")
        clock.t += 30                                     # hard-expired now
        self.assertEqual(cache.get("k"), "k-v2")          # sync load not blocked

    def test_hard_expiry_loads_synchronously(self):
        clock, db = FakeClock(), CountingDB()
        cache = StampedeProtectedCache(db, ttl_s=60, stale_s=30, beta=0, clock=clock,
                                       background=lambda fn: None)
        cache.get("k")
        clock.t += 91
        self.assertEqual(cache.get("k"), "k-v2")

    def test_xfetch_never_early_when_far_from_expiry_and_ramps_up(self):
        clock = FakeClock()
        cache = StampedeProtectedCache(lambda k: 1, ttl_s=60, clock=clock,
                                       rng=random.Random(7))
        from solution import Entry
        e = Entry(1, fresh_until=clock.t + 60, expires_at=clock.t + 60, compute_s=1.0)
        rate = {}
        for remaining in (30, 5, 2, 0.5):
            hits = sum(cache.should_refresh_early(e, e.fresh_until - remaining)
                       for _ in range(10_000))
            rate[remaining] = hits / 10_000
        self.assertEqual(rate[30], 0.0)                   # e^-30: effectively never
        self.assertLess(rate[5], rate[2])
        self.assertLess(rate[2], rate[0.5])
        self.assertAlmostEqual(rate[0.5], 0.607, delta=0.02)   # e^-0.5
        self.assertAlmostEqual(rate[2], 0.135, delta=0.01)     # e^-2

    def test_different_keys_load_independently(self):
        db = CountingDB(delay=0.05)
        cache = StampedeProtectedCache(db, ttl_s=60, beta=0)
        threads = [threading.Thread(target=cache.get, args=(f"k{i % 5}",)) for i in range(25)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(db.calls, 5)


if __name__ == "__main__":
    unittest.main()
