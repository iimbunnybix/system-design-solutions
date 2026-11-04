import threading
import unittest

from solution import (FencedStorage, LeaseManager, NaiveStorage, NotLeader, StaleToken,
                      Worker)


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def gc_pause_scenario(storage_cls):
    """A leads, checks it is leader, then pauses (GC) past expiry; B takes over and
    writes; A resumes and finishes the write it had already decided to make."""
    clock = FakeClock()
    leases = LeaseManager(ttl_s=10, clock=clock)
    store = storage_cls()
    a, b = Worker("a", leases, store), Worker("b", leases, store)
    assert a.try_lead()
    assert a.believes_leader()            # check passes...
    clock.t += 15                         # ...then a 15 s stop-the-world pause
    assert b.try_lead()
    b.write("balance", 200)
    err = None
    try:
        a.write("balance", 100, check_first=False)   # check already happened pre-pause
    except StaleToken as e:
        err = e
    return store, err


class FencingTests(unittest.TestCase):
    def test_naive_storage_lets_zombie_leader_overwrite(self):
        store, err = gc_pause_scenario(NaiveStorage)
        self.assertIsNone(err)
        self.assertEqual(store.data["balance"], 100)     # stale leader won: corruption

    def test_fenced_storage_rejects_zombie_write(self):
        store, err = gc_pause_scenario(FencedStorage)
        self.assertIsInstance(err, StaleToken)
        self.assertEqual(store.data["balance"], 200)
        self.assertEqual(store.log, [("balance", 200)])

    def test_tokens_strictly_increase_across_grants(self):
        clock = FakeClock()
        lm = LeaseManager(5, clock)
        tokens = []
        for i in range(5):
            tokens.append(lm.acquire(f"n{i}").token)
            clock.t += 6
        self.assertEqual(tokens, [1, 2, 3, 4, 5])

    def test_only_one_holder_while_valid(self):
        clock = FakeClock()
        lm = LeaseManager(10, clock)
        self.assertIsNotNone(lm.acquire("a"))
        self.assertIsNone(lm.acquire("b"))
        clock.t += 9.99
        self.assertIsNone(lm.acquire("b"))
        clock.t += 0.02
        self.assertEqual(lm.acquire("b").holder, "b")

    def test_renew_keeps_token_and_extends(self):
        clock = FakeClock()
        lm = LeaseManager(10, clock)
        l1 = lm.acquire("a")
        clock.t += 8
        l2 = lm.renew(l1)
        self.assertEqual(l2.token, l1.token)
        clock.t += 8                              # 16 s after first grant, still valid
        self.assertEqual(lm.current().holder, "a")

    def test_renew_after_takeover_fails(self):
        clock = FakeClock()
        lm = LeaseManager(10, clock)
        old = lm.acquire("a")
        clock.t += 11
        lm.acquire("b")
        with self.assertRaises(NotLeader):
            lm.renew(old)

    def test_release_only_by_current_holder(self):
        clock = FakeClock()
        lm = LeaseManager(10, clock)
        old = lm.acquire("a")
        clock.t += 11
        new = lm.acquire("b")
        lm.release(old)                           # zombie release is a no-op
        self.assertEqual(lm.current(), new)
        lm.release(new)
        self.assertIsNone(lm.current())

    def test_local_check_with_margin(self):
        clock = FakeClock()
        lm = LeaseManager(10, clock)
        w = Worker("a", lm, FencedStorage(), safe_margin_s=2)
        w.try_lead()
        clock.t += 8.5
        with self.assertRaises(NotLeader):
            w.write("k", 1)

    def test_write_without_token_rejected(self):
        with self.assertRaises(StaleToken):
            FencedStorage().write("k", 1)

    def test_equal_token_allowed_for_same_leader(self):
        s = FencedStorage()
        s.write("k", 1, token=3)
        s.write("k", 2, token=3)
        self.assertEqual(s.data["k"], 2)

    def test_concurrent_writers_highest_token_wins(self):
        s = FencedStorage()
        barrier = threading.Barrier(20)

        def w(tok):
            barrier.wait()
            try:
                s.write("k", tok, token=tok)
            except StaleToken:
                pass

        ts = [threading.Thread(target=w, args=(i,)) for i in range(1, 21)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(s.max_token, 20)
        self.assertEqual(s.data["k"], 20)
        accepted = [v for _, v in s.log]                 # log is appended under the lock
        self.assertEqual(accepted, sorted(accepted))     # accepted tokens never go back


if __name__ == "__main__":
    unittest.main()
