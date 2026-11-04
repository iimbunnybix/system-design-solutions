import random
import unittest

from solution import (NaiveRouter, Primary, Replica, Session,
                      SessionConsistentRouter)


def cluster(n=3):
    p = Primary()
    return p, [Replica(p, f"r{i}") for i in range(n)]


class ReproduceStaleReads(unittest.TestCase):
    def test_user_does_not_see_own_write(self):
        p, reps = cluster()
        router, s = NaiveRouter(p, reps), Session()
        router.write(s, "user:1:name", "Alice")
        for r in reps:
            r.catch_up()
        router.write(s, "user:1:name", "Alicia")       # replicas lag now
        self.assertEqual(router.read(s, "user:1:name"), "Alice")

    def test_value_appears_then_disappears(self):
        p, reps = cluster(2)
        router, s = NaiveRouter(p, reps), Session()
        router.write(s, "k", "new")
        reps[0].catch_up()                             # r0 fresh, r1 stale
        seen = [router.read(s, "k"), router.read(s, "k")]
        self.assertEqual(seen, ["new", None])          # time went backwards


class SessionConsistency(unittest.TestCase):
    def test_read_your_writes_falls_back_to_primary_when_all_lag(self):
        p, reps = cluster()
        router, s = SessionConsistentRouter(p, reps), Session()
        router.write(s, "user:1:name", "Alicia")
        self.assertEqual(router.read(s, "user:1:name"), "Alicia")
        self.assertEqual(router.stats.primary_fallbacks, 1)

    def test_uses_fresh_replica_when_available(self):
        p, reps = cluster()
        router, s = SessionConsistentRouter(p, reps), Session()
        router.write(s, "k", "v1")
        reps[2].catch_up()
        for _ in range(5):
            self.assertEqual(router.read(s, "k"), "v1")
        self.assertEqual(router.stats.by_replica, {"r2": 5})
        self.assertEqual(p.reads, 0)

    def test_monotonic_reads(self):
        p, reps = cluster(2)
        router, s = SessionConsistentRouter(p, reps), Session()
        other = Session()
        router.write(other, "k", "new")                # someone else wrote
        reps[0].catch_up()
        seen = [router.read(s, "k") for _ in range(6)]
        first_new = seen.index("new") if "new" in seen else len(seen)
        self.assertTrue(all(v == "new" for v in seen[first_new:]))

    def test_other_sessions_are_not_forced_to_primary(self):
        p, reps = cluster()
        router = SessionConsistentRouter(p, reps)
        writer, reader = Session(), Session()
        router.write(writer, "k", "v")
        router.read(reader, "k")                       # no dependency on the write
        self.assertEqual(router.stats.primary_fallbacks, 0)

    def test_session_token_is_portable_across_routers(self):
        # Two app servers share the cluster; the client carries min_lsn.
        p, reps = cluster()
        app1, app2 = SessionConsistentRouter(p, reps), SessionConsistentRouter(p, reps)
        s = Session()
        app1.write(s, "cart", ["book"])
        self.assertEqual(app2.read(s, "cart"), ["book"])

    def test_randomized_workload_never_violates_guarantees(self):
        rng = random.Random(1234)
        p, reps = cluster(4)
        router = SessionConsistentRouter(p, reps)
        sessions = [Session() for _ in range(20)]
        last_written = {}                              # (session, key) -> value
        last_seen_lsn = [0] * 20
        for _ in range(5000):
            i = rng.randrange(20)
            s, key = sessions[i], f"k{i % 5}"
            if rng.random() < 0.3:
                v = rng.random()
                router.write(s, key, v)
                last_written[(i, key)] = v
            else:
                v = router.read(s, key)
                mine = last_written.get((i, key))
                if mine is not None and p.data[key] == mine:
                    self.assertEqual(v, mine)          # read-your-writes
                self.assertGreaterEqual(s.min_lsn, last_seen_lsn[i])
                last_seen_lsn[i] = s.min_lsn
            r = rng.choice(reps)                       # replicas lag randomly
            r.catch_up(r.applied_lsn + rng.randint(0, 3))
        total = router.stats.replica_reads + router.stats.primary_fallbacks
        self.assertGreater(router.stats.replica_reads / total, 0.5)

    def test_requires_replicas(self):
        with self.assertRaises(ValueError):
            SessionConsistentRouter(Primary(), [])


if __name__ == "__main__":
    unittest.main()
