import random
import unittest

from solution import HybridFeed, PushFeed

CELEB_FOLLOWERS = 50_000


def build(feed_cls, **kw):
    feed = feed_cls(**kw)
    for i in range(CELEB_FOLLOWERS):
        feed.follow(f"u{i}", "celebrity")
    for i in range(0, 1_000):
        feed.follow(f"u{i}", "friend")          # a regular author with 1,000 followers
    return feed


def reference_timeline(feed, user, limit):
    posts = [p for a in feed.following[user] for p in feed.outbox[a]]
    return sorted(posts, key=lambda p: (p.ts, p.post_id), reverse=True)[:limit]


class ReproduceFanOutCost(unittest.TestCase):
    def test_one_celebrity_post_costs_one_write_per_follower(self):
        feed = build(PushFeed)
        before = feed.writes
        feed.post("celebrity", ts=1.0)
        self.assertEqual(feed.writes - before, CELEB_FOLLOWERS + 1)

    def test_ten_celebrity_posts_dwarf_regular_traffic(self):
        feed = build(PushFeed)
        before = feed.writes
        for t in range(10):
            feed.post("celebrity", ts=t)
        celeb_writes = feed.writes - before
        before = feed.writes
        for t in range(10):
            feed.post("friend", ts=t)
        self.assertEqual(celeb_writes, 500_010)
        self.assertEqual(feed.writes - before, 10_010)


class HybridFix(unittest.TestCase):
    def test_celebrity_post_is_a_single_write(self):
        feed = build(HybridFeed)
        before = feed.writes
        feed.post("celebrity", ts=1.0)
        self.assertEqual(feed.writes - before, 1)

    def test_regular_authors_still_pushed(self):
        feed = build(HybridFeed)
        before = feed.writes
        feed.post("friend", ts=1.0)
        self.assertEqual(feed.writes - before, 1_001)

    def test_timeline_merges_pushed_and_pulled_posts_in_order(self):
        feed = build(HybridFeed)
        feed.post("friend", ts=1.0)
        feed.post("celebrity", ts=2.0)
        feed.post("friend", ts=3.0)
        tl = feed.timeline("u5")
        self.assertEqual([(p.author, p.ts) for p in tl],
                         [("friend", 3.0), ("celebrity", 2.0), ("friend", 1.0)])
        self.assertEqual(feed.reads, 2)           # own inbox + one celebrity outbox

    def test_matches_reference_on_random_graph(self):
        rng = random.Random(5)
        feed = HybridFeed(celebrity_threshold=40, inbox_limit=10_000)
        users = [f"u{i}" for i in range(200)]
        for u in users:
            for a in rng.sample(users, 15):
                if a != u:
                    feed.follow(u, a)
        for celeb in ("c1", "c2", "c3"):
            for u in rng.sample(users, 120):
                feed.follow(u, celeb)
        authors = users + ["c1", "c2", "c3"]
        for t in range(2_000):
            feed.post(rng.choice(authors), ts=float(t // 3))   # ties on ts are common
        for u in rng.sample(users, 30):
            self.assertEqual(feed.timeline(u, 50), reference_timeline(feed, u, 50))

    def test_author_crossing_threshold_does_not_duplicate_posts(self):
        feed = HybridFeed(celebrity_threshold=3)
        feed.follow("a", "rising")
        feed.follow("b", "rising")
        feed.post("rising", ts=1.0)               # pushed (2 followers)
        feed.follow("c", "rising")                # now a celebrity
        feed.post("rising", ts=2.0)               # pulled
        tl = feed.timeline("a")
        self.assertEqual([p.ts for p in tl], [2.0, 1.0])
        self.assertEqual(len({p.post_id for p in tl}), 2)

    def test_celebrity_posts_survive_dropping_below_threshold(self):
        # Issue #14: one unfollow took the author under the threshold and
        # every remaining follower lost the posts that were never pushed.
        feed = HybridFeed(celebrity_threshold=3)
        for user in ("u1", "u2", "u3"):
            feed.follow(user, "star")
        post = feed.post("star", 1.0)
        feed.unfollow("u3", "star")
        self.assertEqual(feed.timeline("u1"), [post])
        self.assertEqual(feed.timeline("u3"), [])

    def test_posts_after_dropping_below_threshold_are_pushed_again(self):
        feed = HybridFeed(celebrity_threshold=2)
        feed.follow("u1", "star")
        feed.follow("u2", "star")
        pulled = feed.post("star", 1.0)
        feed.unfollow("u2", "star")
        pushed = feed.post("star", 2.0)
        self.assertEqual(list(feed.inbox["u1"]), [pushed])
        self.assertEqual(feed.timeline("u1"), [pushed, pulled])

    def test_unfollow_hides_previously_pushed_posts(self):
        feed = HybridFeed(celebrity_threshold=100)
        feed.follow("a", "x")
        feed.post("x", ts=1.0)
        feed.unfollow("a", "x")
        self.assertEqual(feed.timeline("a"), [])

    def test_limit_and_bounded_inbox(self):
        feed = HybridFeed(celebrity_threshold=100, inbox_limit=5)
        feed.follow("a", "x")
        for t in range(10):
            feed.post("x", ts=float(t))
        self.assertEqual(len(feed.inbox["a"]), 5)
        self.assertEqual([p.ts for p in feed.timeline("a", limit=3)], [9.0, 8.0, 7.0])

    def test_late_fan_out_keeps_timeline_newest_first(self):
        feed = HybridFeed(celebrity_threshold=2)
        for author in ("friendA", "friendB", "star"):
            feed.follow("u1", author)
        feed.follow("u2", "star")
        feed.post("friendA", ts=5.0)
        feed.post("friendB", ts=3.0)   # fanned out later, older timestamp
        feed.post("star", ts=4.0)
        self.assertEqual([p.ts for p in feed.timeline("u1", limit=2)], [5.0, 4.0])
        self.assertEqual([p.ts for p in feed.timeline("u1", limit=3)], [5.0, 4.0, 3.0])

    def test_bounded_inbox_drops_the_oldest_post_when_arrivals_are_out_of_order(self):
        feed = PushFeed(inbox_limit=2)
        feed.follow("u1", "a")
        feed.post("a", ts=5.0)
        feed.post("a", ts=7.0)
        feed.post("a", ts=6.0)
        self.assertEqual([p.ts for p in feed.timeline("u1")], [7.0, 6.0])

    def test_empty_timeline(self):
        self.assertEqual(HybridFeed().timeline("nobody"), [])


if __name__ == "__main__":
    unittest.main()
