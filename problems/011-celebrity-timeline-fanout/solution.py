"""Home timelines that survive celebrity posts: hybrid push/pull fan-out.

``PushFeed`` is the original design: when a user posts, the post id is written
into the home-timeline inbox of every follower ("fan-out on write"). Reads are
a single inbox lookup, but one post from an account with millions of followers
becomes millions of writes, and the fan-out queue falls behind for everyone.

``HybridFeed`` is the fix used by large social networks:

* authors with fewer than ``celebrity_threshold`` followers are still pushed;
* posts by celebrities are **not** fanned out; they live only in the author's
  own outbox, and are **pulled** and merged into a follower's timeline at
  read time (a user follows few celebrities, so the merge is small);
* timelines are bounded (``inbox_limit``) and merged newest-first by
  ``(timestamp, post_id)``, deduplicated by post id.

Both classes count storage operations so the cost difference is measurable.
"""
from __future__ import annotations

import heapq
import itertools
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import DefaultDict, Deque, Dict, Iterable, List, Set, Tuple


@dataclass(frozen=True, order=True)
class Post:
    ts: float
    post_id: int
    author: str


class _Base:
    def __init__(self, inbox_limit: int = 800) -> None:
        self.followers: DefaultDict[str, Set[str]] = defaultdict(set)
        self.following: DefaultDict[str, Set[str]] = defaultdict(set)
        self.inbox: DefaultDict[str, Deque[Post]] = defaultdict(deque)   # newest at left
        self.outbox: DefaultDict[str, Deque[Post]] = defaultdict(deque)
        self.inbox_limit = inbox_limit
        self.writes = 0          # inbox/outbox entries written
        self.reads = 0           # inbox/outbox lists fetched
        self._ids = itertools.count(1)

    def follow(self, user: str, author: str) -> None:
        self.followers[author].add(user)
        self.following[user].add(author)

    def _push(self, box: Deque[Post], post: Post) -> None:
        """Insert ``post`` keeping ``box`` sorted newest-first by (ts, post_id).

        Fan-out is asynchronous, so posts can arrive out of timestamp order
        (queue delay, retries, clock skew). The timeline's k-way merge needs
        every source sorted, so we insert in order instead of always at the
        front. A fresh post is usually the newest, so the scan stops at once.
        """
        new_key = (post.ts, post.post_id)
        position = 0
        while position < len(box) and (box[position].ts, box[position].post_id) > new_key:
            position += 1
        box.insert(position, post)
        if len(box) > self.inbox_limit:
            box.pop()
        self.writes += 1

    def _new_post(self, author: str, ts: float) -> Post:
        post = Post(ts, next(self._ids), author)
        self._push(self.outbox[author], post)
        return post


class PushFeed(_Base):
    """Fan-out on write for everyone."""

    def post(self, author: str, ts: float) -> Post:
        post = self._new_post(author, ts)
        for f in self.followers[author]:
            self._push(self.inbox[f], post)
        return post

    def timeline(self, user: str, limit: int = 20) -> List[Post]:
        self.reads += 1
        return list(itertools.islice(self.inbox[user], limit))


class HybridFeed(_Base):
    """Push for regular authors, pull at read time for celebrities."""

    def __init__(self, celebrity_threshold: int = 10_000, inbox_limit: int = 800) -> None:
        super().__init__(inbox_limit)
        self.celebrity_threshold = celebrity_threshold
        # Authors who have at least one post that was NOT pushed to inboxes.
        # Their outbox must be pulled at read time for as long as those posts
        # exist, even if the author later drops below the celebrity threshold
        # (deciding from the *current* follower count made old posts vanish).
        self.authors_with_unpushed_posts: Set[str] = set()

    def is_celebrity(self, author: str) -> bool:
        return len(self.followers[author]) >= self.celebrity_threshold

    def post(self, author: str, ts: float) -> Post:
        post = self._new_post(author, ts)
        if self.is_celebrity(author):
            self.authors_with_unpushed_posts.add(author)
        else:
            for f in self.followers[author]:
                self._push(self.inbox[f], post)
        return post

    def timeline(self, user: str, limit: int = 20) -> List[Post]:
        """Merge the user's inbox with the outboxes of followed authors that have un-pushed posts.

        Each source is already sorted newest-first, so a k-way merge stops after
        ``limit`` distinct posts. Posts that were pushed before an author became
        a celebrity can be in both the inbox and the outbox; dedupe by id.
        """
        sources: List[Iterable[Post]] = [self.inbox[user]]
        self.reads += 1
        for author in self.following[user]:
            if author in self.authors_with_unpushed_posts:
                sources.append(self.outbox[author])
                self.reads += 1
        merged = heapq.merge(*sources, key=lambda p: (p.ts, p.post_id), reverse=True)
        out: List[Post] = []
        seen: Set[int] = set()
        for p in merged:
            if p.post_id in seen:
                continue
            if p.author not in self.following[user] and p.author != user:
                continue          # unfollowed since it was pushed
            seen.add(p.post_id)
            out.append(p)
            if len(out) == limit:
                break
        return out

    def unfollow(self, user: str, author: str) -> None:
        self.followers[author].discard(user)
        self.following[user].discard(author)
