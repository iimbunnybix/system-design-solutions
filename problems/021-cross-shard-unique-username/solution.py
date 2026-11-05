"""Keep usernames unique when users are sharded by user_id.

Users live on the shard chosen by ``user_id % shard_count``, and each shard has
its own UNIQUE index on username. That index only sees one shard, so two
sign-ups for "alice" that land on different shards both succeed. This module
keeps that original flow and adds a global username index that is itself
partitioned by a hash of the *username*: every claim for one name goes to the
same partition, which can therefore enforce uniqueness. Sign-up becomes
reserve (index) -> create (user shard) -> confirm (index), and a failed create
releases the reservation.
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from typing import Callable

RESERVATION_TTL_SECONDS = 60.0   # a PENDING claim older than this may be reclaimed
PENDING = "pending"
CONFIRMED = "confirmed"


class UsernameTaken(Exception):
    """Raised when another user already holds (or is creating) this username."""


class UserShard:
    """One user database shard with its own *local* unique username index."""

    def __init__(self, shard_id: int) -> None:
        self.shard_id = shard_id
        self.users: dict[int, str] = {}           # user_id -> username
        self.local_usernames: set[str] = set()    # unique only within this shard
        self.fail_next_insert = False             # lets tests simulate a DB error
        self._lock = threading.Lock()

    def insert_user(self, user_id: int, username: str) -> None:
        """Insert a user row, enforcing the shard-local unique index."""
        with self._lock:
            if self.fail_next_insert:
                self.fail_next_insert = False
                raise ConnectionError(f"shard {self.shard_id} unavailable")
            if username in self.local_usernames:
                raise UsernameTaken(username)
            self.users[user_id] = username
            self.local_usernames.add(username)

    def has_user_with_name(self, user_id: int, username: str) -> bool:
        """True if this shard already stores ``user_id`` with ``username``."""
        with self._lock:
            return self.users.get(user_id) == username


def shard_for_user(shards: list[UserShard], user_id: int) -> UserShard:
    """Users are placed by user_id, which is why the local index is not enough."""
    return shards[user_id % len(shards)]


def normalize_username(username: str) -> str:
    """Compare names case-insensitively and ignore surrounding spaces."""
    return username.strip().casefold()


def naive_register(shards: list[UserShard], user_id: int, username: str) -> None:
    """Original sign-up: rely on the user shard's local unique index only."""
    shard_for_user(shards, user_id).insert_user(user_id, normalize_username(username))


@dataclass
class Reservation:
    user_id: int
    state: str
    reserved_at: float


class IndexPartition:
    """One partition of the global username index; owns a slice of all names."""

    def __init__(self) -> None:
        self.claims: dict[str, Reservation] = {}
        self._lock = threading.Lock()

    def reserve(self, name: str, user_id: int, now: float,
                owner_finished: Callable[[int, str], bool]) -> None:
        """Claim ``name`` for ``user_id`` or raise UsernameTaken."""
        with self._lock:
            existing = self.claims.get(name)
            if existing is not None and existing.user_id != user_id:
                if not self._is_reclaimable(existing, name, now, owner_finished):
                    raise UsernameTaken(name)
            # Same user retrying keeps its claim; otherwise start a fresh one.
            if existing is None or existing.user_id != user_id:
                self.claims[name] = Reservation(user_id, PENDING, now)

    def _is_reclaimable(self, claim: Reservation, name: str, now: float,
                        owner_finished: Callable[[int, str], bool]) -> bool:
        """An old PENDING claim is free only if its owner never created the user."""
        if claim.state == CONFIRMED:
            return False
        if now - claim.reserved_at < RESERVATION_TTL_SECONDS:
            return False
        # The owner may have crashed *after* creating the user but before
        # confirming. Check the user shard so we never hand out a live name.
        if owner_finished(claim.user_id, name):
            claim.state = CONFIRMED
            return False
        return True

    def confirm(self, name: str, user_id: int) -> None:
        """Mark the claim permanent once the user row exists."""
        with self._lock:
            claim = self.claims.get(name)
            if claim is None or claim.user_id != user_id:
                raise UsernameTaken(name)
            claim.state = CONFIRMED

    def release(self, name: str, user_id: int) -> None:
        """Drop a claim we own, so a failed sign-up does not burn the name."""
        with self._lock:
            claim = self.claims.get(name)
            if claim is not None and claim.user_id == user_id and claim.state == PENDING:
                del self.claims[name]


class UsernameIndex:
    """Global username index, partitioned by a hash of the normalized name."""

    def __init__(self, partition_count: int) -> None:
        self.partitions = [IndexPartition() for _ in range(partition_count)]

    def partition_for(self, name: str) -> IndexPartition:
        # A stable hash (not Python's per-process randomized hash()) so every
        # app server routes the same name to the same partition.
        digest = hashlib.sha256(name.encode("utf-8")).digest()
        return self.partitions[int.from_bytes(digest[:8], "big") % len(self.partitions)]


def register(index: UsernameIndex, shards: list[UserShard], user_id: int,
             username: str, now: float) -> None:
    """Fixed sign-up: reserve globally, create on the user shard, then confirm."""
    name = normalize_username(username)
    partition = index.partition_for(name)

    def owner_finished(owner_id: int, claimed_name: str) -> bool:
        return shard_for_user(shards, owner_id).has_user_with_name(owner_id, claimed_name)

    partition.reserve(name, user_id, now, owner_finished)
    try:
        shard_for_user(shards, user_id).insert_user(user_id, name)
    except BaseException:
        partition.release(name, user_id)
        raise
    partition.confirm(name, user_id)
