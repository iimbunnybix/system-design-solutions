"""Leader election with leases + fencing tokens.

A lease alone cannot guarantee a single writer: the old leader may be
paused (GC, VM migration, network partition) past its lease expiry, a new
leader is elected, and when the old one resumes it still *believes* it is
leader and writes. Fencing fixes this at the resource: every lease grant
comes with a strictly increasing token, and storage rejects any write whose
token is lower than the highest it has seen.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


class NotLeader(Exception):
    pass


class StaleToken(Exception):
    pass


@dataclass(frozen=True)
class Lease:
    holder: str
    token: int          # fencing token, strictly increasing across grants
    expires_at: float


class LeaseManager:
    """Single-key lease service (what etcd/ZooKeeper/Consul provide).

    In etcd the natural fencing token is the key's revision / lease ID; in
    ZooKeeper the zxid or sequential znode number.
    """

    def __init__(self, ttl_s: float, clock: Callable[[], float] = time.monotonic):
        if ttl_s <= 0:
            raise ValueError("ttl must be positive")
        self.ttl = ttl_s
        self.clock = clock
        self._lease: Lease | None = None
        self._next_token = 1
        self._lock = threading.Lock()

    def acquire(self, holder: str) -> Lease | None:
        """Grant the lease if free or expired. Re-acquire by the holder = renew."""
        with self._lock:
            now = self.clock()
            cur = self._lease
            if cur and cur.expires_at > now and cur.holder != holder:
                return None
            if cur and cur.holder == holder and cur.expires_at > now:
                self._lease = Lease(holder, cur.token, now + self.ttl)   # renew, same token
            else:
                self._lease = Lease(holder, self._next_token, now + self.ttl)
                self._next_token += 1
            return self._lease

    def renew(self, lease: Lease) -> Lease:
        with self._lock:
            now = self.clock()
            cur = self._lease
            if cur is None or cur.token != lease.token or cur.expires_at <= now:
                raise NotLeader("lease lost")
            self._lease = Lease(cur.holder, cur.token, now + self.ttl)
            return self._lease

    def release(self, lease: Lease) -> None:
        with self._lock:
            if self._lease and self._lease.token == lease.token:
                self._lease = None          # only the current holder can release

    def current(self) -> Lease | None:
        with self._lock:
            now = self.clock()
            return self._lease if self._lease and self._lease.expires_at > now else None


class NaiveStorage:
    """The original resource: trusts whoever calls it."""

    def __init__(self):
        self.data: dict[str, Any] = {}
        self.log: list[tuple[str, Any]] = []

    def write(self, key: str, value: Any, token: int | None = None) -> None:
        self.data[key] = value
        self.log.append((key, value))


class FencedStorage(NaiveStorage):
    """Rejects writes carrying a token older than the newest one seen.

    The check-and-write must be atomic at the resource (here a lock; in a DB a
    conditional UPDATE ... WHERE fence_token <= :token).
    """

    def __init__(self):
        super().__init__()
        self.max_token = 0
        self._lock = threading.Lock()

    def write(self, key: str, value: Any, token: int | None = None) -> None:
        if token is None:
            raise StaleToken("write without fencing token")
        with self._lock:
            if token < self.max_token:
                raise StaleToken(f"token {token} < {self.max_token}")
            self.max_token = token
            super().write(key, value)


class Worker:
    """A leader-elected worker. ``safe_margin`` stops work a bit before expiry
    (clock drift), but correctness comes from the fencing token, not timing."""

    def __init__(self, name: str, leases: LeaseManager, storage: NaiveStorage,
                 safe_margin_s: float = 0.0):
        self.name, self.leases, self.storage = name, leases, storage
        self.margin = safe_margin_s
        self.lease: Lease | None = None

    def try_lead(self) -> bool:
        self.lease = self.leases.acquire(self.name)
        return self.lease is not None

    def believes_leader(self) -> bool:
        # Uses the *local* view of the lease; after a pause this view is stale.
        return self.lease is not None and \
            self.leases.clock() < self.lease.expires_at - self.margin

    def write(self, key: str, value: Any, check_first: bool = True) -> None:
        if self.lease is None:
            raise NotLeader(self.name)
        if check_first and not self.believes_leader():
            raise NotLeader(f"{self.name}: lease expired locally")
        self.storage.write(key, value, token=self.lease.token)
