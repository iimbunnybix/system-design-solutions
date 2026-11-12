"""Session affinity on a per-user cookie instead of the client's source IP.

The load balancer kept each user on one backend by hashing the source IP.
A large enterprise customer sends all of its thousands of users through one
corporate NAT, so they all share one public IP, hash to the same backend and
overload it while the other backends idle. The fix keys affinity on a session
cookie (one per user), placed on a consistent-hash ring so that adding or
removing a backend moves only a small share of sessions. A client that arrives
without a cookie is given a new session id, which spreads cookieless traffic.
"""
from __future__ import annotations

import bisect
import hashlib
import random
from dataclasses import dataclass

BACKEND_COUNT = 8
VIRTUAL_NODES_PER_BACKEND = 200        # more points on the ring = smoother spread
ENTERPRISE_USERS = 3_000               # all behind one corporate NAT address
OTHER_USERS = 1_000                    # each on their own home/mobile IP
REQUESTS_PER_USER = 10
SESSION_ID_BITS = 64
SESSION_COOKIE = "lb_session"


@dataclass
class Request:
    """What the balancer sees: the source address and the session cookie, if any."""
    source_ip: str
    session_cookie: str | None


def stable_hash(text: str) -> int:
    """64-bit hash that is the same in every balancer process (unlike hash())."""
    digest = hashlib.blake2b(text.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def backend_names(count: int = BACKEND_COUNT) -> list[str]:
    """backend-0, backend-1, ..."""
    return [f"backend-{index}" for index in range(count)]


def naive_route_by_source_ip(request: Request, backends: list[str]) -> str:
    """Original rule: hash the source IP. Everyone behind one NAT shares a backend."""
    return backends[stable_hash(request.source_ip) % len(backends)]


class HashRing:
    """Consistent-hash ring: each backend owns many points; a key goes clockwise."""

    def __init__(self, backends: list[str], virtual_nodes: int = VIRTUAL_NODES_PER_BACKEND) -> None:
        if not backends:
            raise ValueError("the ring needs at least one backend")
        self.virtual_nodes = virtual_nodes
        self.points: list[int] = []
        self.owner_of_point: dict[int, str] = {}
        for backend in backends:
            self.add(backend)

    def add(self, backend: str) -> None:
        """Place the backend's virtual nodes on the ring."""
        for replica in range(self.virtual_nodes):
            point = stable_hash(f"{backend}#{replica}")
            bisect.insort(self.points, point)
            self.owner_of_point[point] = backend

    def remove(self, backend: str) -> None:
        """Take a backend out; its keys fall to the next points clockwise."""
        self.points = [point for point in self.points if self.owner_of_point[point] != backend]
        for point in list(self.owner_of_point):
            if self.owner_of_point[point] == backend:
                del self.owner_of_point[point]

    def lookup(self, key: str) -> str:
        """Backend owning the first point at or after the key's hash (wrapping round)."""
        index = bisect.bisect_left(self.points, stable_hash(key))
        if index == len(self.points):
            index = 0
        return self.owner_of_point[self.points[index]]


class SessionAffinityBalancer:
    """Routes by session cookie on a hash ring; mints a session for cookieless clients."""

    def __init__(self, backends: list[str], rng: random.Random) -> None:
        self.ring = HashRing(backends)
        # Seeded here for repeatable tests; production would use the secrets module.
        self.rng = rng

    def route(self, request: Request) -> tuple[str, str | None]:
        """Return (backend, cookie to set). The cookie is None if the client sent one."""
        if request.session_cookie:
            return self.ring.lookup(request.session_cookie), None
        # Fallback: no cookie yet (first visit, or a client that drops cookies).
        # A fresh random id spreads such requests evenly instead of by IP.
        new_session = self.new_session_id()
        return self.ring.lookup(new_session), new_session

    def new_session_id(self) -> str:
        """Random, unguessable-in-production session id."""
        return format(self.rng.getrandbits(SESSION_ID_BITS), "016x")


@dataclass
class User:
    """One person browsing; keeps whatever session cookie the balancer sets."""
    source_ip: str
    cookie: str | None = None


def incident_users(rng: random.Random) -> list[User]:
    """Enterprise users all on one NAT IP, plus ordinary users on their own IPs."""
    nat_ip = "203.0.113.10"                # documentation range, stands in for the NAT
    users = [User(nat_ip) for _ in range(ENTERPRISE_USERS)]
    for _ in range(OTHER_USERS):
        users.append(User(f"198.51.{rng.randrange(256)}.{rng.randrange(1, 255)}"))
    return users


def load_with_naive(users: list[User], backends: list[str]) -> dict[str, int]:
    """Requests per backend when routing by source IP."""
    load = {backend: 0 for backend in backends}
    for _ in range(REQUESTS_PER_USER):
        for user in users:
            load[naive_route_by_source_ip(Request(user.source_ip, None), backends)] += 1
    return load


def load_with_sessions(users: list[User], balancer: SessionAffinityBalancer,
                       backends: list[str]) -> dict[str, int]:
    """Requests per backend when routing by session cookie; users keep their cookie."""
    load = {backend: 0 for backend in backends}
    for _ in range(REQUESTS_PER_USER):
        for user in users:
            backend, cookie_to_set = balancer.route(Request(user.source_ip, user.cookie))
            if cookie_to_set is not None:
                user.cookie = cookie_to_set
            load[backend] += 1
    return load


def fair_share(load: dict[str, int]) -> float:
    """Requests per backend if traffic were spread perfectly evenly."""
    return sum(load.values()) / len(load)


def share_of_sessions_moved_by_ring(sessions: list[str], backends: list[str], removed: str) -> float:
    """Share of sessions that change backend when one backend leaves the ring."""
    before = HashRing(backends)
    after = HashRing(backends)
    after.remove(removed)
    moved = 0
    for session in sessions:
        if before.lookup(session) != after.lookup(session):
            moved += 1
    return moved / len(sessions)


def share_of_sessions_moved_by_modulo(sessions: list[str], backends: list[str], removed: str) -> float:
    """Same, for plain hash % backend_count (why the ring is used instead)."""
    remaining = [backend for backend in backends if backend != removed]
    moved = 0
    for session in sessions:
        old = backends[stable_hash(session) % len(backends)]
        new = remaining[stable_hash(session) % len(remaining)]
        if old != new:
            moved += 1
    return moved / len(sessions)
