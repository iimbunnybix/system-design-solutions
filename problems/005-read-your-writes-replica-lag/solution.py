"""Read-your-writes and monotonic reads on top of asynchronously replicated databases.

The incident: to take load off the primary, all reads were moved to read
replicas behind a round-robin pool. Replication is asynchronous and usually
lags by milliseconds, but under write bursts some replicas fell seconds
behind. Users saved their profile, the page reloaded from a lagging replica
and showed the old value, so they saved again (and filed tickets). Some saw a
value appear and then "disappear" when the next read hit a staler replica.

``SessionConsistentRouter`` keeps the replicas for reads but gives each
client session two guarantees using log sequence numbers (LSNs):

* **read-your-writes**: after a write, reads only go to a replica that has
  applied at least that write's LSN;
* **monotonic reads**: a session never reads from a replica older than one it
  already read from.

The session carries a single number (``min_lsn``), e.g. in a cookie or token.
If no replica is fresh enough, the read goes to the primary.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any


class Primary:
    """Accepts writes and appends them to a replication log."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.log: list[tuple[int, str, Any]] = []
        self.lsn = 0
        self.reads = 0

    def write(self, key: str, value: Any) -> int:
        self.lsn += 1
        self.data[key] = value
        self.log.append((self.lsn, key, value))
        return self.lsn

    def read(self, key: str) -> Any:
        self.reads += 1
        return self.data.get(key)


class Replica:
    """Applies the primary's log asynchronously; ``catch_up`` simulates lag."""

    def __init__(self, primary: Primary, name: str) -> None:
        self.primary, self.name = primary, name
        self.data: dict[str, Any] = {}
        self.applied_lsn = 0
        self.reads = 0

    def catch_up(self, upto: int | None = None) -> None:
        target = self.primary.lsn if upto is None else min(upto, self.primary.lsn)
        for lsn, key, value in self.primary.log[self.applied_lsn:target]:
            self.data[key] = value
            self.applied_lsn = lsn

    def read(self, key: str) -> Any:
        self.reads += 1
        return self.data.get(key)


@dataclass
class Session:
    """Client-side consistency token: highest LSN this client depends on."""
    min_lsn: int = 0


class NaiveRouter:
    """The original setup: writes to primary, reads round-robin over replicas."""

    def __init__(self, primary: Primary, replicas: list[Replica]) -> None:
        self.primary = primary
        self._rr = itertools.cycle(replicas)

    def write(self, session: Session, key: str, value: Any) -> None:
        self.primary.write(key, value)

    def read(self, session: Session, key: str) -> Any:
        return next(self._rr).read(key)


@dataclass
class RouterStats:
    replica_reads: int = 0
    primary_fallbacks: int = 0
    by_replica: dict[str, int] = field(default_factory=dict)


class SessionConsistentRouter:
    """Route reads to a replica that is at least as fresh as the session."""

    def __init__(self, primary: Primary, replicas: list[Replica]) -> None:
        if not replicas:
            raise ValueError("need at least one replica")
        self.primary, self.replicas = primary, replicas
        self._rr = itertools.count()
        self.stats = RouterStats()

    def write(self, session: Session, key: str, value: Any) -> int:
        lsn = self.primary.write(key, value)
        session.min_lsn = max(session.min_lsn, lsn)
        return lsn

    def read(self, session: Session, key: str) -> Any:
        """Read ``key`` without going back in time for this session."""
        start = next(self._rr)
        n = len(self.replicas)
        # Rotate the starting point so fresh replicas share the load.
        for i in range(n):
            r = self.replicas[(start + i) % n]
            if r.applied_lsn >= session.min_lsn:
                session.min_lsn = max(session.min_lsn, r.applied_lsn)   # monotonic reads
                self.stats.replica_reads += 1
                self.stats.by_replica[r.name] = self.stats.by_replica.get(r.name, 0) + 1
                return r.read(key)
        self.stats.primary_fallbacks += 1
        session.min_lsn = max(session.min_lsn, self.primary.lsn)
        return self.primary.read(key)
