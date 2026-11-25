"""A primary database crash lost orders that customers had already been told were placed.

The incident: the orders primary acknowledged each write as soon as it was
on its own disk and shipped it to two replicas asynchronously. When the
primary's host died, the runbook promoted the cross-region disaster-recovery
replica, which was 80 ms behind. Every order acknowledged in those last
80 ms existed only on the dead primary and was lost.

The fix has two parts. Semi-synchronous replication: acknowledge a write
only after at least one replica has confirmed it, so every acknowledged
write survives the loss of the primary. Most-caught-up promotion: promote
the replica with the highest log position, so the replica that confirmed
those writes is the one that takes over.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

WRITE_INTERVAL_MS = 1                # 1,000 orders per second, one per millisecond
CRASH_AT_MS = 10_000                 # the primary's host dies here
SAME_REGION_LAG_MS = 2               # replica in another zone of the same region
CROSS_REGION_LAG_MS = 80             # disaster-recovery replica in another region


@dataclass(frozen=True)
class Replica:
    """A replica that receives each write ``lag_ms`` after the primary wrote it.

    ``lag_ms`` is the time for the write to reach the replica and for its
    confirmation to come back, which is how long a semi-sync ack waits.
    """
    name: str
    lag_ms: int
    reachable: bool = True

    def has_write(self, written_at_ms: int, now_ms: int) -> bool:
        """True if the write from ``written_at_ms`` reached this replica by ``now_ms``."""
        return self.reachable and written_at_ms + self.lag_ms <= now_ms


def default_replicas() -> list[Replica]:
    """The incident's topology; the runbook lists the DR replica first."""
    return [Replica("dr-replica", CROSS_REGION_LAG_MS),
            Replica("zone-b-replica", SAME_REGION_LAG_MS)]


# An ack policy returns when the client hears "success", or None if never.
AckPolicy = Callable[[int, list[Replica]], "int | None"]
PromotionPolicy = Callable[[list[Replica], int], Replica]


def write_times_before_crash(crash_at_ms: int = CRASH_AT_MS) -> list[int]:
    """Times at which the primary accepted a write before it died."""
    return list(range(0, crash_at_ms, WRITE_INTERVAL_MS))


# ---------------------------------------------------------------- original
def async_ack_time(written_at_ms: int, replicas: list[Replica]) -> int | None:
    """Original: the client is told "order placed" as soon as the primary wrote it.

    ``replicas`` is unused; it keeps the same signature as the fixed policy.
    """
    return written_at_ms


def promote_first_listed(replicas: list[Replica], crash_at_ms: int) -> Replica:
    """Original runbook: promote whichever replica is listed first."""
    return replicas[0]


# ---------------------------------------------------------------- the fix
def semi_sync_ack_time(written_at_ms: int, replicas: list[Replica]) -> int | None:
    """Acknowledge when the fastest reachable replica has confirmed the write.

    Returns None when no replica can confirm: the write is refused rather
    than acknowledged with only one copy.
    """
    fastest_lag = None
    for replica in replicas:
        if replica.reachable and (fastest_lag is None or replica.lag_ms < fastest_lag):
            fastest_lag = replica.lag_ms
    if fastest_lag is None:
        return None
    return written_at_ms + fastest_lag


def log_position(replica: Replica, crash_at_ms: int) -> int:
    """How many writes the replica holds when the primary dies."""
    count = 0
    for written_at in write_times_before_crash(crash_at_ms):
        if replica.has_write(written_at, crash_at_ms):
            count += 1
    return count


def promote_most_caught_up(replicas: list[Replica], crash_at_ms: int) -> Replica:
    """Promote the reachable replica with the highest log position."""
    candidates = [replica for replica in replicas if replica.reachable]
    best = candidates[0]
    for replica in candidates[1:]:
        if log_position(replica, crash_at_ms) > log_position(best, crash_at_ms):
            best = replica
    return best


# ---------------------------------------------------------------- simulation
def acknowledged_writes(ack_time: AckPolicy, replicas: list[Replica], crash_at_ms: int) -> list[int]:
    """Writes the client was told succeeded before the primary died.

    WHY "before the crash": an ack is sent by the primary, so a write whose
    ack was due after the crash was never acknowledged; the client saw a
    timeout and retries it against the new primary.
    """
    acked = []
    for written_at in write_times_before_crash(crash_at_ms):
        acked_at = ack_time(written_at, replicas)
        if acked_at is not None and acked_at <= crash_at_ms:
            acked.append(written_at)
    return acked


def lost_acknowledged_writes(ack_time: AckPolicy, promote: PromotionPolicy, replicas: list[Replica],
                             crash_at_ms: int = CRASH_AT_MS) -> int:
    """Acknowledged writes missing from the promoted replica after failover."""
    new_primary = promote(replicas, crash_at_ms)
    lost = 0
    for written_at in acknowledged_writes(ack_time, replicas, crash_at_ms):
        if not new_primary.has_write(written_at, crash_at_ms):
            lost += 1
    return lost
