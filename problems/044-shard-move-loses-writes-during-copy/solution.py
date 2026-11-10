"""Moving a shard to a new server without losing the writes made during the copy.

The original rebalancer copied a busy shard to a new server key by key and
then switched routing to the new server. The shard kept taking writes during
the copy, and any write to a key that had already been copied (or to a key
created after the copy started) existed only on the old server, so it vanished
at the switch.

The fix is the snapshot-plus-change-log pattern: remember the source's
change-log position before copying, copy, then replay every change logged
since that position until the target is nearly caught up, freeze writes for a
moment, replay the last few changes and switch. Nothing acknowledged is lost
and writes are refused only during the short freeze.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field

COPY_BATCH_KEYS = 500          # keys copied per step of the bulk copy
REPLAY_BATCH_ENTRIES = 100     # log entries replayed per catch-up step
CUTOVER_MAX_LAG = 50           # freeze writes only when this few changes remain
MAX_CATCH_UP_STEPS = 1_000     # give up if writes outrun the replay forever
NEW_KEY_SHARE = 0.1            # share of simulated writes that create a new key


class ShardFrozen(Exception):
    """The shard is in its cutover freeze; the client should retry shortly."""


class CatchUpTooSlow(Exception):
    """Writes arrive faster than the target can replay them."""


@dataclass
class ShardServer:
    """One server's copy of the shard plus its change log."""

    name: str
    data: dict[str, int] = field(default_factory=dict)
    change_log: list[tuple[str, int]] = field(default_factory=list)

    def apply(self, key: str, value: int) -> None:
        """Store a write and append it to the change log."""
        self.data[key] = value
        self.change_log.append((key, value))

    def log_position(self) -> int:
        """Number of changes logged so far; replay starts after this point."""
        return len(self.change_log)


@dataclass
class Router:
    """Sends writes for the shard to its current owner."""

    owner: ShardServer
    frozen: bool = False

    def write(self, key: str, value: int) -> None:
        """Route one client write, refusing it during the cutover freeze."""
        if self.frozen:
            raise ShardFrozen(key)
        self.owner.apply(key, value)


class LiveTraffic:
    """Seeded client writes; remembers what was acknowledged to clients."""

    def __init__(self, router: Router, existing_keys: int, writes_per_step: int, seed: int):
        self.router = router
        self.writes_per_step = writes_per_step
        self.rng = random.Random(seed)
        self.next_new_key = existing_keys
        self.existing_keys = existing_keys
        self.acknowledged: dict[str, int] = {}
        self.refused = 0

    def step(self) -> None:
        """Send one step of writes: mostly updates, some brand-new keys."""
        for _ in range(self.writes_per_step):
            if self.rng.random() < NEW_KEY_SHARE:
                key = f"user:{self.next_new_key}"
                self.next_new_key += 1
            else:
                key = f"user:{self.rng.randrange(self.existing_keys)}"
            value = self.rng.randrange(1_000_000)
            try:
                self.router.write(key, value)
            except ShardFrozen:
                self.refused += 1
                continue
            self.acknowledged[key] = value


def copy_in_batches(source: ShardServer, target: ShardServer, traffic: LiveTraffic) -> None:
    """Bulk-copy the source while live traffic keeps writing to it."""
    keys = sorted(source.data)
    for start in range(0, len(keys), COPY_BATCH_KEYS):
        for key in keys[start:start + COPY_BATCH_KEYS]:
            target.data[key] = source.data[key]
        # The copy takes real time; clients keep writing meanwhile.
        traffic.step()


def naive_move(router: Router, target: ShardServer, traffic: LiveTraffic) -> None:
    """Original rebalancer: copy, then switch. Writes made during the copy are lost."""
    copy_in_batches(router.owner, target, traffic)
    router.owner = target


def replay(source: ShardServer, target: ShardServer, position: int, limit: int) -> int:
    """Apply up to ``limit`` logged changes after ``position``; return the new position."""
    end = min(position + limit, source.log_position())
    # Replaying in log order means a later write always overwrites an earlier one,
    # even for keys whose newest value the bulk copy already picked up.
    for key, value in source.change_log[position:end]:
        target.apply(key, value)
    return end


def catch_up(source: ShardServer, target: ShardServer, position: int,
             traffic: LiveTraffic) -> int:
    """Replay changes while writes continue, until the lag is small enough to freeze."""
    for _ in range(MAX_CATCH_UP_STEPS):
        if source.log_position() - position <= CUTOVER_MAX_LAG:
            return position
        position = replay(source, target, position, REPLAY_BATCH_ENTRIES)
        traffic.step()
    raise CatchUpTooSlow("writes outran the replay; retry the move off-peak")


def move_with_catch_up(router: Router, target: ShardServer, traffic: LiveTraffic) -> None:
    """Snapshot + change-log replay + short freeze, then switch routing."""
    source = router.owner
    # Remember where the log is *before* copying: everything after this point
    # may have been missed by the copy and must be replayed.
    position = source.log_position()
    copy_in_batches(source, target, traffic)
    position = catch_up(source, target, position, traffic)
    router.frozen = True
    # Clients that write during the freeze get ShardFrozen and retry.
    traffic.step()
    replay(source, target, position, source.log_position() - position)
    router.owner = target
    router.frozen = False
