"""Comments vanished and reappeared because each refresh hit a random replica.

The comments service sends every read to a random asynchronous replica, and
the replicas lag the primary by different amounts. A reader who refreshes can
land on a fresh replica (sees a new comment), then on a staler one (the
comment is gone), then on a fresh one again (it is back): a violation of
monotonic reads. The fix gives each reader session a token holding the
highest replication position (LSN) it has already seen. The router only uses
replicas that have applied at least that position, falls back to the primary
when none has, and raises the token after every response.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

SEED = 75
PRIMARY_LSN = 1_000
# How far each replica has applied the primary's log (fixed for the model).
REPLICA_APPLIED_LSNS = {"replica-a": 1_000, "replica-b": 960, "replica-c": 900}
# The thread's comments and the LSN at which each one was committed.
COMMENT_COMMIT_LSNS = {"c1": 850, "c2": 880, "c3": 930, "c4": 990}
SESSIONS = 1_000
REFRESHES_PER_SESSION = 10


@dataclass
class Server:
    """A database server and how far it has applied the replication log."""

    name: str
    applied_lsn: int


@dataclass
class ReadResult:
    """What one page load returned and which server answered it."""

    server_name: str
    applied_lsn: int
    comment_ids: list[str]


@dataclass
class SessionToken:
    """Carried by the reader (cookie or header); starts at 0 for a new reader."""

    highest_seen_lsn: int = 0


def make_replicas() -> list[Server]:
    """Build the replica fleet from the configured applied LSNs."""
    replicas = []
    for name, applied_lsn in REPLICA_APPLIED_LSNS.items():
        replicas.append(Server(name, applied_lsn))
    return replicas


def make_primary() -> Server:
    """The primary has, by definition, applied everything."""
    return Server("primary", PRIMARY_LSN)


def read_comments(server: Server) -> ReadResult:
    """Return the comments this server can see: those committed at or before its LSN."""
    visible = []
    for comment_id, commit_lsn in COMMENT_COMMIT_LSNS.items():
        if commit_lsn <= server.applied_lsn:
            visible.append(comment_id)
    return ReadResult(server.name, server.applied_lsn, visible)


# ---------------------------------------------------------------- original
def naive_read(replicas: list[Server], rng: random.Random) -> ReadResult:
    """Original router: any replica, picked at random, regardless of its lag."""
    return read_comments(rng.choice(replicas))


# ---------------------------------------------------------------- the fix
def choose_server(replicas: list[Server], primary: Server, token: SessionToken,
                  rng: random.Random) -> Server:
    """Pick a random replica at least as fresh as this reader has already seen.

    WHY: a replica behind the token would show the reader an older state
    than their previous page, which is exactly the "comment vanished" bug.
    """
    fresh_enough = []
    for replica in replicas:
        if replica.applied_lsn >= token.highest_seen_lsn:
            fresh_enough.append(replica)
    if not fresh_enough:
        # No replica has caught up yet; the primary always has.
        return primary
    return rng.choice(fresh_enough)


def monotonic_read(replicas: list[Server], primary: Server, token: SessionToken,
                   rng: random.Random) -> ReadResult:
    """Read from a fresh-enough server, then raise the token to what we saw."""
    result = read_comments(choose_server(replicas, primary, token, rng))
    # WHY max: the token must never move backwards, or old replicas become eligible again.
    token.highest_seen_lsn = max(token.highest_seen_lsn, result.applied_lsn)
    return result


# ---------------------------------------------------------------- measuring
def count_vanishings(results: list[ReadResult]) -> int:
    """How many refreshes lost a comment that the previous refresh had shown."""
    vanishings = 0
    for previous, current in zip(results, results[1:]):
        missing = set(previous.comment_ids) - set(current.comment_ids)
        if missing:
            vanishings += 1
    return vanishings


def refresh_session(use_token: bool, rng: random.Random,
                    refreshes: int = REFRESHES_PER_SESSION) -> list[ReadResult]:
    """One reader loads the thread ``refreshes`` times in a row."""
    replicas = make_replicas()
    primary = make_primary()
    token = SessionToken()
    results = []
    for _ in range(refreshes):
        if use_token:
            results.append(monotonic_read(replicas, primary, token, rng))
        else:
            results.append(naive_read(replicas, rng))
    return results


def simulate(use_token: bool, seed: int = SEED, sessions: int = SESSIONS) -> dict[str, object]:
    """Run many readers and report how often comments flickered and who served reads."""
    rng = random.Random(seed)
    sessions_with_flicker = 0
    total_vanishings = 0
    reads_per_server: dict[str, int] = {}
    for _ in range(sessions):
        results = refresh_session(use_token, rng)
        vanishings = count_vanishings(results)
        total_vanishings += vanishings
        if vanishings > 0:
            sessions_with_flicker += 1
        for result in results:
            reads_per_server[result.server_name] = reads_per_server.get(result.server_name, 0) + 1
    return {
        "flicker_session_fraction": sessions_with_flicker / sessions,
        "vanishings": total_vanishings,
        "reads_per_server": reads_per_server,
    }
