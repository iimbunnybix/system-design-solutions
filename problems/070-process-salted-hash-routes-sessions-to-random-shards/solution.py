"""Session shard chosen with Python's hash(), which differs in every process.

A web app stores login sessions on 8 cache shards and picked the shard with
``hash(session_id) % 8``. Python salts the hash of strings with a random seed
per process (PYTHONHASHSEED), so every worker process maps the same session
to a different shard. A login written by one worker was looked up on the
wrong shard by the next, and users were logged out at random.

The fix routes with a hash that is defined by the bytes of the key alone
(the first 8 bytes of SHA-256), so every process, host and deploy agrees.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

SHARD_COUNT = 8
HASH_BYTES_USED = 8   # 64 bits of the digest is plenty for a modulo over a few shards
HERE = Path(__file__).resolve().parent


# --- the original routing ---------------------------------------------------------
def naive_shard_for(session_id: str, shard_count: int = SHARD_COUNT) -> int:
    """Original: built-in hash(), stable only inside one process."""
    return hash(session_id) % shard_count


# --- the fix ------------------------------------------------------------------------
def stable_shard_for(session_id: str, shard_count: int = SHARD_COUNT) -> int:
    """Shard from a digest of the key's UTF-8 bytes, identical in every process.

    WHY SHA-256 and not hash(): hash() of str is randomised per process to
    resist hash-flooding attacks, so it must never leave the process. A
    cryptographic digest is overkill for strength but is in the standard
    library, fast enough for routing, and the same everywhere.
    """
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    digest = hashlib.sha256(session_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:HASH_BYTES_USED], "big") % shard_count


# --- a cluster of shards, shared by every worker -------------------------------------
class ShardedSessionStore:
    """Session data split over shards; each worker routes with its own function."""

    def __init__(self, shard_count: int = SHARD_COUNT) -> None:
        self.shards: list[dict[str, str]] = [{} for _ in range(shard_count)]

    def save(self, shard: int, session_id: str, user: str) -> None:
        """Write a session to the shard a worker chose."""
        self.shards[shard][session_id] = user

    def load(self, shard: int, session_id: str) -> str | None:
        """Read a session from the shard a worker chose (None = logged out)."""
        return self.shards[shard].get(session_id)


# --- running routing in a separate worker process -------------------------------------
def routes_in_new_process(session_ids: list[str], function_name: str, hash_seed: str) -> list[int]:
    """Route ``session_ids`` in a fresh interpreter with the given PYTHONHASHSEED.

    WHY a subprocess: the hash seed is fixed when the interpreter starts, so the
    only honest way to show two workers disagreeing is to start two workers.
    """
    environment = dict(os.environ, PYTHONHASHSEED=hash_seed)
    script = (
        "import json, sys\n"
        "import solution\n"
        f"route = solution.{function_name}\n"
        "ids = json.load(sys.stdin)\n"
        "print(json.dumps([route(session_id) for session_id in ids]))\n"
    )
    completed = subprocess.run([sys.executable, "-c", script], input=json.dumps(session_ids),
                               capture_output=True, text=True, cwd=HERE, env=environment, check=True)
    return json.loads(completed.stdout)


def agreement_rate(first_routes: list[int], second_routes: list[int]) -> float:
    """Share of keys that two workers send to the same shard."""
    if not first_routes:
        return 1.0
    same = 0
    for first, second in zip(first_routes, second_routes):
        if first == second:
            same += 1
    return same / len(first_routes)


def login_then_next_request(store: ShardedSessionStore, session_ids: list[str],
                            login_routes: list[int], next_routes: list[int]) -> int:
    """Worker A saves each login, worker B serves the next request; return logouts."""
    for session_id, shard in zip(session_ids, login_routes):
        store.save(shard, session_id, user=f"user-for-{session_id}")
    logged_out = 0
    for session_id, shard in zip(session_ids, next_routes):
        if store.load(shard, session_id) is None:
            logged_out += 1
    return logged_out
