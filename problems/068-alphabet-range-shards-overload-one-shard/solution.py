"""Pick range-shard boundaries from the data, not from the alphabet.

The user directory was range-sharded by username so prefix searches stay on
one shard. The four ranges split the alphabet evenly (a-f, g-l, m-r, s-z),
but usernames do not start evenly: far more start with "s", "m" or "j" than
with "q", "x" or "y". One shard held about 1.5 times its share of users and
took the most traffic, so it was the first to run out of disk and CPU.

The fix computes split points from a sample of real keys so each range holds
about the same number of keys, routes by the whole key, and can split a
range in two at its median when it grows too large.
"""
from __future__ import annotations

from bisect import bisect_right

# An even split of the alphabet into four ranges: what the original used.
ALPHABET_SPLIT_POINTS = ["g", "m", "s"]
# Split a shard once it holds this many times the average shard size.
HOT_SHARD_RATIO = 1.3


def shard_for(key: str, split_points: list[str]) -> int:
    """Index of the range containing ``key``; boundaries belong to the right range."""
    return bisect_right(split_points, key.lower())


def shard_sizes(keys: list[str], split_points: list[str]) -> list[int]:
    """How many keys each of the ``len(split_points) + 1`` shards holds."""
    sizes = [0] * (len(split_points) + 1)
    for key in keys:
        sizes[shard_for(key, split_points)] += 1
    return sizes


def imbalance(sizes: list[int]) -> float:
    """Largest shard divided by the average; 1.0 is perfectly even."""
    average = sum(sizes) / len(sizes)
    return max(sizes) / average


# --- the original, buggy version ---------------------------------------------
def naive_split_points() -> list[str]:
    """Original: equal slices of the alphabet, whatever the keys look like."""
    return list(ALPHABET_SPLIT_POINTS)


# --- the fix -----------------------------------------------------------------
def split_points_from_sample(sample: list[str], shard_count: int) -> list[str]:
    """Boundaries at the sample's quantiles, so each range gets ~equal keys.

    Duplicate boundaries are dropped: a boundary that equals the previous one
    would create an empty range that can never receive a key.
    """
    if shard_count < 1:
        raise ValueError("need at least one shard")
    ordered = sorted(key.lower() for key in sample)
    if len(ordered) < shard_count:
        raise ValueError("sample is smaller than the number of shards")
    points: list[str] = []
    for shard_index in range(1, shard_count):
        candidate = ordered[shard_index * len(ordered) // shard_count]
        if not points or candidate > points[-1]:
            points.append(candidate)
    return points


def split_hot_shard(keys: list[str], split_points: list[str]) -> list[str]:
    """Add a boundary at the median of the largest shard if it is too big.

    Splitting one range only moves that range's keys, unlike recomputing
    every boundary, which would move keys between all shards.
    """
    sizes = shard_sizes(keys, split_points)
    hottest = sizes.index(max(sizes))
    if imbalance(sizes) < HOT_SHARD_RATIO:
        return list(split_points)
    in_hot_shard = sorted(key.lower() for key in keys if shard_for(key, split_points) == hottest)
    median = in_hot_shard[len(in_hot_shard) // 2]
    if median in split_points or median == in_hot_shard[0]:
        # Every key in the shard is the same value: a range split cannot help.
        return list(split_points)
    return sorted(split_points + [median])
