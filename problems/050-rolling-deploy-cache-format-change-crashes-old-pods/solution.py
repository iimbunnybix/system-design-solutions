"""Cache entries that old and new app versions can share during a rolling deploy.

Release 2 of the pricing service changed the cached product format from
{"price": 1999} to {"price": {"amount": 1999, "currency": "INR"}} under the
same cache key. During the rolling deploy both versions ran side by side and
shared one cache, so old pods crashed formatting the new dict and new pods
crashed looking up keys missing from old entries. Errors flipped back and
forth with whichever version wrote the key last. This version puts the schema
version in the cache key, so each release only ever reads entries in a format
it understands, and treats undecodable entries as a cache miss, not a crash.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable


class SharedCache:
    """Stand-in for Redis/Memcached: one key-value store shared by every pod."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


@dataclass(frozen=True)
class ProductFormat:
    """How one release encodes and decodes a cached product."""
    schema_version: int
    encode: Callable[[dict], str]
    decode: Callable[[str], dict]
    render_price: Callable[[dict], str]


def _render_v1(product: dict) -> str:
    return f"₹{product['price'] / 100:.2f}"


def _render_v2(product: dict) -> str:
    price = product["price"]
    return f"{price['currency']} {price['amount'] / 100:.2f}"


V1_FORMAT = ProductFormat(1, json.dumps, json.loads, _render_v1)
V2_FORMAT = ProductFormat(2, json.dumps, json.loads, _render_v2)


def naive_cache_key(product_id: int) -> str:
    """Original key: the same for every release."""
    return f"product:{product_id}"


def versioned_cache_key(product_id: int, schema_version: int) -> str:
    """Fixed key: each schema version has its own namespace in the shared cache."""
    return f"product:v{schema_version}:{product_id}"


@dataclass
class PricingPod:
    """One running pod of a given release."""
    product_format: ProductFormat
    load_from_database: Callable[[int, int], dict]
    cache: SharedCache
    versioned_keys: bool
    decode_failures: int = 0

    def _key(self, product_id: int) -> str:
        if self.versioned_keys:
            return versioned_cache_key(product_id, self.product_format.schema_version)
        return naive_cache_key(product_id)

    def _read_cached(self, key: str) -> dict | None:
        """Decode a cached entry; a broken entry counts as a miss, never an error page."""
        raw = self.cache.get(key)
        if raw is None:
            return None
        try:
            product = self.product_format.decode(raw)
            # Render once here so a wrong-shape entry is caught as a miss too.
            self.product_format.render_price(product)
            return product
        except (ValueError, KeyError, TypeError):
            self.decode_failures += 1
            return None

    def price_label(self, product_id: int) -> str:
        """Return the price shown on the product page, using the shared cache."""
        key = self._key(product_id)
        if self.versioned_keys:
            product = self._read_cached(key)
        else:
            # Original behaviour: trust whatever is in the cache.
            raw = self.cache.get(key)
            product = None if raw is None else self.product_format.decode(raw)
        if product is None:
            product = self.load_from_database(product_id, self.product_format.schema_version)
            self.cache.set(key, self.product_format.encode(product))
        return self.product_format.render_price(product)
