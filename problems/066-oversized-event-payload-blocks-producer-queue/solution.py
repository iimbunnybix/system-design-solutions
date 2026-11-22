"""Send large event payloads by reference (claim check) instead of inline.

The billing service published an ``invoice.created`` event with the invoice
PDF embedded as base64. Most PDFs are small, but multi-page statements are
larger than the broker's maximum message size (about 1 MiB, close to Kafka's
default). The broker rejected them, the producer treated the rejection like
a network error and retried it forever, and because the outbox is sent in
order, every invoice event behind it was stuck too.

The fix uses the claim-check pattern: payloads above an inline limit are
stored in a blob store under their SHA-256 hash, and the message carries only
a reference, the size and the hash. The consumer fetches the blob and checks
size and hash before using it. Small payloads stay inline. A message that is
still too large is a permanent error and is never retried.
"""
from __future__ import annotations

import base64
import hashlib
import json
import random
from dataclasses import dataclass, field

BROKER_MAX_MESSAGE_BYTES = 1_048_576   # 1 MiB
INLINE_LIMIT_BYTES = 256 * 1024        # payloads above this go to the blob store
NAIVE_MAX_ATTEMPTS = 50                # stands in for "retry forever"
BLOB_REF_PREFIX = "blob://sha256/"


class MessageTooLarge(Exception):
    """Permanent broker error: the same bytes will be rejected every time."""


class PayloadIntegrityError(Exception):
    """The fetched blob is not the one the message describes."""


@dataclass(frozen=True)
class InvoiceEvent:
    invoice_id: str
    customer_id: str
    pdf: bytes


@dataclass
class Broker:
    """A topic that rejects messages above its size limit."""
    max_message_bytes: int = BROKER_MAX_MESSAGE_BYTES
    messages: list[bytes] = field(default_factory=list)
    rejected_attempts: int = 0

    def publish(self, message: bytes) -> None:
        if len(message) > self.max_message_bytes:
            self.rejected_attempts += 1
            raise MessageTooLarge(f"{len(message)} bytes > {self.max_message_bytes}")
        self.messages.append(message)


@dataclass
class BlobStore:
    """Object storage keyed by content hash (like S3 with sha256 keys)."""
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        # Same content, same key: a retried publish does not store a second copy.
        self.objects[digest] = data
        return BLOB_REF_PREFIX + digest

    def get(self, reference: str) -> bytes:
        return self.objects[reference.removeprefix(BLOB_REF_PREFIX)]


# --- original ----------------------------------------------------------------
def naive_message(event: InvoiceEvent) -> bytes:
    """Original encoding: the whole PDF inline as base64 (about 4/3 the size)."""
    body = {"invoice_id": event.invoice_id, "customer_id": event.customer_id,
            "pdf_base64": base64.b64encode(event.pdf).decode("ascii")}
    return json.dumps(body).encode("utf-8")


def naive_publish_outbox(events: list[InvoiceEvent], broker: Broker) -> int:
    """Publish in order, retrying every error; returns how many were delivered.

    An outbox is ordered, so a message that never succeeds stops all after it.
    """
    delivered = 0
    for event in events:
        message = naive_message(event)
        for _ in range(NAIVE_MAX_ATTEMPTS):
            try:
                broker.publish(message)
                break
            except MessageTooLarge:
                continue   # the bug: retrying a permanent error
        else:
            return delivered
        delivered += 1
    return delivered


# --- fix: claim check ----------------------------------------------------------
def build_message(event: InvoiceEvent, blobs: BlobStore,
                  inline_limit: int = INLINE_LIMIT_BYTES) -> bytes:
    """Encode the event, moving a large PDF into the blob store first."""
    body: dict[str, object] = {"invoice_id": event.invoice_id, "customer_id": event.customer_id}
    if len(event.pdf) <= inline_limit:
        body["pdf_base64"] = base64.b64encode(event.pdf).decode("ascii")
    else:
        # The hash and size let the consumer prove it fetched the right bytes.
        body["pdf_ref"] = blobs.put(event.pdf)
        body["pdf_size"] = len(event.pdf)
        body["pdf_sha256"] = hashlib.sha256(event.pdf).hexdigest()
    return json.dumps(body).encode("utf-8")


def publish_outbox(events: list[InvoiceEvent], broker: Broker, blobs: BlobStore) -> int:
    """Publish in order with claim checks; returns how many were delivered.

    MessageTooLarge is not retried: it propagates so the event can be moved
    aside (dead-lettered) instead of blocking the outbox.
    """
    delivered = 0
    for event in events:
        broker.publish(build_message(event, blobs))
        delivered += 1
    return delivered


def read_message(message: bytes, blobs: BlobStore) -> InvoiceEvent:
    """Decode a message, fetching and verifying the PDF if it was stored aside."""
    body = json.loads(message)
    if "pdf_base64" in body:
        pdf = base64.b64decode(body["pdf_base64"])
    else:
        pdf = blobs.get(body["pdf_ref"])
        verify_payload(pdf, body["pdf_size"], body["pdf_sha256"])
    return InvoiceEvent(body["invoice_id"], body["customer_id"], pdf)


def verify_payload(pdf: bytes, expected_size: int, expected_sha256: str) -> None:
    """Refuse a blob that was truncated, overwritten or swapped."""
    if len(pdf) != expected_size:
        raise PayloadIntegrityError(f"size {len(pdf)} != {expected_size}")
    if hashlib.sha256(pdf).hexdigest() != expected_sha256:
        raise PayloadIntegrityError("sha256 mismatch")


# --- a day of invoices, for the simulation -------------------------------------
INVOICE_COUNT = 200
LARGE_STATEMENT_SHARE = 0.10           # multi-page statements


def sample_invoices(seed: int) -> list[InvoiceEvent]:
    """Mostly 20-200 KB PDFs, about 10% large statements of 0.8-3 MB."""
    rng = random.Random(seed)
    events = []
    for index in range(INVOICE_COUNT):
        if rng.random() < LARGE_STATEMENT_SHARE:
            size = rng.randint(800_000, 3_000_000)
        else:
            size = rng.randint(20_000, 200_000)
        events.append(InvoiceEvent(f"inv-{index:04d}", f"cust-{index % 37}", rng.randbytes(size)))
    return events
