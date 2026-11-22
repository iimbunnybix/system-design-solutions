# 066 · One multi-page invoice stopped every invoice email for the day while the producer retried it over and over

## Problem
The billing service writes an `invoice.created` event to an outbox for every
invoice. A relay publishes the outbox to a message broker **in order**, and
the email, accounting and archive services consume the topic. The event
carried the invoice PDF inline as base64, so consumers did not need to call
back to billing.

Most invoices are 20-200 KB. One morning a wholesale customer got a
multi-page statement. The broker's maximum message size is 1 MiB (1,048,576
bytes, close to Kafka's default of about 1 MB), and the broker rejected the
message. The relay retried it like any other error, and because the outbox is
ordered, every invoice behind it waited. No invoice emails went out until an
engineer deleted the row by hand that afternoon.

In the simulation (200 invoices, seeded; about 10% are statements of
0.8-3 MB):

- 19 of the 200 messages are larger than the broker limit. 3 of them have a
  PDF smaller than 1 MiB that only crosses the limit because base64 adds a
  third.
- The fourth invoice of the day is the first oversized one. The relay
  delivers **3 invoices**, then spends all **50 attempts** on the fourth and
  the other **197 invoices are stuck**.

## Root cause
Two mistakes compound:

1. **Unbounded payloads in a bounded channel.** Message brokers are built for
   many small messages and enforce a size limit. Embedding a document whose
   size depends on customer data means some message will eventually exceed
   it, and base64 makes every payload about 33% larger.
2. **A permanent error was retried like a transient one.** "Message too
   large" will fail the same way every time. In an ordered outbox, retrying
   it forever is head-of-line blocking for everything behind it.

## Solution
The **claim-check** pattern (`build_message`, `read_message`):

- A PDF up to `INLINE_LIMIT_BYTES` (256 KiB) stays inline, so the common
  case needs no extra storage call.
- A larger PDF is written to a blob store under its SHA-256 hash, and the
  message carries `pdf_ref`, `pdf_size` and `pdf_sha256` instead of the
  bytes. Content addressing means a retried publish stores the same object,
  not a second copy.
- The consumer fetches the blob and checks size and hash before using it,
  so a truncated, overwritten or swapped object raises
  `PayloadIntegrityError` instead of emailing the wrong invoice.
- `publish_outbox` never retries `MessageTooLarge`; it raises at once so the
  relay can move that event to a dead-letter table and continue.

In the same simulation all **200 invoices are delivered**, 19 PDFs go to the
blob store, and the largest message is 265,222 bytes, about a quarter of the
broker limit.

## Trade-offs
- Consumers now depend on the blob store being available and must have read
  access to it. A blob store outage delays the large invoices only.
- Blobs need a lifecycle: keep them at least as long as the topic's
  retention (plus replay windows), or a consumer replaying old messages will
  find the reference gone.
- Write the blob **before** publishing the message. If publishing fails
  afterwards, the orphaned blob costs some storage; the reverse order would
  publish a dangling reference.
- Raising the broker's size limit only moves the problem: large messages
  slow down replication, consumer fetches and memory use on every broker and
  client, and the next statement can be larger still.
- Compression (gzip/zstd at the producer) helps text payloads but not PDFs,
  which are already compressed.
- The inline limit is a tuning choice: lower means more blob calls, higher
  means larger messages. Measure message sizes per event type and alert on
  ones approaching the limit.

## Run
```bash
python -m unittest -v test_solution.py
```
