import json
import unittest

from solution import (BROKER_MAX_MESSAGE_BYTES, INLINE_LIMIT_BYTES, INVOICE_COUNT, BlobStore,
                      Broker, InvoiceEvent, MessageTooLarge, PayloadIntegrityError,
                      build_message, naive_message, naive_publish_outbox, publish_outbox,
                      read_message, sample_invoices)

SEED = 7


def invoice(size: int, invoice_id: str = "inv-1") -> InvoiceEvent:
    """An invoice whose PDF is ``size`` bytes of filler."""
    return InvoiceEvent(invoice_id, "cust-1", bytes(index % 251 for index in range(size)))


def message_body(message: bytes) -> dict:
    return json.loads(message)


class ReproduceProblem(unittest.TestCase):
    def test_large_statement_is_rejected_by_the_broker(self):
        with self.assertRaises(MessageTooLarge):
            Broker().publish(naive_message(invoice(2_000_000)))

    def test_pdf_under_one_mib_is_still_too_large_after_base64(self):
        message = naive_message(invoice(900_000))
        self.assertGreater(len(message), BROKER_MAX_MESSAGE_BYTES)

    def test_one_oversized_event_blocks_the_rest_of_the_outbox(self):
        broker = Broker()
        delivered = naive_publish_outbox(sample_invoices(SEED), broker)
        self.assertEqual(delivered, 3)
        self.assertEqual(broker.rejected_attempts, 50)


class FixClaimCheck(unittest.TestCase):
    def test_every_invoice_of_the_day_is_delivered(self):
        broker, blobs = Broker(), BlobStore()
        self.assertEqual(publish_outbox(sample_invoices(SEED), broker, blobs), INVOICE_COUNT)
        self.assertEqual(len(blobs.objects), 19)

    def test_every_message_fits_under_the_broker_limit(self):
        broker = Broker()
        publish_outbox(sample_invoices(SEED), broker, BlobStore())
        largest = max(len(message) for message in broker.messages)
        self.assertLess(largest, BROKER_MAX_MESSAGE_BYTES)

    def test_small_pdf_stays_inline_and_uses_no_blob(self):
        blobs = BlobStore()
        body = message_body(build_message(invoice(50_000), blobs))
        self.assertIn("pdf_base64", body)
        self.assertEqual(blobs.objects, {})

    def test_pdf_exactly_at_the_inline_limit_stays_inline(self):
        body = message_body(build_message(invoice(INLINE_LIMIT_BYTES), BlobStore()))
        self.assertIn("pdf_base64", body)

    def test_large_pdf_is_sent_as_a_reference_with_size_and_hash(self):
        body = message_body(build_message(invoice(INLINE_LIMIT_BYTES + 1), BlobStore()))
        self.assertNotIn("pdf_base64", body)
        self.assertEqual(body["pdf_size"], INLINE_LIMIT_BYTES + 1)
        self.assertTrue(body["pdf_ref"].endswith(body["pdf_sha256"]))

    def test_consumer_gets_back_the_original_events(self):
        events = sample_invoices(SEED)
        broker, blobs = Broker(), BlobStore()
        publish_outbox(events, broker, blobs)
        decoded = [read_message(message, blobs) for message in broker.messages]
        self.assertEqual(decoded, events)

    def test_same_pdf_published_twice_is_stored_once(self):
        blobs = BlobStore()
        build_message(invoice(2_000_000, "inv-1"), blobs)
        build_message(invoice(2_000_000, "inv-1"), blobs)
        self.assertEqual(len(blobs.objects), 1)


class FixVerifyAndFailFast(unittest.TestCase):
    def stored_large_message(self):
        blobs = BlobStore()
        message = build_message(invoice(500_000), blobs)
        return message, blobs, message_body(message)["pdf_sha256"]

    def test_overwritten_blob_is_rejected(self):
        message, blobs, digest = self.stored_large_message()
        blobs.objects[digest] = bytes(500_000)   # same size, different content
        with self.assertRaises(PayloadIntegrityError):
            read_message(message, blobs)

    def test_truncated_blob_is_rejected(self):
        message, blobs, digest = self.stored_large_message()
        blobs.objects[digest] = blobs.objects[digest][:1000]
        with self.assertRaises(PayloadIntegrityError):
            read_message(message, blobs)

    def test_message_that_is_still_too_large_is_tried_once_not_retried(self):
        broker = Broker(max_message_bytes=50)
        with self.assertRaises(MessageTooLarge):
            publish_outbox([invoice(10)], broker, BlobStore())
        self.assertEqual(broker.rejected_attempts, 1)


if __name__ == "__main__":
    unittest.main()
