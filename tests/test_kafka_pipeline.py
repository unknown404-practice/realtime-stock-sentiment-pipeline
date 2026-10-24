"""
Unit tests for Kafka Producer and Consumer Streaming Logic (FLAW-DATA-01, FLAW-DATA-02, FLAW-DATA-05).
Tests asynchronous non-blocking publishing, partition keying, message serialization,
batched consumer enrichment, and idempotent persistence.
"""

import json
import unittest
from unittest.mock import MagicMock, patch

from consumer import enrich_items, process_batch
from producer import create_producer, publish_stream
from src.ingestion.news_fetcher import NewsFetcher


class MockSendFuture:
    """Mock Kafka send Future supporting add_callback and add_errback."""

    def __init__(self, succeeds=True):
        self.succeeds = succeeds
        self.callbacks = []
        self.errbacks = []

    def add_callback(self, fn):
        self.callbacks.append(fn)
        if self.succeeds:
            fn(MagicMock(partition=0, offset=42))
        return self

    def add_errback(self, fn):
        self.errbacks.append(fn)
        if not self.succeeds:
            fn(Exception("Simulated broker transmission failure"))
        return self


class MockKafkaProducer:
    """Mock KafkaProducer tracking send and flush calls."""

    def __init__(self):
        self.sent_messages = []
        self.flush_count = 0
        self.closed = False

    def send(self, topic, key=None, value=None):
        self.sent_messages.append({"topic": topic, "key": key, "value": value})
        return MockSendFuture(succeeds=True)

    def flush(self, timeout=None):
        self.flush_count += 1

    def close(self, timeout=None):
        self.closed = True


class MockFinBERTService:
    """Mock FinBERT service returning predictable calibrated scores."""

    def __init__(self):
        self.model_name = "ProsusAI/finbert"
        self.device = "cpu"
        self.batch_size = 16

    def predict_batch(self, headlines):
        results = []
        for h in headlines:
            results.append({
                "sentiment": "POSITIVE" if "beats" in h.lower() or "surges" in h.lower() else "NEUTRAL",
                "confidence": 0.9412,
                "model_name": "ProsusAI/finbert",
                "model_version": "1.0",
                "inference_latency_ms": 12.5,
                "raw_scores": {"positive": 0.9412, "negative": 0.0210, "neutral": 0.0378}
            })
        return results


class MockMongoCollection:
    """Mock MongoDB collection recording bulk writes."""

    def __init__(self):
        self.bulk_writes = []

    def bulk_write(self, operations, ordered=False):
        self.bulk_writes.extend(operations)
        res = MagicMock()
        res.upserted_count = len(operations)
        res.modified_count = 0
        return res


class TestKafkaProducerPipeline(unittest.TestCase):
    """Verify Producer asynchronous publishing, keying, and batching."""

    def test_publish_stream_keys_by_ticker(self):
        """Verify messages are keyed by ticker for partition routing (FLAW-DATA-05)."""
        mock_producer = MockKafkaProducer()
        mock_fetcher = MagicMock(spec=NewsFetcher)
        mock_fetcher.get_next_news_item.side_effect = [
            {"ticker": "NVDA", "headline": "NVIDIA revenue beats estimates by 20%", "source": "Reuters"},
            {"ticker": "AAPL", "headline": "Apple announces quarterly dividend", "source": "Bloomberg"},
            {"ticker": "TSLA", "headline": "Tesla delivery numbers surge in Q3", "source": "CNBC"}
        ]

        sent, errors = publish_stream(
            producer=mock_producer,
            topic="test-stock-news",
            max_messages=3,
            interval=0.01,
            fetcher=mock_fetcher
        )

        self.assertEqual(sent, 3)
        self.assertEqual(errors, 0)
        self.assertEqual(len(mock_producer.sent_messages), 3)

        # Check key and topic assignments
        self.assertEqual(mock_producer.sent_messages[0]["key"], "NVDA")
        self.assertEqual(mock_producer.sent_messages[0]["topic"], "test-stock-news")
        self.assertEqual(mock_producer.sent_messages[1]["key"], "AAPL")
        self.assertEqual(mock_producer.sent_messages[2]["key"], "TSLA")

        # Verify producer.flush() was called on shutdown in finally block
        self.assertGreaterEqual(mock_producer.flush_count, 1)
        self.assertTrue(mock_producer.closed)

    def test_publish_payload_schema(self):
        """Verify the emitted payload adheres to required schema."""
        mock_producer = MockKafkaProducer()
        mock_fetcher = MagicMock(spec=NewsFetcher)
        mock_fetcher.get_next_news_item.return_value = {
            "ticker": "MSFT",
            "headline": "Microsoft launches autonomous agent platform",
            "source": "TechCrunch",
            "url": "https://example.com/msft-ai",
            "author": "Alice"
        }

        sent, errors = publish_stream(
            producer=mock_producer,
            topic="stock-news",
            max_messages=1,
            interval=0.01,
            fetcher=mock_fetcher
        )

        self.assertEqual(sent, 1)
        payload = mock_producer.sent_messages[0]["value"]
        self.assertEqual(payload["ticker"], "MSFT")
        self.assertEqual(payload["headline"], "Microsoft launches autonomous agent platform")
        self.assertIn("timestamp", payload)
        self.assertEqual(payload["source"], "TechCrunch")
        self.assertEqual(payload["url"], "https://example.com/msft-ai")
        self.assertEqual(payload["author"], "Alice")


class TestKafkaConsumerPipeline(unittest.TestCase):
    """Verify Consumer batched enrichment and idempotent storage (FLAW-DATA-02, FLAW-DATA-03)."""

    def setUp(self):
        self.service = MockFinBERTService()

    def test_enrich_items_skips_empty_headlines(self):
        """Ensure invalid or headline-less payloads are ignored."""
        raw_items = [
            {"ticker": "AAPL", "headline": "Apple quarterly revenue beats estimates"},
            {"ticker": "NVDA", "headline": ""},  # empty
            "not a dict",                         # invalid type
            {"ticker": "TSLA"}                   # missing headline
        ]

        enriched = enrich_items(raw_items, self.service)
        self.assertEqual(len(enriched), 1)
        self.assertEqual(enriched[0]["ticker"], "AAPL")
        self.assertEqual(enriched[0]["sentiment"], "POSITIVE")
        self.assertEqual(enriched[0]["confidence"], 0.9412)
        self.assertEqual(enriched[0]["model_name"], "ProsusAI/finbert")

    def test_process_batch_idempotent_upsert(self):
        """Verify process_batch passes enriched documents to MongoDB."""
        raw_items = [
            {"ticker": "NVDA", "headline": "NVIDIA beats revenue forecasts by 15%"},
            {"ticker": "MSFT", "headline": "Microsoft schedules annual meeting"}
        ]

        mock_db = MagicMock()
        mock_collection = MockMongoCollection()
        mock_db.__getitem__.return_value = mock_collection

        upserted_count = process_batch(raw_items, self.service, mock_db)
        self.assertEqual(upserted_count, 2)
        self.assertEqual(len(mock_collection.bulk_writes), 2)


if __name__ == "__main__":
    unittest.main()
