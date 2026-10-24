"""
Unit tests for MongoDB Database repository and idempotent persistence.
Tests deterministic ID generation, micro-batch upsert, pure queries, and truthful heartbeat.
"""

import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from src.db.mongo import (
    generate_record_id,
    upsert_sentiment_batch,
    fetch_sentiment_records,
    fetch_pipeline_logs,
    check_stream_heartbeat,
    insert_pipeline_log,
)


class TestDBRepository(unittest.TestCase):
    def test_deterministic_id_generation(self):
        id1 = generate_record_id("AAPL", "Apple reports record profit", "2026-10-24T12:00:00Z")
        id2 = generate_record_id("AAPL", "Apple reports record profit", "2026-10-24T12:00:00Z")
        id3 = generate_record_id("NVDA", "Nvidia launches new chip", "2026-10-24T12:00:00Z")

        self.assertEqual(id1, id2, "Identical inputs must yield identical deterministic SHA256 ID")
        self.assertNotEqual(id1, id3, "Different inputs must yield different IDs")
        self.assertEqual(len(id1), 64)

    def test_upsert_sentiment_batch_empty(self):
        mock_db = MagicMock()
        res = upsert_sentiment_batch(mock_db, [])
        self.assertEqual(res, 0)
        mock_db.__getitem__.assert_not_called()

    def test_upsert_sentiment_batch_mock(self):
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        mock_bulk_result = MagicMock()
        mock_bulk_result.upserted_count = 2
        mock_bulk_result.modified_count = 0
        mock_collection.bulk_write.return_value = mock_bulk_result

        records = [
            {"ticker": "NVDA", "headline": "Nvidia announces new AI chip", "sentiment": "POSITIVE", "confidence": 0.95},
            {"ticker": "TSLA", "headline": "Tesla expands supercharger network", "sentiment": "POSITIVE", "confidence": 0.91}
        ]

        count = upsert_sentiment_batch(mock_db, records)
        self.assertEqual(count, 2)
        mock_collection.bulk_write.assert_called_once()
        ops = mock_collection.bulk_write.call_args[0][0]
        self.assertEqual(len(ops), 2)

    def test_fetch_sentiment_records_read_only(self):
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.limit.return_value = [
            {"ticker": "AAPL", "headline": "Headline 1", "sentiment": "POSITIVE", "confidence": 0.9}
        ]
        mock_collection.find.return_value = mock_cursor

        records = fetch_sentiment_records(mock_db, limit=10)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["ticker"], "AAPL")
        # Ensure pure read query: no insert or update methods called on DB
        mock_collection.insert_one.assert_not_called()
        mock_collection.update_one.assert_not_called()

    def test_check_stream_heartbeat_truthful_live(self):
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        now = datetime.now(timezone.utc)
        fresh_time = (now - timedelta(seconds=10)).isoformat()
        mock_collection.find_one.return_value = {"timestamp": fresh_time}

        is_live, age = check_stream_heartbeat(mock_db, max_age_seconds=45)
        self.assertTrue(is_live)
        self.assertIsNotNone(age)
        self.assertLess(age, 45.0)

    def test_check_stream_heartbeat_truthful_stale(self):
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        now = datetime.now(timezone.utc)
        stale_time = (now - timedelta(seconds=120)).isoformat()
        mock_collection.find_one.return_value = {"timestamp": stale_time}

        is_live, age = check_stream_heartbeat(mock_db, max_age_seconds=45)
        self.assertFalse(is_live)
        self.assertIsNotNone(age)
        self.assertGreaterEqual(age, 45.0)

    def test_check_stream_heartbeat_empty_database(self):
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find_one.return_value = None

        is_live, age = check_stream_heartbeat(mock_db, max_age_seconds=45)
        self.assertFalse(is_live)
        self.assertIsNone(age)


if __name__ == "__main__":
    unittest.main()
