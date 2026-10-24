"""
Unit tests for Real-Time Financial News Ingestion Engine.
Tests ticker symbol extraction, schema validation, and deduplication hashing.
"""

import unittest
from datetime import datetime

from src.ingestion.news_fetcher import SUPPORTED_TICKERS, NewsFetcher, extract_ticker_from_text


class TestNewsFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = NewsFetcher()

    def test_ticker_extraction_direct_symbols(self):
        self.assertEqual(extract_ticker_from_text("NVDA launches new Blackwell AI accelerator chips."), "NVDA")
        self.assertEqual(extract_ticker_from_text("Apple quarterly revenue beats forecasts."), "AAPL")
        self.assertEqual(extract_ticker_from_text("Tesla vehicle deliveries exceed estimates."), "TSLA")
        self.assertEqual(extract_ticker_from_text("Microsoft Azure cloud revenue accelerates."), "MSFT")
        self.assertEqual(extract_ticker_from_text("Amazon Web Services closes multi-year deal."), "AMZN")

    def test_ticker_extraction_company_names(self):
        self.assertEqual(extract_ticker_from_text("Alphabet announces quarterly dividend program."), "GOOGL")
        self.assertEqual(extract_ticker_from_text("Meta Platforms expands AI open-source investments."), "META")
        self.assertEqual(extract_ticker_from_text("JPMorgan Chase investment banking profit surges."), "JPM")
        self.assertEqual(extract_ticker_from_text("Bank of America increases quarterly shareholder dividend."), "BAC")

    def test_news_payload_schema(self):
        item = self.fetcher.get_next_news_item()
        self.assertIsInstance(item, dict)

        required_fields = ["ticker", "headline", "timestamp", "source", "url", "author"]
        for field in required_fields:
            self.assertIn(field, item, f"Missing required payload field: {field}")

        self.assertIn(item["ticker"], SUPPORTED_TICKERS)
        self.assertGreater(len(item["headline"]), 10)
        self.assertTrue(item["url"].startswith("http"))

        # Verify timestamp is valid ISO format
        dt = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
        self.assertIsNotNone(dt)

    def test_pool_iteration_uniqueness(self):
        items = [self.fetcher.get_next_news_item() for _ in range(5)]
        headlines = [i["headline"] for i in items]
        # Verify successive items are distinct headlines
        self.assertEqual(len(set(headlines)), len(headlines))


if __name__ == "__main__":
    unittest.main()
