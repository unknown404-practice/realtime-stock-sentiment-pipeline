"""
Behavioral unit tests for dashboard presentation components.
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from src.dashboard.components import (
    load_css,
    render_header,
    render_logs_table,
    render_metrics,
    render_news_feed,
)


class TestDashboardComponents(unittest.TestCase):
    def test_load_css_light_and_dark(self):
        css_light = load_css(is_dark=False)
        self.assertIn('data-theme="light"', css_light)
        self.assertIn("<style>", css_light)

        css_dark = load_css(is_dark=True)
        self.assertIn('data-theme="dark"', css_dark)
        self.assertIn("</style>", css_dark)

    @patch("src.dashboard.components.st")
    def test_render_header_live_and_offline(self, mock_st):
        mock_col1 = MagicMock()
        mock_col2 = MagicMock()
        mock_st.columns.return_value = (mock_col1, mock_col2)

        # Test live
        render_header("MongoDB Atlas", is_live=True, age_sec=5.0)
        mock_st.columns.assert_called_once()
        mock_st.markdown.assert_called()
        markdown_call = mock_st.markdown.call_args[0][0]
        self.assertIn("STREAM: LIVE FAST", markdown_call)
        self.assertIn("status-live", markdown_call)

        # Test offline
        mock_st.markdown.reset_mock()
        render_header("Disconnected", is_live=False, age_sec=None)
        markdown_call = mock_st.markdown.call_args[0][0]
        self.assertIn("STREAM: OFFLINE / PAUSED", markdown_call)
        self.assertIn("status-offline", markdown_call)

    @patch("src.dashboard.components.st")
    def test_render_metrics_empty_and_populated(self, mock_st):
        created_cols = []

        def make_cols(n):
            c = [MagicMock() for _ in range(n if isinstance(n, int) else len(n))]
            created_cols.append(c)
            return c

        mock_st.columns.side_effect = make_cols

        # Test empty df
        render_metrics(pd.DataFrame())
        created_cols[0][0].metric.assert_called_with("Streamed Records", "0")

        # Test populated df
        df = pd.DataFrame([
            {"ticker": "NVDA", "sentiment": "POSITIVE", "confidence": 0.95},
            {"ticker": "TSLA", "sentiment": "NEGATIVE", "confidence": 0.85},
            {"ticker": "AAPL", "sentiment": "NEUTRAL", "confidence": 0.75},
        ])
        render_metrics(df)
        created_cols[1][0].metric.assert_called_with("Streamed Headlines", "3")
        created_cols[1][1].metric.assert_called_with("Positive", "33.3%", delta="1 items")
        created_cols[1][2].metric.assert_called_with("Negative", "33.3%", delta="-1 items", delta_color="inverse")

    @patch("src.dashboard.components.st")
    def test_render_news_feed_filtering(self, mock_st):
        df = pd.DataFrame([
            {"timestamp": "2026-10-24T12:00:00Z", "ticker": "NVDA", "sentiment": "POSITIVE", "confidence": 0.95, "headline": "NVIDIA Blackwell GPU"},
            {"timestamp": "2026-10-24T12:01:00Z", "ticker": "TSLA", "sentiment": "NEGATIVE", "confidence": 0.85, "headline": "Tesla recall issues"},
        ])

        # Filter by ticker
        render_news_feed(df, ticker="NVDA", sentiment="ALL", search="")
        mock_st.dataframe.assert_called_once()
        passed_df = mock_st.dataframe.call_args[0][0]
        self.assertEqual(len(passed_df), 1)
        self.assertEqual(passed_df.iloc[0]["ticker"], "NVDA")

        # Filter by search
        mock_st.dataframe.reset_mock()
        render_news_feed(df, ticker="ALL", sentiment="ALL", search="recall")
        passed_df = mock_st.dataframe.call_args[0][0]
        self.assertEqual(len(passed_df), 1)
        self.assertEqual(passed_df.iloc[0]["ticker"], "TSLA")

    @patch("src.dashboard.components.st")
    def test_render_logs_table(self, mock_st):
        logs = [
            {"time": "12:00:00.100", "level": "INFO", "component": "KAFKA", "message": "Ready"}
        ]
        render_logs_table(logs, total_count=1)
        mock_st.dataframe.assert_called_once()
        mock_st.download_button.assert_called_once()


if __name__ == "__main__":
    unittest.main()
