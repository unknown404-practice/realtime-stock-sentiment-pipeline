import unittest
from streamlit.testing.v1 import AppTest

class TestDashboardApp(unittest.TestCase):
    def test_app_loads_and_toggles(self):
        at = AppTest.from_file("app.py", default_timeout=20)
        at.run()
        self.assertFalse(at.exception, f"App had exceptions: {at.exception}")
        
        # Verify theme toggle exists in the header controls
        toggles = [t for t in at.toggle if "Dark" in t.label or "Light" in t.label]
        self.assertGreater(len(toggles), 0, "Theme toggle must exist")
        
        theme_toggle = toggles[0]
        self.assertEqual(theme_toggle.value, False, "Default theme should be Light (False)")
        
        # Test toggling to Dark Mode
        theme_toggle.set_value(True).run()
        self.assertFalse(at.exception, f"App threw exception after switching to Dark: {at.exception}")
        self.assertTrue(theme_toggle.value, "Theme toggle should be True for Dark Mode")

        # Test toggling back to Light Mode
        theme_toggle.set_value(False).run()
        self.assertFalse(at.exception, f"App threw exception after switching back to Light: {at.exception}")
        self.assertFalse(theme_toggle.value, "Theme toggle should be False for Light Mode")

    def test_sidebar_theme_button_and_sync(self):
        at = AppTest.from_file("app.py", default_timeout=20)
        at.run()
        self.assertFalse(at.exception)

        # Find sidebar button for theme switching
        buttons = [b for b in at.button if "Switch to" in b.label]
        self.assertGreater(len(buttons), 0, "Sidebar theme switch button must exist")

        # Click button to switch to Dark
        buttons[0].click().run()
        self.assertFalse(at.exception)
        theme_toggle = [t for t in at.toggle if "Dark" in t.label or "Light" in t.label][0]
        self.assertTrue(theme_toggle.value, "Header toggle must synchronize to True after sidebar click")

        # Click button again to switch back to Light
        buttons = [b for b in at.button if "Switch to" in b.label]
        buttons[0].click().run()
        self.assertFalse(at.exception)
        theme_toggle = [t for t in at.toggle if "Dark" in t.label or "Light" in t.label][0]
        self.assertFalse(theme_toggle.value, "Header toggle must synchronize to False after second click")

    def test_repeated_theme_cycling(self):
        at = AppTest.from_file("app.py", default_timeout=20)
        at.run()
        self.assertFalse(at.exception)

        theme_toggle = [t for t in at.toggle if "Dark" in t.label or "Light" in t.label][0]
        for cycle in range(3):
            theme_toggle.set_value(True).run()
            self.assertFalse(at.exception, f"Exception in cycle {cycle} setting Dark")
            self.assertTrue(theme_toggle.value)

            theme_toggle.set_value(False).run()
            self.assertFalse(at.exception, f"Exception in cycle {cycle} setting Light")
            self.assertFalse(theme_toggle.value)

    def test_css_responsive_rules_present(self):
        with open("app.py", "r", encoding="utf-8") as f:
            content = f.read()

        # Check media query breakpoints
        self.assertIn("@media (min-width: 1200px)", content, "Large desktop media query missing")
        self.assertIn("@media (min-width: 992px) and (max-width: 1199px)", content, "Laptop media query missing")
        self.assertIn("@media (min-width: 768px) and (max-width: 991px)", content, "Tablet media query missing")
        self.assertIn("@media (max-width: 767px)", content, "Mobile phones media query missing")
        self.assertIn("@media (max-width: 520px)", content, "Mobile narrow media query missing")
        self.assertIn("@media (max-width: 480px)", content, "Extra-small mobile media query missing")
        self.assertIn("@media (hover: none) and (pointer: coarse)", content, "Touch device optimizations missing")

        # Check anti-dimming & anti-clipping rules
        self.assertIn('[data-testid="stHeader"]', content, "Streamlit header fix missing")
        self.assertIn("padding-top: 3.6rem", content, "Header padding fix missing")
        self.assertIn('[data-stale="true"]', content, "Zero-dimming override missing")

        # Check theme variables
        self.assertIn("--color-scheme: dark", content)
        self.assertIn("--color-scheme: light", content)
        self.assertIn("--status-pill-bg", content)
        self.assertIn("--db-badge-bg", content)

        # Check enhanced widget theming and dataframe rules
        self.assertIn('[data-testid="stDataFrame"]', content, "DataFrame styling missing")
        self.assertIn("df_dark_css", content, "DataFrame dark theme canvas rule missing")
        self.assertIn('[data-testid="stSlider"]', content, "Slider styling missing")

    def test_filter_and_interactions(self):
        at = AppTest.from_file("app.py", default_timeout=20)
        at.run()
        self.assertFalse(at.exception)

        # Test ticker selection if found
        ticker_boxes = [s for s in at.selectbox if "Ticker" in s.label]
        if ticker_boxes:
            ticker_boxes[0].select("NVDA").run()
            self.assertFalse(at.exception)

        # Test sentiment filter if found
        sentiment_radios = [r for r in at.radio if "Sentiment" in r.label]
        if sentiment_radios:
            sentiment_radios[0].set_value("POSITIVE").run()
            self.assertFalse(at.exception)

        # Test search input if found
        search_inputs = [t for t in at.text_input if "Search" in t.label]
        if search_inputs:
            search_inputs[0].input("chip").run()
            self.assertFalse(at.exception)

    def test_lifetime_logs_persistence(self):
        import os
        from pymongo import MongoClient
        from app import add_local_log, DB_NAME, LOGS_COLLECTION_NAME

        uri = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=1000)
            client.admin.command("ping")
        except Exception:
            self.skipTest("MongoDB not reachable for persistence test")

        db = client[DB_NAME]
        initial_count = db[LOGS_COLLECTION_NAME].count_documents({})
        self.assertGreater(initial_count, 0, "pipeline_logs must contain lifetime historical records")

        # Test inserting a new log with persistence enabled
        test_msg = f"TEST_LIFETIME_EVENT_{os.urandom(4).hex()}"
        add_local_log("INFO", "SYSTEM", test_msg, persist_to_db=True)

        found = db[LOGS_COLLECTION_NAME].find_one({"message": test_msg})
        self.assertIsNotNone(found, "Newly added log must persist durably to MongoDB")
        self.assertEqual(found.get("level"), "INFO")
        self.assertEqual(found.get("component"), "SYSTEM")

        # Cleanup test document
        db[LOGS_COLLECTION_NAME].delete_one({"_id": found["_id"]})

    def test_lifetime_download_buttons_present(self):
        at = AppTest.from_file("app.py", default_timeout=20)
        at.run()
        self.assertFalse(at.exception)

        download_buttons = [b for b in at.download_button if "Lifetime" in b.label or "Log" in b.label]
        self.assertGreater(len(download_buttons), 0, "Lifetime log download buttons must be present")

    def test_dynamic_heartbeat_status(self):
        from app import check_stream_heartbeat
        with open("app.py", "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn(".status-live", content)
        self.assertIn(".status-idle", content)
        self.assertIn(".pulse-dot-idle", content)
        self.assertIn("--status-idle-bg", content)

        is_live, age_sec = check_stream_heartbeat(max_age_seconds=30)
        self.assertIsInstance(is_live, bool)
        if age_sec is not None:
            self.assertGreaterEqual(age_sec, 0.0)

    def test_dynamic_heartbeat_branches(self):
        """Deep verification of heartbeat branches with simulated database documents."""
        from datetime import datetime, timezone, timedelta
        from unittest.mock import patch, MagicMock
        from app import check_stream_heartbeat

        # Branch 1: Fresh document (< 30s) -> Live
        now = datetime.now(timezone.utc)
        fresh_ts = (now - timedelta(seconds=5)).isoformat()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value.find_one.return_value = {
            "_id": MagicMock(),
            "timestamp": fresh_ts
        }
        with patch("app.client", MagicMock()) as mock_client:
            mock_client.__getitem__.return_value = mock_db
            is_live, age = check_stream_heartbeat(max_age_seconds=30)
            self.assertTrue(is_live, "Record < 30s old must be flagged as live")
            self.assertIsNotNone(age)
            self.assertLess(age, 30.0)

        # Branch 2: Stale document (>= 30s) -> Idle
        stale_ts = (now - timedelta(seconds=60)).isoformat()
        mock_db.__getitem__.return_value.find_one.return_value = {
            "_id": MagicMock(),
            "timestamp": stale_ts
        }
        with patch("app.client", MagicMock()) as mock_client:
            mock_client.__getitem__.return_value = mock_db
            is_live, age = check_stream_heartbeat(max_age_seconds=30)
            self.assertFalse(is_live, "Record >= 30s old must be flagged as idle")
            self.assertGreaterEqual(age, 30.0)

        # Branch 3: Empty database -> Idle (False, None)
        mock_db.__getitem__.return_value.find_one.return_value = None
        with patch("app.client", MagicMock()) as mock_client:
            mock_client.__getitem__.return_value = mock_db
            is_live, age = check_stream_heartbeat(max_age_seconds=30)
            self.assertFalse(is_live)
            self.assertIsNone(age)

    def test_broker_reachability_check_fail_fast(self):
        """Verify check_broker_reachable fails fast on unreachable brokers without deadlocking."""
        from producer import check_broker_reachable as prod_check
        from consumer import check_broker_reachable as cons_check
        import time

        t0 = time.time()
        res_prod = prod_check("localhost:9999", timeout=1.0)
        t_prod = time.time() - t0
        self.assertFalse(res_prod, "Unreachable broker must return False")
        self.assertLess(t_prod, 2.5, f"Must fail fast within 2.5s, took {t_prod:.2f}s")

        t0 = time.time()
        res_cons = cons_check("localhost:9999", timeout=1.0)
        t_cons = time.time() - t0
        self.assertFalse(res_cons, "Unreachable broker must return False")
        self.assertLess(t_cons, 2.5, f"Must fail fast within 2.5s, took {t_cons:.2f}s")

    def test_consumer_mongo_resolution(self):
        from consumer import resolve_mongo_uri
        uri, label = resolve_mongo_uri()
        self.assertTrue(uri.startswith("mongodb"), f"Resolved URI must be a valid mongo URI: {uri}")
        self.assertIn("Local Docker MongoDB", label)

    def test_producer_configuration(self):
        from producer import parse_args, TICKERS, HEADLINE_TEMPLATES
        self.assertEqual(len(TICKERS), 10)
        self.assertIn("NVDA", TICKERS)
        self.assertIn("AAPL", TICKERS)
        self.assertGreater(len(HEADLINE_TEMPLATES), 10)

    def test_pipeline_runner_configuration(self):
        from run_pipeline import parse_args
        import sys
        orig_argv = sys.argv
        try:
            sys.argv = [
                "run_pipeline.py",
                "--interval", "2.5",
                "--max-messages", "10",
                "--mongo-uri", "mongodb://localhost:27017/",
                "--db-name", "TestDB"
            ]
            args = parse_args()
            self.assertEqual(args.interval, 2.5)
            self.assertEqual(args.max_messages, 10)
            self.assertEqual(args.mongo_uri, "mongodb://localhost:27017/")
            self.assertEqual(args.db_name, "TestDB")
            self.assertFalse(args.producer_only)
            self.assertFalse(args.consumer_only)
        finally:
            sys.argv = orig_argv

    def test_consumer_pipeline_logs_schema(self):
        """Verify StockDB.pipeline_logs contains required schema fields from consumer."""
        import os
        from pymongo import MongoClient
        from app import DB_NAME, LOGS_COLLECTION_NAME

        uri = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=1000)
            client.admin.command("ping")
        except Exception:
            self.skipTest("MongoDB not reachable for schema check")

        db = client[DB_NAME]
        sample = db[LOGS_COLLECTION_NAME].find_one({"level": "FINBERT"})
        if sample:
            required_keys = ["time", "iso_time", "level", "component", "message", "ticker", "sentiment"]
            for k in required_keys:
                self.assertIn(k, sample, f"pipeline_logs document missing required key: {k}")

if __name__ == "__main__":
    unittest.main()


