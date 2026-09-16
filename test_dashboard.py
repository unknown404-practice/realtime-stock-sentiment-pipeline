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

if __name__ == "__main__":
    unittest.main()
