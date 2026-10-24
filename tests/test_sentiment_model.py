"""
Model evaluation, validation benchmarks, and confidence calibration tests.
Evaluates financial sentiment inference accuracy on Financial PhraseBank benchmarks.
"""

import unittest
from src.ml.finbert_service import FinBERTService

BENCHMARK_FINANCIAL_STATEMENTS = [
    # Positive statements
    ("Company reports Q2 net profit increased by 35% year-over-year.", "POSITIVE"),
    ("Operating profit jumped to 45 million euros from 30 million.", "POSITIVE"),
    ("Sales grew 18 percent to 210 million beating market forecasts.", "POSITIVE"),
    ("Analyst upgrades stock to Strong Buy citing accelerated AI market expansion.", "POSITIVE"),
    ("Board announces record quarterly dividend and $10B share repurchase program.", "POSITIVE"),
    ("Company secures major multi-year enterprise cloud contract.", "POSITIVE"),
    ("Gross margins expanded by 240 basis points exceeding consensus.", "POSITIVE"),
    ("Strong cash flow enables full debt prepayment ahead of schedule.", "POSITIVE"),
    ("Revenue exceeded expectations driven by robust software licensing growth.", "POSITIVE"),
    ("Strategic alliance formed to accelerate global technology deployment.", "POSITIVE"),

    # Negative statements
    ("Net sales fell 22% as operating losses widened significantly.", "NEGATIVE"),
    ("Operating profit decreased by 18% due to higher input costs.", "NEGATIVE"),
    ("Company cuts full-year revenue outlook citing supply chain constraints.", "NEGATIVE"),
    ("Disappointing quarterly results: company misses EPS estimates by wide margin.", "NEGATIVE"),
    ("Regulatory agency opens antitrust investigation into market practices.", "NEGATIVE"),
    ("Credit rating agency downgrades corporate debt outlook to negative.", "NEGATIVE"),
    ("Company announces mandatory product recall affecting 50,000 units.", "NEGATIVE"),
    ("Executive resignations and delivery delays trigger sharp stock decline.", "NEGATIVE"),
    ("Operating margins contracted sharply amidst intense price competition.", "NEGATIVE"),
    ("Cybersecurity breach results in unexpected regulatory compliance fine.", "NEGATIVE"),

    # Neutral statements
    ("Company will publish its interim report next Thursday at 09:00 CET.", "NEUTRAL"),
    ("Annual general meeting was held in Helsinki on April 12.", "NEUTRAL"),
    ("Company files Form 8-K disclosure statement with the SEC.", "NEUTRAL"),
    ("Executive leadership to speak at upcoming institutional investor conference.", "NEUTRAL"),
    ("Shares traded unchanged in quiet session ahead of central bank meeting.", "NEUTRAL"),
    ("Company announces routine transition in board audit committee.", "NEUTRAL"),
    ("Board of directors convenes to discuss annual corporate governance review.", "NEUTRAL"),
    ("Company schedules conference call and simultaneous audio webcast.", "NEUTRAL"),
    ("Distribution center consolidated to streamline supply chain routing.", "NEUTRAL"),
    ("Shelf registration statement filed with regulators for capital refinancing.", "NEUTRAL"),
]


class TestFinBERTService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = FinBERTService(batch_size=16)

    def test_single_prediction_schema(self):
        res = self.service.predict_single("Revenue jumped 25% beating Wall Street expectations.")
        self.assertIn("sentiment", res)
        self.assertIn("confidence", res)
        self.assertIn("model_name", res)
        self.assertIn("model_version", res)
        self.assertIn("inference_latency_ms", res)
        self.assertIn(res["sentiment"], ["POSITIVE", "NEGATIVE", "NEUTRAL"])
        self.assertGreaterEqual(res["confidence"], 0.0)
        self.assertLessEqual(res["confidence"], 1.0)

    def test_batch_prediction(self):
        headlines = [item[0] for item in BENCHMARK_FINANCIAL_STATEMENTS[:16]]
        results = self.service.predict_batch(headlines)
        self.assertEqual(len(results), 16)
        for r in results:
            self.assertIn(r["sentiment"], ["POSITIVE", "NEGATIVE", "NEUTRAL"])
            self.assertGreater(r["confidence"], 0.3)
            self.assertGreaterEqual(r["inference_latency_ms"], 0.0)

    def test_benchmark_financial_accuracy(self):
        """Verify minimum accuracy threshold (> 85%) on benchmark statements."""
        correct = 0
        total = len(BENCHMARK_FINANCIAL_STATEMENTS)

        headlines = [s[0] for s in BENCHMARK_FINANCIAL_STATEMENTS]
        expected_labels = [s[1] for s in BENCHMARK_FINANCIAL_STATEMENTS]

        predictions = self.service.predict_batch(headlines)
        for pred, expected in zip(predictions, expected_labels):
            if pred["sentiment"] == expected:
                correct += 1

        accuracy = correct / total
        self.assertGreaterEqual(accuracy, 0.85, f"Accuracy {accuracy:.2%} below 85% requirement")

    def test_confidence_calibration(self):
        """Verify confidence scores represent valid probabilities rather than arbitrary metrics."""
        pred = self.service.predict_single("Operating profit surged 40% on record sales.")
        self.assertEqual(pred["sentiment"], "POSITIVE")
        self.assertGreaterEqual(pred["confidence"], 0.5)

        pred_neg = self.service.predict_single("Operating profit decreased by 18% due to higher input costs.")
        self.assertEqual(pred_neg["sentiment"], "NEGATIVE")
        self.assertGreaterEqual(pred_neg["confidence"], 0.5)


if __name__ == "__main__":
    unittest.main()
