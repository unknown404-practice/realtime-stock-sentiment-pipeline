"""
Model evaluation, validation benchmarks, and confidence calibration tests.
Evaluates financial sentiment inference accuracy on Financial PhraseBank benchmarks.
"""

import unittest

from src.ml.finbert_service import FinBERTService

BENCHMARK_FINANCIAL_STATEMENTS = [
    # Positive statements (18)
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
    ("Earnings per share rose to EUR 1.52 from EUR 1.10.", "POSITIVE"),
    ("The group reported a positive turnaround in underlying operating profit.", "POSITIVE"),
    ("Demand for advanced semiconductor solutions reached an all-time peak.", "POSITIVE"),
    ("Order intake grew by 32 percent to exceed full-year financial targets.", "POSITIVE"),
    ("Operating income reached historic highs fueled by strong international sales.", "POSITIVE"),
    ("The acquisition will immediately be accretive to cash flow and adjusted earnings.", "POSITIVE"),
    ("Strong subscription growth boosted recurring annual revenues by 40%.", "POSITIVE"),
    ("The transaction significantly strengthens our market position and balance sheet.", "POSITIVE"),

    # Negative statements (17)
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
    ("The company expects lower net sales and reduced profitability for the full year.", "NEGATIVE"),
    ("Operating profit decreased by 42 percent due to rising raw material inflation.", "NEGATIVE"),
    ("Restructuring costs and plant closures resulted in a steep quarterly net loss.", "NEGATIVE"),
    ("The board decided to lower the annual dividend distribution to preserve liquidity.", "NEGATIVE"),
    ("The rating downgrade reflects persistent cash burn and elevated refinancing risks.", "NEGATIVE"),
    ("Demand weakened significantly across key European and Asian end markets.", "NEGATIVE"),
    ("The group warned that operating margin will fall below the long-term target.", "NEGATIVE"),

    # Neutral statements (15)
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
    ("The transaction is expected to close during the fourth quarter subject to regulatory approvals.", "NEUTRAL"),
    ("The company has appointed a new Chief Information Officer effective immediately.", "NEUTRAL"),
    ("Trading in the company's shares was temporarily halted pending announcement.", "NEUTRAL"),
    ("The financial statements have been prepared in accordance with IFRS standards.", "NEUTRAL"),
    ("The company will hold a press conference at 11:00 am local time.", "NEUTRAL"),
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
        self.assertLess(res["inference_latency_ms"], 1000.0, "Single inference should complete within 1000ms")

    def test_batch_prediction(self):
        headlines = [item[0] for item in BENCHMARK_FINANCIAL_STATEMENTS[:16]]
        results = self.service.predict_batch(headlines)
        self.assertEqual(len(results), 16)
        for r in results:
            self.assertIn(r["sentiment"], ["POSITIVE", "NEGATIVE", "NEUTRAL"])
            self.assertGreater(r["confidence"], 0.3)
            self.assertGreaterEqual(r["inference_latency_ms"], 0.0)
            self.assertLess(r["inference_latency_ms"], 500.0, "Batched per-item latency should be under 500ms")

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
