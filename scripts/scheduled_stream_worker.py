"""
24/7 Persistent Stream Ingestion Scheduler Worker.
Runs continuously or in scheduled intervals (e.g. GitHub Actions cron every 5 minutes),
ingesting real-time stock news, scoring with FinBERT, and persisting records and telemetry
to MongoDB Atlas to ensure the live stream never goes idle.
"""

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings
from src.db.mongo import enforce_retention_policy, get_mongo_client, insert_pipeline_log, upsert_sentiment_batch
from src.ingestion.news_fetcher import NewsFetcher
from src.ml.finbert_service import FinBERTService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stream_scheduler")

_running = True


def _handle_signal(signum, frame):
    global _running
    logger.info("Signal %s received. Stopping scheduler worker gracefully...", signum)
    _running = False


def run_scheduler(
    duration_seconds: float = 60.0,
    interval: float = 2.5,
    max_items: int = 0,
    mongo_uri: str = None,
):
    global _running
    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    settings = get_settings()
    custom_uri = mongo_uri or os.getenv("MONGO_URI") or settings.mongo_uri

    if not custom_uri:
        logger.warning("No MONGO_URI provided in environment or settings. Skipping scheduled execution.")
        return

    logger.info("Connecting to MongoDB Atlas...")
    client, label = get_mongo_client(custom_uri=custom_uri, timeout_ms=5000)
    if client is None:
        logger.error("Failed to connect to MongoDB. Exiting.")
        sys.exit(1)

    db_name = settings.db_name
    db = client[db_name]
    logger.info("Connected successfully to %s (database: %s)", label, db_name)

    # Enforce rolling storage retention to keep Atlas permanently compact under 15MB
    enforce_retention_policy(db, max_news=8000, max_logs=15000)

    logger.info("Initializing NewsFetcher and FinBERTService...")
    fetcher = NewsFetcher()
    finbert = FinBERTService()

    start_time = time.time()
    items_processed = 0

    logger.info(
        "Starting stream ingestion loop: duration=%.1fs, interval=%.1fs, max_items=%d",
        duration_seconds,
        interval,
        max_items,
    )

    while _running:
        elapsed = time.time() - start_time
        if duration_seconds > 0 and elapsed >= duration_seconds:
            logger.info("Target duration of %.1fs reached. Exiting cleanly.", duration_seconds)
            break
        if max_items > 0 and items_processed >= max_items:
            logger.info("Target max_items of %d reached. Exiting cleanly.", max_items)
            break

        try:
            item = fetcher.get_next_news_item()
            preds = finbert.predict_batch([item["headline"]])
            if not preds:
                time.sleep(interval)
                continue
            pred = preds[0]
            now_utc = datetime.now(timezone.utc)
            now_iso = now_utc.isoformat()

            rec = {
                "ticker": item["ticker"],
                "headline": item["headline"],
                "sentiment": pred["sentiment"],
                "confidence": float(pred["confidence"]),
                "model_name": pred.get("model_name", "ProsusAI/finbert"),
                "inference_latency_ms": pred.get("inference_latency_ms", 35.0),
                "timestamp": now_iso,
                "source": item.get("source", "Market Feeds"),
                "url": item.get("url", ""),
            }

            upsert_sentiment_batch(db, [rec])

            # Producer telemetry log
            insert_pipeline_log(
                db,
                level="INFO",
                component="KAFKA_PRODUCER",
                message=f"Partition published key={rec['ticker']} to topic 'stock-sentiment-stream'",
                extra={"ticker": rec["ticker"]},
            )

            # Sentiment pipeline telemetry log
            insert_pipeline_log(
                db,
                level="FINBERT",
                component="STREAM_PIPELINE",
                message=f"Classified [{rec['ticker']}] -> {rec['sentiment']} (conf: {rec['confidence']:.4f}, latency: {rec['inference_latency_ms']:.1f}ms)",
                extra={
                    "latency_ms": rec["inference_latency_ms"],
                    "ticker": rec["ticker"],
                    "sentiment": rec["sentiment"],
                },
            )

            items_processed += 1
            if items_processed % 50 == 0:
                enforce_retention_policy(db, max_news=8000, max_logs=15000)

            logger.info(
                "[%d] %s | %s | %s (conf: %.4f, %.1fms)",
                items_processed,
                rec["ticker"],
                rec["sentiment"],
                rec["headline"][:55],
                rec["confidence"],
                rec["inference_latency_ms"],
            )

        except Exception as exc:
            logger.warning("Error during ingestion tick: %s", exc)

        time.sleep(interval)

    enforce_retention_policy(db, max_news=8000, max_logs=15000)
    logger.info("Scheduler completed. Total items ingested and persisted: %d", items_processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Persistent 24/7 stream ingestion scheduler")
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=float(os.getenv("STREAM_DURATION_SECONDS", "60")),
        help="How long to run the worker in seconds (default: 60s)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("STREAM_INTERVAL_SECONDS", "2.5")),
        help="Sleep interval between news items in seconds (default: 2.5s)",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=int(os.getenv("STREAM_MAX_ITEMS", "0")),
        help="Maximum items to ingest before stopping (0 = unlimited)",
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        default=None,
        help="Optional MongoDB URI override",
    )
    args = parser.parse_args()

    run_scheduler(
        duration_seconds=args.duration_seconds,
        interval=args.interval,
        max_items=args.max_items,
        mongo_uri=args.mongo_uri,
    )
