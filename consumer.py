#!/usr/bin/env python3
"""
Real-Time Stock News FinBERT Consumer
====================================
Consumes financial headlines from Apache Kafka, performs accelerated batched
FinBERT sentiment classification, persists enriched records into MongoDB with
idempotent micro-batching, and manually commits Kafka offsets for at-least-once reliability.
"""

import argparse
import json
import logging
import os
import signal
import socket
import sys
import time
import warnings
from datetime import datetime, timezone
from typing import Optional, Tuple

# Ensure robust UTF-8 printing on Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

warnings.filterwarnings("ignore")
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("consumer")

# Flexible Kafka imports supporting kafka-python-ng and kafka-python
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    try:
        from kafka_python_ng import KafkaConsumer
        from kafka_python_ng.errors import KafkaError
    except ImportError:
        KafkaConsumer = None

from pymongo import MongoClient

from src.config import get_settings
from src.db.mongo import (
    ensure_indexes,
    get_mongo_client,
    insert_pipeline_log,
    upsert_sentiment_batch,
    MongoRepository,
)
from src.ml.finbert_service import FinBERTService

# Default constants for backwards compatibility
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "stock-news"
DEFAULT_GROUP_ID = "finbert-sentiment-consumer-group"
DEFAULT_DB_NAME = "StockDB"
DEFAULT_COLLECTION = "news_sentiment"
DEFAULT_LOGS_COLLECTION = "pipeline_logs"
DEFAULT_MODEL_NAME = "ProsusAI/finbert"


def check_broker_reachable(bootstrap_servers: str, timeout: float = 1.5) -> bool:
    """Quickly probe if at least one bootstrap server endpoint is accepting TCP connections."""
    for server in bootstrap_servers.split(","):
        server = server.strip()
        if not server:
            continue
        if ":" in server:
            host, port_str = server.split(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                port = 9092
        else:
            host, port = server, 9092

        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            continue
    return False


def resolve_mongo_uri(custom_uri: Optional[str] = None) -> Tuple[str, str]:
    """Resolve MongoDB connection string with fast-fail local check and config fallback."""
    settings = get_settings()
    if custom_uri:
        return custom_uri, "CLI Specified URI"

    # Priority 1: Fast local Docker MongoDB (<2ms latency)
    local_uri = settings.local_mongo_uri
    try:
        test_client = MongoClient(local_uri, serverSelectionTimeoutMS=500)
        test_client.admin.command("ping")
        test_client.close()
        return local_uri, "Local Docker MongoDB (<2ms, Live Stream)"
    except Exception:
        pass

    # Priority 2: Configured Atlas URI
    if settings.mongo_uri:
        return settings.mongo_uri, "MongoDB Atlas (Remote Cloud)"

    return local_uri, "Local Docker MongoDB (<2ms, Live Stream)"


def parse_args():
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Batched FinBERT Sentiment Kafka Consumer")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=settings.kafka_bootstrap_servers,
        help=f"Kafka bootstrap servers (default: {settings.kafka_bootstrap_servers})"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=settings.kafka_topic,
        help=f"Kafka topic to consume (default: {settings.kafka_topic})"
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=settings.kafka_group_id,
        help=f"Kafka consumer group id (default: {settings.kafka_group_id})"
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        default=None,
        help="Optional explicit MongoDB connection URI override"
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=settings.db_name,
        help=f"MongoDB database name (default: {settings.db_name})"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum messages to consume before stopping (0 = infinite)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.batch_size,
        help=f"Inference and persistence micro-batch size (default: {settings.batch_size})"
    )
    return parser.parse_args()


def enrich_items(items: list, finbert: FinBERTService) -> list:
    """Enrich raw news items with batched FinBERT sentiment inference."""
    valid_items = [item for item in items if isinstance(item, dict) and item.get("headline")]
    if not valid_items:
        return []

    headlines_to_predict = [item["headline"] for item in valid_items]
    predictions = finbert.predict_batch(headlines_to_predict)

    enriched_records = []
    for item, pred in zip(valid_items, predictions):
        enriched_record = {
            "ticker": item.get("ticker", "UNKNOWN"),
            "headline": item.get("headline", ""),
            "timestamp": item.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            "sentiment": pred["sentiment"],
            "confidence": pred["confidence"],
            "model_name": pred["model_name"],
            "model_version": pred.get("model_version", "1.0"),
            "inference_latency_ms": pred.get("inference_latency_ms", 0.0),
            "source": item.get("source", "MarketNews"),
            "url": item.get("url", ""),
            "author": item.get("author", "Financial Reporter")
        }
        enriched_records.append(enriched_record)
    return enriched_records


def process_batch(items: list, service: FinBERTService, repo_or_db) -> int:
    """Enrich items with FinBERT and execute idempotent bulk upsert into MongoDB."""
    enriched = enrich_items(items, service)
    if not enriched:
        return 0
    if isinstance(repo_or_db, MongoRepository):
        return repo_or_db.upsert_batch(enriched)
    return upsert_sentiment_batch(repo_or_db, enriched)


def main():
    args = parse_args()
    bootstrap_servers = args.bootstrap_servers
    topic = args.topic
    group_id = args.group_id
    db_name = args.db_name
    max_messages = max(0, args.max_messages)
    batch_size = max(1, args.batch_size)

    print("=" * 80, flush=True)
    print("[+] Batched FinBERT Sentiment Consumer (At-Least-Once Engine)", flush=True)
    print(f"* Target Broker : {bootstrap_servers}", flush=True)
    print(f"* Kafka Topic   : {topic} (Group: {group_id})", flush=True)
    print(f"* Micro-Batch   : {batch_size} records per tensor forward pass", flush=True)
    print(f"* Max Messages  : {'Infinite' if max_messages == 0 else max_messages}", flush=True)
    print("=" * 80, flush=True)

    # 1. MongoDB Connection & Index Setup (FLAW-DATA-06)
    print("\n[1/4] Connecting to MongoDB Storage Layer...", flush=True)
    target_uri, uri_label = resolve_mongo_uri(args.mongo_uri)
    mongo_client, active_label = get_mongo_client(target_uri)
    if mongo_client is None:
        print("[Consumer Fatal] Could not connect to any MongoDB instance.", flush=True)
        sys.exit(1)

    db = mongo_client[db_name]
    ensure_indexes(db)
    print(f"      Active MongoDB: {active_label}", flush=True)
    print(f"      Target Database: '{db_name}' with verified compound unique indexes.", flush=True)

    # 2. Verify Kafka Reachability
    print(f"\n[2/4] Verifying Kafka broker reachability at {bootstrap_servers}...", flush=True)
    if not check_broker_reachable(bootstrap_servers, timeout=2.0):
        print(f"[Consumer Fatal] Kafka broker at {bootstrap_servers} is unreachable.", flush=True)
        mongo_client.close()
        sys.exit(1)
    print(f"      Kafka broker reachable at {bootstrap_servers}.", flush=True)

    # 3. Load Batched FinBERT Service (FLAW-ML-01, FLAW-ML-02, FLAW-ML-03)
    settings = get_settings()
    print(f"\n[3/4] Initializing FinBERT Sentiment Engine ('{settings.model_name}')...", flush=True)
    finbert = FinBERTService(model_name=settings.model_name, batch_size=batch_size)
    print("      FinBERT service initialized.", flush=True)

    # 4. Initialize Kafka Consumer with MANUAL OFFSET COMMITTING (FLAW-DATA-02)
    print(f"\n[4/4] Subscribing Kafka consumer to '{topic}' (group: {group_id})...", flush=True)
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,    # AT-LEAST-ONCE: Manual commit only after DB persistence!
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            request_timeout_ms=10000,
            max_poll_records=batch_size
        )
        print("      Kafka consumer active. Ingesting incoming financial headlines...", flush=True)
        print("-" * 80, flush=True)
    except Exception as exc:
        print(f"[Consumer Fatal] Failed to initialize Kafka consumer: {exc}", flush=True)
        mongo_client.close()
        sys.exit(1)

    running = True

    def stop_signal_handler(signum, frame):
        nonlocal running
        if running:
            running = False
            print("\n[Consumer] Stop signal received. Finishing in-flight batch...", flush=True)

    signal.signal(signal.SIGINT, stop_signal_handler)
    signal.signal(signal.SIGTERM, stop_signal_handler)

    total_processed = 0

    try:
        while running:
            msg_dict = consumer.poll(timeout_ms=1000)
            if not running:
                break
            if not msg_dict:
                continue

            # Flatten messages across partitions into a micro-batch
            messages_batch = []
            for records in msg_dict.values():
                for message in records:
                    messages_batch.append(message)

            if not messages_batch:
                continue

            raw_items = [msg.value for msg in messages_batch if isinstance(msg.value, dict)]
            enriched_records = enrich_items(raw_items, finbert)

            if not enriched_records:
                consumer.commit()
                continue

            # Idempotent Micro-Batch Database Upsert (FLAW-DATA-03, FLAW-DATA-04)
            upsert_sentiment_batch(db, enriched_records)

            # Manual offset commit AFTER successful persistence (FLAW-DATA-02)
            consumer.commit()

            # Record telemetry log for the batch (NOT 1:1 duplicate records!)
            insert_pipeline_log(
                db,
                level="INFO",
                component="KAFKA_CONSUMER",
                message=f"Processed and persisted micro-batch of {len(enriched_records)} headlines",
                extra={
                    "batch_size": len(enriched_records),
                    "model": predictions[0]["model_name"] if predictions else "FinBERT"
                }
            )

            total_processed += len(enriched_records)
            for rec in enriched_records:
                print(
                    f"[{rec['sentiment']}] {rec['ticker']}: \"{rec['headline'][:55]}...\" "
                    f"(conf: {rec['confidence']:.4f}, {rec['inference_latency_ms']}ms)",
                    flush=True
                )

            if max_messages > 0 and total_processed >= max_messages:
                print(f"[Consumer] Reached target of {max_messages} messages. Exiting cleanly.", flush=True)
                break

    except KeyboardInterrupt:
        print("\n[Consumer] KeyboardInterrupt received.", flush=True)
    except Exception as err:
        print(f"\n[Consumer Error] Unexpected exception in processing loop: {err}", flush=True)
        insert_pipeline_log(db, level="ERROR", component="KAFKA_CONSUMER", message=str(err))
    finally:
        print("[Consumer] Closing Kafka consumer and MongoDB connections...", flush=True)
        try:
            consumer.close()
            mongo_client.close()
        except Exception:
            pass
        print(f"[Consumer] Shutdown complete. Total processed: {total_processed}.", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
