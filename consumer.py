#!/usr/bin/env python3
"""
Real-Time Stock News FinBERT Consumer
====================================
Consumes stock news from Apache Kafka (topic: stock-news), executes local
FinBERT sentiment classification, and persists enriched records and audit logs
into MongoDB (StockDB.news_sentiment and StockDB.pipeline_logs).
"""

import os
import sys
import time
import json
import signal
import socket
import argparse
import warnings
from datetime import datetime, timezone

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

from pymongo import MongoClient
from kafka import KafkaConsumer
from transformers import pipeline

# Default configuration
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "stock-news"
DEFAULT_GROUP_ID = "finbert-sentiment-consumer-group"
DEFAULT_DB_NAME = "StockDB"
DEFAULT_COLLECTION = "news_sentiment"
DEFAULT_LOGS_COLLECTION = "pipeline_logs"
DEFAULT_MODEL_NAME = "ProsusAI/finbert"


def check_broker_reachable(bootstrap_servers: str, timeout: float = 1.5) -> bool:
    """
    Quickly probe if at least one bootstrap server endpoint is accepting TCP connections.
    Fails fast in < timeout seconds to prevent client libraries from deadlocking indefinitely.
    """
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


def resolve_mongo_uri(custom_uri: str = None) -> tuple[str, str]:
    """
    Resolve MongoDB connection string.
    Prioritizes local Docker MongoDB (<2ms latency) before falling back to
    .streamlit/secrets.toml / MongoDB Atlas, matching app.py to prevent split-brain.
    """
    if custom_uri:
        return custom_uri, "CLI Specified URI"

    # Priority 1: Fast local Docker MongoDB
    local_uri = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
    try:
        test_client = MongoClient(local_uri, serverSelectionTimeoutMS=500)
        test_client.admin.command("ping")
        test_client.close()
        return local_uri, "Local Docker MongoDB (<2ms, Live Stream)"
    except Exception:
        pass

    # Priority 2: Streamlit cloud secrets (Atlas)
    base_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else "."
    secrets_file = os.path.join(base_dir, ".streamlit", "secrets.toml")
    if os.path.exists(secrets_file):
        try:
            with open(secrets_file, "r", encoding="utf-8") as f:
                for line in f:
                    line_clean = line.strip()
                    if line_clean.startswith("MONGO_URI"):
                        parts = line_clean.split("=", 1)
                        if len(parts) == 2:
                            atlas_uri = parts[1].strip().strip('"').strip("'")
                            test_client = MongoClient(atlas_uri, serverSelectionTimeoutMS=2000)
                            test_client.admin.command("ping")
                            test_client.close()
                            return atlas_uri, "MongoDB Atlas (Remote Cloud)"
        except Exception:
            pass

    # Priority 3: MONGO_URI environment variable
    if os.getenv("MONGO_URI"):
        return os.getenv("MONGO_URI"), "Environment MONGO_URI"

    # Priority 4: Default localhost
    return "mongodb://localhost:27017/", "Default MongoDB"


def parse_args():
    parser = argparse.ArgumentParser(description="Headless FinBERT Sentiment Kafka Consumer")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)),
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=os.getenv("KAFKA_TOPIC", os.getenv("TOPIC_NAME", DEFAULT_TOPIC)),
        help=f"Kafka topic to consume (default: {DEFAULT_TOPIC})"
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP_ID),
        help=f"Kafka consumer group id (default: {DEFAULT_GROUP_ID})"
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
        default=os.getenv("DB_NAME", DEFAULT_DB_NAME),
        help=f"MongoDB database name (default: {DEFAULT_DB_NAME})"
    )
    parser.add_argument(
        "--collection-name",
        type=str,
        default=os.getenv("COLLECTION_NAME", DEFAULT_COLLECTION),
        help=f"MongoDB sentiment collection name (default: {DEFAULT_COLLECTION})"
    )
    parser.add_argument(
        "--logs-collection",
        type=str,
        default=os.getenv("LOGS_COLLECTION_NAME", DEFAULT_LOGS_COLLECTION),
        help=f"MongoDB lifetime logs collection name (default: {DEFAULT_LOGS_COLLECTION})"
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default=os.getenv("MODEL_NAME", DEFAULT_MODEL_NAME),
        help=f"Hugging Face model identifier (default: {DEFAULT_MODEL_NAME})"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.getenv("MAX_CONSUMER_MESSAGES", "0")),
        help="Maximum messages to consume before stopping (0 = infinite)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    bootstrap_servers = args.bootstrap_servers
    topic = args.topic
    group_id = args.group_id
    db_name = args.db_name
    collection_name = args.collection_name
    logs_collection_name = args.logs_collection
    model_name = args.model_name
    max_messages = max(0, args.max_messages)

    print("=" * 80, flush=True)
    print("[+] Real-Time Stock Sentiment Consumer (FinBERT + MongoDB)", flush=True)
    print(f"* Kafka Broker    : {bootstrap_servers}", flush=True)
    print(f"* Topic           : {topic}", flush=True)
    print(f"* Consumer Group  : {group_id}", flush=True)
    print(f"* AI Model        : {model_name} (Local Inference)", flush=True)
    print(f"* Target Database : {db_name}.{collection_name}", flush=True)
    print(f"* Max Messages    : {'Infinite' if max_messages == 0 else max_messages}", flush=True)
    print("=" * 80, flush=True)

    # 1. Resolve MongoDB URI
    mongo_uri, uri_source = resolve_mongo_uri(args.mongo_uri)
    masked_uri = mongo_uri.split("@")[-1] if "@" in mongo_uri else mongo_uri
    print(f"\n[1/4] Connecting to MongoDB ({uri_source}: {masked_uri})...", flush=True)
    try:
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command("ping")
        db = mongo_client[db_name]
        collection = db[collection_name]
        logs_col = db[logs_collection_name]
        print(f"      Connected successfully to database '{db_name}'.", flush=True)
    except Exception as exc:
        print(f"[Consumer Fatal] Failed to connect to MongoDB at {masked_uri}: {exc}", flush=True)
        print("Please ensure MongoDB container is running ('docker compose up -d').", flush=True)
        sys.exit(1)

    # 2. Verify Kafka Reachability (fail-fast before loading heavy FinBERT model)
    print(f"\n[2/4] Verifying Kafka broker reachability at {bootstrap_servers}...", flush=True)
    if not check_broker_reachable(bootstrap_servers, timeout=2.0):
        print(f"[Consumer Fatal] Kafka broker at {bootstrap_servers} is unreachable.", flush=True)
        print("Please ensure Docker Kafka container is active ('docker compose up -d').", flush=True)
        mongo_client.close()
        sys.exit(1)
    print(f"      Kafka broker reachable at {bootstrap_servers}.", flush=True)

    # 3. Load FinBERT Pipeline
    print(f"\n[3/4] Loading FinBERT model ('{model_name}')...", flush=True)
    print("      (Executing local PyTorch inference; zero external API latency)", flush=True)
    try:
        classifier = pipeline("text-classification", model=model_name)
        print("      FinBERT pipeline initialized successfully in local memory.", flush=True)
    except Exception as exc:
        print(f"[Consumer Fatal] Failed to load model '{model_name}': {exc}", flush=True)
        mongo_client.close()
        sys.exit(1)

    # 4. Initialize Kafka Consumer
    print(f"\n[4/4] Subscribing Kafka consumer to '{topic}' (group: {group_id})...", flush=True)
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            request_timeout_ms=10000
        )
        print("      Kafka consumer active. Ingesting incoming financial headlines...", flush=True)
        print("-" * 80, flush=True)
    except Exception as exc:
        print(f"[Consumer Fatal] Failed to initialize Kafka consumer: {exc}", flush=True)
        mongo_client.close()
        sys.exit(1)

    # Graceful shutdown handler
    running = True

    def stop_signal_handler(signum, frame):
        nonlocal running
        if running:
            running = False
            print("\n[Consumer] Stop signal received. Initiating graceful shutdown...", flush=True)

    signal.signal(signal.SIGINT, stop_signal_handler)
    signal.signal(signal.SIGTERM, stop_signal_handler)

    # 5. Processing Loop
    processed_count = 0
    try:
        while running:
            msg_dict = consumer.poll(timeout_ms=1000)
            if not running:
                break
            if not msg_dict:
                continue

            for records in msg_dict.values():
                for message in records:
                    if not running:
                        break

                    try:
                        payload = message.value
                        if not isinstance(payload, dict):
                            continue

                        headline = payload.get("headline", "")
                        ticker = payload.get("ticker", "UNKNOWN")
                        timestamp = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()

                        if not headline:
                            continue

                        # Local FinBERT classification (accelerated)
                        try:
                            import torch
                            with torch.inference_mode():
                                inference_res = classifier(headline)[0]
                        except Exception:
                            inference_res = classifier(headline)[0]

                        sentiment_label = inference_res.get("label", "NEUTRAL").upper()
                        confidence_score = round(float(inference_res.get("score", 0.0)), 4)

                        # Persist enriched sentiment record
                        enriched_record = {
                            "ticker": ticker,
                            "headline": headline,
                            "timestamp": timestamp,
                            "sentiment": sentiment_label,
                            "confidence": confidence_score
                        }
                        collection.insert_one(enriched_record)
                        processed_count += 1

                        # Persist lifetime audit log entry into StockDB.pipeline_logs
                        now_utc = datetime.now(timezone.utc)
                        now_str = now_utc.strftime("%H:%M:%S.%f")[:-3]
                        log_entry = {
                            "time": now_str,
                            "iso_time": timestamp,
                            "level": "FINBERT",
                            "component": "KAFKA_CONSUMER",
                            "message": f"[{sentiment_label}] {ticker}: \"{headline[:55]}...\" (conf: {confidence_score:.4f})",
                            "ticker": ticker,
                            "sentiment": sentiment_label,
                            "confidence": confidence_score,
                            "headline": headline
                        }
                        try:
                            logs_col.insert_one(log_entry)
                        except Exception:
                            pass

                        # Print clean live console log
                        print(f"[{sentiment_label}] {ticker}: {headline} (conf: {confidence_score:.4f})", flush=True)

                        if max_messages > 0 and processed_count >= max_messages:
                            print(f"[Consumer] Reached target of {max_messages} messages. Exiting cleanly.", flush=True)
                            running = False
                            break

                    except Exception as msg_err:
                        print(f"[Consumer Warning] Error processing message: {msg_err}", flush=True)
                        continue

                if not running:
                    break

    except KeyboardInterrupt:
        print("\n[Consumer] KeyboardInterrupt received.", flush=True)
    except Exception as err:
        print(f"\n[Consumer Error] Stream processing exception: {err}", flush=True)
    finally:
        print("[Consumer] Closing Kafka consumer and MongoDB connections...", flush=True)
        try:
            consumer.close()
        except Exception:
            pass
        try:
            mongo_client.close()
        except Exception:
            pass
        print(f"[Consumer] Shutdown complete. Total messages processed: {processed_count}.", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
