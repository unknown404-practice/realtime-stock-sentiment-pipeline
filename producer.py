#!/usr/bin/env python3
"""
Real-Time Stock News Kafka Producer
===================================
Streams realistic financial headlines across major market tickers to an Apache
Kafka broker (topic: stock-news) with ISO timestamps and clean signal handling.
"""

import os
import sys
import time
import json
import random
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

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# Default configuration
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "stock-news"
DEFAULT_INTERVAL = 0.8

# Key market tickers
TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "JPM", "BAC"]

# Diverse financial headlines across positive, negative, and neutral market events
HEADLINE_TEMPLATES = [
    # Positive market events
    "{ticker} reports quarterly earnings and revenue beating Wall Street estimates by a wide margin.",
    "{ticker} unveils next-generation enterprise AI accelerator chips, boosting forward guidance.",
    "Analyst upgrades {ticker} to Strong Buy with an aggressive price target increase.",
    "{ticker} announces record quarterly free cash flow and a new $15 billion share buyback program.",
    "Major European regulatory body greenlights {ticker}'s landmark multi-billion dollar acquisition.",
    "{ticker} forms strategic cloud computing and robotics alliance with global automotive leaders.",
    "Investor sentiment surges as {ticker} expands gross margins ahead of market expectations.",
    "{ticker} raises full-year dividend payout following robust institutional customer adoption.",
    "{ticker} closes major multi-year sovereign cloud infrastructure contract with global partners.",
    "{ticker} operating profit jumps 28% year-over-year fueled by high-margin software licensing.",

    # Negative market events
    "{ticker} faces new antitrust investigation from regulators over alleged market monopolization.",
    "{ticker} cuts full-year revenue outlook citing prolonged semiconductor supply chain constraints.",
    "Disappointing quarterly results: {ticker} misses EPS forecast as operating expenses rise sharply.",
    "Credit rating agency downgrades outlook on {ticker} senior unsecured corporate bonds.",
    "{ticker} shares tumble following executive departures and product delivery delays.",
    "Wall Street firm cuts {ticker} price target due to softening consumer demand and macro headwinds.",
    "{ticker} announces mandatory global recall of 85,000 hardware units due to battery defect.",
    "{ticker} CFO warns of persistent currency fluctuations and rising geopolitical risks.",
    "{ticker} discloses unexpected regulatory compliance fine following cybersecurity review.",
    "{ticker} gross margins contract by 210 basis points amidst fierce price competition.",

    # Neutral / Corporate market events
    "{ticker} schedules its Q3 fiscal financial results conference call and webcast for next week.",
    "{ticker} concludes annual general shareholder meeting and ratifies board appointments.",
    "{ticker} executive leadership speaks at Goldman Sachs Global Technology & Media Conference.",
    "{ticker} consolidates regional distribution hubs to streamline supply chain logistics.",
    "{ticker} trades sideways in quiet trading session ahead of Federal Reserve interest rate decision.",
    "{ticker} submits mandatory Form 8-K disclosure with the Securities and Exchange Commission.",
    "{ticker} announces upcoming transition in board of directors governance committee.",
    "{ticker} files shelf registration statement with regulators for routine capital refinancing."
]


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


def ensure_kafka_topic(bootstrap_servers: str, topic_name: str) -> None:
    """Verify topic existence or create it via KafkaAdminClient."""
    print(f"[Producer] Checking Kafka broker at {bootstrap_servers}...", flush=True)
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="stock_news_admin_cli",
            request_timeout_ms=5000
        )
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            print(f"[Admin] Topic '{topic_name}' is verified.", flush=True)
        else:
            print(f"[Admin] Topic '{topic_name}' not found. Creating topic...", flush=True)
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"[Admin] Topic '{topic_name}' created successfully.", flush=True)
        admin_client.close()
    except TopicAlreadyExistsError:
        print(f"[Admin] Topic '{topic_name}' already exists.", flush=True)
    except Exception as exc:
        print(f"[Admin Notice] Topic check/creation result: {exc}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Headless Real-Time Stock News Kafka Producer")
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
        help=f"Kafka target topic (default: {DEFAULT_TOPIC})"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("PUBLISH_INTERVAL_SECONDS", str(DEFAULT_INTERVAL))),
        help=f"Publish interval in seconds (default: {DEFAULT_INTERVAL}s)"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.getenv("MAX_PRODUCER_MESSAGES", "0")),
        help="Maximum messages to produce before stopping (0 = infinite)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    bootstrap_servers = args.bootstrap_servers
    topic = args.topic
    interval = max(0.1, args.interval)
    max_messages = max(0, args.max_messages)

    print("=" * 80, flush=True)
    print("[+] Real-Time Stock News Producer (Headless CLI)", flush=True)
    print(f"* Target Broker : {bootstrap_servers}", flush=True)
    print(f"* Kafka Topic   : {topic}", flush=True)
    print(f"* Interval      : {interval:.1f}s", flush=True)
    print(f"* Max Messages  : {'Infinite' if max_messages == 0 else max_messages}", flush=True)
    print("=" * 80, flush=True)

    # 1. Fast broker connectivity verification
    if not check_broker_reachable(bootstrap_servers, timeout=2.0):
        print(f"[Producer Fatal] Kafka broker at {bootstrap_servers} is unreachable.", flush=True)
        print("Please ensure Docker Kafka container is active ('docker compose up -d').", flush=True)
        sys.exit(1)

    # 2. Topic setup
    ensure_kafka_topic(bootstrap_servers, topic)

    # 3. Producer initialization
    print(f"[Producer] Connecting KafkaProducer to {bootstrap_servers}...", flush=True)
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=10000
        )
        print(f"[Producer] Producer connected successfully. Emitting news every {interval:.1f}s.", flush=True)
        print("-" * 80, flush=True)
    except Exception as exc:
        print(f"[Producer Fatal] Failed to connect to Kafka at {bootstrap_servers}: {exc}", flush=True)
        print("Please ensure Docker Kafka container is active ('docker compose up -d').", flush=True)
        sys.exit(1)

    # Graceful shutdown handler
    running = True

    def stop_signal_handler(signum, frame):
        nonlocal running
        if running:
            running = False
            print("\n[Producer] Stop signal received. Initiating graceful shutdown...", flush=True)

    signal.signal(signal.SIGINT, stop_signal_handler)
    signal.signal(signal.SIGTERM, stop_signal_handler)

    # 3. Publish loop
    sent_count = 0
    try:
        while running:
            ticker = random.choice(TICKERS)
            headline = random.choice(HEADLINE_TEMPLATES).format(ticker=ticker)
            timestamp = datetime.now(timezone.utc).isoformat()

            payload = {
                "ticker": ticker,
                "headline": headline,
                "timestamp": timestamp
            }

            producer.send(topic, value=payload)
            producer.flush()

            sent_count += 1
            print(f"[{sent_count:04d}] Sent -> {ticker}: \"{headline}\" | {timestamp}", flush=True)

            if max_messages > 0 and sent_count >= max_messages:
                print(f"[Producer] Completed target of {max_messages} messages. Exiting cleanly.", flush=True)
                break

            # Interruptible sleep for rapid reaction to Ctrl+C
            sleep_chunks = int(interval / 0.1)
            for _ in range(sleep_chunks):
                if not running:
                    break
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n[Producer] KeyboardInterrupt received.", flush=True)
    except Exception as err:
        print(f"\n[Producer Error] Unexpected exception in publish loop: {err}", flush=True)
    finally:
        print("[Producer] Flushing pending messages and closing Kafka producer...", flush=True)
        try:
            producer.flush(timeout=5)
            producer.close(timeout=5)
        except Exception:
            pass
        print(f"[Producer] Shutdown complete. Total messages published: {sent_count}.", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
