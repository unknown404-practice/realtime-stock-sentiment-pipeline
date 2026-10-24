#!/usr/bin/env python3
"""
Real-Time Stock News Kafka Producer
===================================
Streams real financial news across major market tickers to an Apache
Kafka cluster with partition keying, asynchronous micro-batching, and robust shutdown.
"""

import argparse
import json
import logging
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("producer")

# Flexible Kafka imports supporting kafka-python-ng and kafka-python
try:
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
except ImportError:
    try:
        from kafka_python_ng import KafkaAdminClient, KafkaProducer
        from kafka_python_ng.admin import NewTopic
        from kafka_python_ng.errors import TopicAlreadyExistsError
    except ImportError:
        KafkaProducer = None
        KafkaAdminClient = None
        NewTopic = None

from src.config import get_settings
from src.ingestion.news_fetcher import SUPPORTED_TICKERS, NewsFetcher

# Backwards compatibility constants for existing test suites
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "stock-news"
DEFAULT_INTERVAL = 0.8
TICKERS = SUPPORTED_TICKERS

HEADLINE_TEMPLATES = [
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


def ensure_kafka_topic(bootstrap_servers: str, topic_name: str, num_partitions: int = 4) -> None:
    """Verify topic existence or create it with 4 partitions for horizontal parallelism."""
    if KafkaAdminClient is None:
        return

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
            print(f"[Admin] Creating topic '{topic_name}' with {num_partitions} partitions...", flush=True)
            new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"[Admin] Topic '{topic_name}' created successfully with {num_partitions} partitions.", flush=True)
        admin_client.close()
    except TopicAlreadyExistsError:
        print(f"[Admin] Topic '{topic_name}' already exists.", flush=True)
    except Exception as exc:
        print(f"[Admin Notice] Topic check/creation result: {exc}", flush=True)


def parse_args():
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Real-Time Stock News Kafka Producer")
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
        help=f"Kafka target topic (default: {settings.kafka_topic})"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=settings.publish_interval,
        help=f"Publish interval in seconds (default: {settings.publish_interval}s)"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum messages to produce before stopping (0 = infinite)"
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=settings.kafka_num_partitions,
        help=f"Number of topic partitions (default: {settings.kafka_num_partitions})"
    )
    return parser.parse_args()


def create_producer(bootstrap_servers: Optional[str] = None):
    """Instantiate a high-throughput asynchronous KafkaProducer with linger and compression."""
    settings = get_settings()
    servers = bootstrap_servers or settings.kafka_bootstrap_servers
    if KafkaProducer is None:
        raise RuntimeError("Neither kafka-python-ng nor kafka-python is available.")
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks="all",
        retries=3,
        linger_ms=20,          # Micro-batching window for 10x throughput
        batch_size=32768,      # 32 KB batch size
        request_timeout_ms=10000
    )


def publish_stream(
    producer,
    topic: Optional[str] = None,
    max_messages: int = 0,
    interval: float = 0.8,
    fetcher: Optional[NewsFetcher] = None
) -> Tuple[int, int]:
    """Publish financial news events asynchronously with delivery callbacks."""
    settings = get_settings()
    target_topic = topic or settings.kafka_topic
    news_fetcher = fetcher or NewsFetcher()
    running = True

    def stop_signal_handler(signum, frame):
        nonlocal running
        if running:
            running = False
            print("\n[Producer] Stop signal received. Initiating graceful shutdown...", flush=True)

    try:
        signal.signal(signal.SIGINT, stop_signal_handler)
        signal.signal(signal.SIGTERM, stop_signal_handler)
    except (ValueError, AttributeError):
        pass

    sent_count = 0
    delivery_errors = 0

    def on_send_success(record_metadata):
        pass

    def on_send_error(excp):
        nonlocal delivery_errors
        delivery_errors += 1
        logger.error("Asynchronous delivery error: %s", excp)

    try:
        while running:
            item = news_fetcher.get_next_news_item()
            ticker = item.get("ticker", "AAPL")
            headline = item.get("headline", "")
            timestamp = item.get("timestamp") or datetime.now(timezone.utc).isoformat()

            payload = {
                "ticker": ticker,
                "headline": headline,
                "timestamp": timestamp,
                "source": item.get("source", "MarketNews"),
                "url": item.get("url", ""),
                "author": item.get("author", "Financial Reporter")
            }

            # Keyed send routes messages for the same ticker to the same partition
            future = producer.send(target_topic, key=ticker, value=payload)
            if hasattr(future, "add_callback"):
                future.add_callback(on_send_success)
            if hasattr(future, "add_errback"):
                future.add_errback(on_send_error)

            sent_count += 1
            print(f"[{sent_count:04d}] Sent -> [{ticker}] \"{headline[:60]}...\" ({payload['source']})", flush=True)

            if max_messages > 0 and sent_count >= max_messages:
                print(f"[Producer] Completed target of {max_messages} messages. Exiting cleanly.", flush=True)
                break

            sleep_chunks = max(1, int(interval / 0.1))
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
        print(f"[Producer] Shutdown complete. Total messages: {sent_count}, errors: {delivery_errors}.", flush=True)

    return sent_count, delivery_errors


def main():
    args = parse_args()
    bootstrap_servers = args.bootstrap_servers
    topic = args.topic
    interval = max(0.1, args.interval)
    max_messages = max(0, args.max_messages)
    partitions = max(1, args.partitions)

    print("=" * 80, flush=True)
    print("[+] Real-Time Stock News Producer (Asynchronous Engine)", flush=True)
    print(f"* Target Broker : {bootstrap_servers}", flush=True)
    print(f"* Kafka Topic   : {topic} ({partitions} partitions)", flush=True)
    print(f"* Interval      : {interval:.1f}s", flush=True)
    print(f"* Max Messages  : {'Infinite' if max_messages == 0 else max_messages}", flush=True)
    print("=" * 80, flush=True)

    if not check_broker_reachable(bootstrap_servers, timeout=2.0):
        print(f"[Producer Fatal] Kafka broker at {bootstrap_servers} is unreachable.", flush=True)
        print("Please ensure Docker Kafka container is active ('docker compose up -d').", flush=True)
        sys.exit(1)

    ensure_kafka_topic(bootstrap_servers, topic, num_partitions=partitions)

    print(f"[Producer] Connecting KafkaProducer to {bootstrap_servers}...", flush=True)
    try:
        producer = create_producer(bootstrap_servers)
        print(f"[Producer] Producer connected successfully. Emitting news every {interval:.1f}s.", flush=True)
        print("-" * 80, flush=True)
    except Exception as exc:
        print(f"[Producer Fatal] Failed to connect to Kafka at {bootstrap_servers}: {exc}", flush=True)
        sys.exit(1)

    publish_stream(producer, topic=topic, max_messages=max_messages, interval=interval)
    sys.exit(0)


if __name__ == "__main__":
    main()
