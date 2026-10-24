import json
import os
import sys

def make_notebook(cells, metadata=None):
    if metadata is None:
        metadata = {
            "kernelspec": {
                "display_name": "Python (Stock Sentiment)",
                "language": "python",
                "name": "stock-sentiment-pipeline"
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.10.11"
            }
        }
    return {
        "cells": cells,
        "metadata": metadata,
        "nbformat": 4,
        "nbformat_minor": 5
    }

def md_cell(source_lines, cell_id):
    if isinstance(source_lines, str):
        source_lines = [line + "\n" for line in source_lines.splitlines()]
    return {
        "cell_type": "markdown",
        "id": cell_id,
        "metadata": {},
        "source": source_lines
    }

def code_cell(source_lines, cell_id):
    if isinstance(source_lines, str):
        source_lines = [line + "\n" for line in source_lines.splitlines()]
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": cell_id,
        "metadata": {},
        "outputs": [],
        "source": source_lines
    }

def build_producer_notebook():
    cells = [
        md_cell("""# 📈 Real-Time Stock News Producer & Stream EDA
Exploratory Data Analysis, Live Market News Ingestion, and Kafka Streaming Generation.

---

### 🔍 Architectural Overview:
1. **Real Ingestion**: Connects to the Finnhub Financial News API and Yahoo Finance RSS feeds via `src.ingestion.news_fetcher.NewsFetcher`.
2. **Entity & Ticker Extraction**: Dynamically identifies mentioned ticker symbols and company names in market news.
3. **Partition Key Routing**: Assigns `key=ticker.encode('utf-8')` to route related stock headlines to deterministic Kafka partitions.
4. **Asynchronous Publishing**: Utilizes `linger_ms=20` batching, `gzip` compression, and non-blocking delivery callbacks without synchronous flush bottlenecks.
""", "cell-producer-intro"),
        code_cell("""import sys
import os
import json
import time
from datetime import datetime, timezone

# Ensure project root is on sys.path
if "." not in sys.path:
    sys.path.insert(0, ".")

from src.config import config
from src.ingestion.news_fetcher import NewsFetcher

print("Configuration Loaded:")
print(f"  Kafka Bootstrap: {config.KAFKA_BOOTSTRAP_SERVERS}")
print(f"  Kafka Topic:     {config.KAFKA_TOPIC}")
print(f"  Partitions:      {config.KAFKA_PARTITIONS}")
print(f"  Default Tickers: {config.DEFAULT_TICKERS}")

fetcher = NewsFetcher(finnhub_api_key=config.FINNHUB_API_KEY)
print(f"NewsFetcher initialized with {len(fetcher.fallback_pool)} curated fallback articles.")
""", "cell-producer-setup"),
        md_cell("""## 1. Live Market News Ingestion & Ticker Analysis
Fetch headlines across target tickers, inspect article metadata, and review ticker distributions.
""", "cell-producer-eda-intro"),
        code_cell("""print("Fetching market news articles...")
articles = fetcher.fetch_all(limit_per_ticker=2)
print(f"Total articles retrieved: {len(articles)}")

if articles:
    print("\\nSample Article Schema:")
    for k, v in articles[0].items():
        print(f"  {k}: {v}")

from collections import Counter
ticker_counts = Counter(a["ticker"] for a in articles)
source_counts = Counter(a.get("source", "unknown") for a in articles)

print("\\nArticles per Ticker:")
for t, c in ticker_counts.items():
    print(f"  {t}: {c}")

print("\\nArticles per Source:")
for s, c in source_counts.items():
    print(f"  {s}: {c}")
""", "cell-producer-fetch"),
        md_cell("""## 2. Kafka Partition Key Distribution Analysis
In Apache Kafka, messages with the same partition key are guaranteed to route to the same partition, preserving causal order per stock ticker.
Here we simulate murmur2 partition hashing across our 4 configured topic partitions.
""", "cell-producer-partitions-intro"),
        code_cell("""def murmur2(data: bytes) -> int:
    m = 0x5bd1e995
    r = 24
    length = len(data)
    h = 0 ^ length
    for i in range(0, length & ~3, 4):
        k = data[i] | (data[i + 1] << 8) | (data[i + 2] << 16) | (data[i + 3] << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= (k >> r)
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    rem = length & 3
    if rem >= 3:
        h ^= data[length - 1] << 16
    if rem >= 2:
        h ^= data[length - (rem - 1)] << 8
    if rem >= 1:
        h ^= data[length - rem]
        h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 13)
    h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 15)
    return (h & 0x7FFFFFFF)

num_partitions = config.KAFKA_PARTITIONS
print(f"Simulating Kafka partition routing across {num_partitions} partitions:")
partition_map = {}
for ticker in config.DEFAULT_TICKERS:
    part = murmur2(ticker.encode("utf-8")) % num_partitions
    partition_map.setdefault(part, []).append(ticker)

for part in range(num_partitions):
    tickers_in_part = partition_map.get(part, [])
    print(f"  Partition {part}: {len(tickers_in_part)} tickers -> {tickers_in_part}")
""", "cell-producer-partitions-code"),
        md_cell("""## 3. Streaming Producer Execution with Non-Blocking Callbacks
Demonstrating async publication using `producer.py`. If Kafka is offline, falls back to a dry-run demonstration.
""", "cell-producer-stream-intro"),
        code_cell("""from producer import create_producer, publish_stream

try:
    producer = create_producer()
    print("Connected to Kafka broker! Publishing 5 sample events with callbacks...")
    publish_stream(producer=producer, max_messages=5, interval=1.0)
except Exception as e:
    print(f"Kafka broker not reachable ({e}).")
    print("Demonstrating structured dry-run payload generation:")
    for ticker in config.DEFAULT_TICKERS[:5]:
        event = fetcher.generate_event(ticker)
        print(f"  [DRY-RUN] Ticker={event['ticker']} | Timestamp={event['timestamp']} | Headline={event['headline'][:65]}...")
""", "cell-producer-stream-code")
    ]
    return make_notebook(cells)

def build_consumer_notebook():
    cells = [
        md_cell("""# 🧠 Financial Sentiment Consumer & Model Benchmark (FinBERT EDA)
Local FinBERT NLP Inference, Model Comparison (FinBERT vs VADER), Vectorized Batching, and Idempotent MongoDB Persistence.

---

### 🔍 Architectural Overview:
1. **Domain-Specific FinBERT**: Utilizes `ProsusAI/finbert` fine-tuned on financial market text with 3 calibrated sentiment classes (`POSITIVE`, `NEGATIVE`, `NEUTRAL`).
2. **Vectorized Batch Inference**: Dynamically runs mini-batches (up to 16/32 headlines) through PyTorch attention tensors on CUDA, MPS, or CPU.
3. **Calibrated Softmax Probabilities**: Computes numerical confidence scores with `torch.softmax()`.
4. **Idempotent Micro-Batch Persistence**: Computes deterministic SHA-256 `_id` values and persists records using MongoDB `bulk_write` with `UpdateOne(..., upsert=True)`.
""", "cell-consumer-intro"),
        code_cell("""import sys
import os
import time
import json

if "." not in sys.path:
    sys.path.insert(0, ".")

from src.config import config
from src.ml.finbert_service import FinBERTService
from src.db.mongo import MongoRepository

print("Initializing FinBERT Service...")
service = FinBERTService()
print(f"  Model:      {service.model_name}")
print(f"  Device:     {service.device}")
print(f"  Batch Size: {service.batch_size}")
""", "cell-consumer-setup"),
        md_cell("""## 1. Domain Nuance Benchmark: FinBERT vs Heuristic Lexicon (VADER)
General-purpose sentiment tools like VADER rely on general valence dictionaries that misinterpret financial context (e.g., words like 'fell short', 'slumped', 'cuts', 'expanded').
FinBERT uses self-attention trained on corporate disclosures to accurately distinguish true market sentiment.
""", "cell-consumer-nuance-intro"),
        code_cell("""test_cases = [
    ("Company reported quarterly sales fell short of forecasts but operating margins expanded significantly.", "POSITIVE"),
    ("Federal Reserve maintains interest rates unchanged following two-day monetary policy meeting.", "NEUTRAL"),
    ("Antitrust regulators launch formal monopolization investigation into enterprise cloud contracts.", "NEGATIVE"),
    ("Firm reduces long-term debt by $2.4 billion following strategic asset divestiture.", "POSITIVE"),
    ("Shares dropped 4% in premarket trading after CFO announced unexpected retirement.", "NEGATIVE"),
    ("Board of directors authorizes new $10 billion common stock repurchase program.", "POSITIVE"),
    ("Semiconductor equipment manufacturer reports record quarterly bookings and raised forward guidance.", "POSITIVE"),
    ("Company warns of persistent foreign currency exchange headwinds impacting annual revenue.", "NEGATIVE"),
]

headlines = [tc[0] for tc in test_cases]
ground_truths = [tc[1] for tc in test_cases]

# FinBERT Inference
start = time.perf_counter()
finbert_results = service.predict_batch(headlines)
finbert_latency_ms = (time.perf_counter() - start) * 1000 / len(headlines)

# Heuristic Lexicon Simulation (representing VADER-style rules)
def heuristic_sentiment(text: str) -> str:
    text_lower = text.lower()
    pos_words = ["expanded", "record", "repurchase", "beating", "gains", "raised"]
    neg_words = ["fell short", "dropped", "warns", "investigation", "headwinds", "cuts"]
    score = sum(1 for w in pos_words if w in text_lower) - sum(1 for w in neg_words if w in text_lower)
    if score > 0:
        return "POSITIVE"
    elif score < 0:
        return "NEGATIVE"
    return "NEUTRAL"

vader_preds = [heuristic_sentiment(h) for h in headlines]
finbert_preds = [r["sentiment"] for r in finbert_results]

header = f"{'Headline':<65} | {'True':<8} | {'FinBERT':<8} | {'VADER':<8} | {'Conf':<6}"
print(header)
print("-" * len(header))
for i in range(len(headlines)):
    h_snippet = headlines[i][:62] + "..." if len(headlines[i]) > 62 else headlines[i]
    gt = ground_truths[i]
    fb = finbert_preds[i]
    vd = vader_preds[i]
    conf = finbert_results[i]["confidence"]
    print(f"{h_snippet:<65} | {gt:<8} | {fb:<8} | {vd:<8} | {conf:.4f}")

fb_acc = sum(1 for p, g in zip(finbert_preds, ground_truths) if p == g) / len(ground_truths) * 100
vd_acc = sum(1 for p, g in zip(vader_preds, ground_truths) if p == g) / len(ground_truths) * 100

print(f"\\nAccuracy on Financial Nuance Benchmark:")
print(f"  FinBERT Accuracy: {fb_acc:.1f}% (Avg Latency: {finbert_latency_ms:.2f}ms/headline)")
print(f"  VADER Accuracy:   {vd_acc:.1f}%")
""", "cell-consumer-nuance-code"),
        md_cell("""## 2. Vectorized Batch Inference Scaling Benchmark
Evaluating inference latency and throughput across batch sizes (1, 4, 8, 16).
Vectorized batching parallelizes matrix operations across transformer attention heads.
""", "cell-consumer-batch-intro"),
        code_cell("""sample_pool = headlines * 2  # 16 headlines
batch_sizes = [1, 4, 8, 16]

print(f"Benchmarking batch throughput on device: {service.device}\\n")
hdr = f"{'Batch Size':<12} | {'Total Time (ms)':<16} | {'Throughput (headlines/sec)':<28} | {'Per-Item (ms)':<14}"
print(hdr)
print("-" * len(hdr))

for bs in batch_sizes:
    batch = sample_pool[:bs]
    # Warmup
    service.predict_batch(batch[:1])
    
    start_t = time.perf_counter()
    _ = service.predict_batch(batch)
    elapsed_ms = (time.perf_counter() - start_t) * 1000
    throughput = (len(batch) / elapsed_ms) * 1000 if elapsed_ms > 0 else 0
    per_item = elapsed_ms / len(batch)
    print(f"{bs:<12} | {elapsed_ms:<16.2f} | {throughput:<28.1f} | {per_item:<14.2f}")
""", "cell-consumer-batch-code"),
        md_cell("""## 3. Micro-Batched Consumer & Idempotent MongoDB Persistence
Demonstrating micro-batch aggregation, FinBERT enrichment, deterministic SHA-256 IDs, and bulk upsert into MongoDB.
""", "cell-consumer-persistence-intro"),
        code_cell("""from consumer import process_batch
from datetime import datetime, timezone

mock_records = [
    {"ticker": "NVDA", "headline": "NVIDIA unveils next-gen Blackwell GPU architecture for hyperscale AI datacenters.", "timestamp": datetime.now(timezone.utc).isoformat()},
    {"ticker": "AAPL", "headline": "Apple expands enterprise health initiatives with new sensor patents.", "timestamp": datetime.now(timezone.utc).isoformat()},
    {"ticker": "TSLA", "headline": "Tesla delivers record quarterly energy storage deployments surpassing 4 GWh.", "timestamp": datetime.now(timezone.utc).isoformat()},
]

print("Processing mock micro-batch through consumer pipeline...")
repo = MongoRepository()
count = process_batch(mock_records, service=service, repo_or_db=repo)
print(f"Bulk persistence complete: Upserted records = {count}")
""", "cell-consumer-persistence-code")
    ]
    return make_notebook(cells)

if __name__ == "__main__":
    p_nb = build_producer_notebook()
    with open("Producer.ipynb", "w", encoding="utf-8") as f:
        json.dump(p_nb, f, indent=2)
    print("Producer.ipynb written successfully.")

    c_nb = build_consumer_notebook()
    with open("Consumer.ipynb", "w", encoding="utf-8") as f:
        json.dump(c_nb, f, indent=2)
    print("Consumer.ipynb written successfully.")
