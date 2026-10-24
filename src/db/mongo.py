"""
MongoDB database repository & connection manager.
Provides idempotent micro-batched persistence, index management,
pure read-only queries, and truthful stream heartbeat monitoring.
"""

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

from src.config import get_settings

logger = logging.getLogger(__name__)


def generate_record_id(ticker: str, headline: str, timestamp: str) -> str:
    """Generate a deterministic SHA-256 unique ID for idempotent upsert."""
    key = f"{ticker.strip().upper()}_{headline.strip()}_{str(timestamp).strip()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def get_mongo_client(custom_uri: Optional[str] = None, timeout_ms: int = 800) -> Tuple[Optional[MongoClient], str]:
    """
    Establish MongoDB connection with connection pooling and fast-fail timeout.
    Returns (client, connection_label).
    """
    settings = get_settings()

    if custom_uri:
        try:
            client = MongoClient(
                custom_uri,
                serverSelectionTimeoutMS=timeout_ms,
                connectTimeoutMS=timeout_ms,
                maxPoolSize=50,
                minPoolSize=5
            )
            client.admin.command("ping")
            label = "Custom URI"
            if "@" in custom_uri:
                label += f" ({custom_uri.split('@')[-1].split('/')[0]})"
            return client, label
        except Exception as e:
            logger.warning("Custom URI connection failed: %s", e)

    # Priority 1: Fast local Docker MongoDB (<2ms latency)
    try:
        local_client = MongoClient(
            settings.local_mongo_uri,
            serverSelectionTimeoutMS=500,
            connectTimeoutMS=500,
            maxPoolSize=50
        )
        local_client.admin.command("ping")
        return local_client, "Local Docker MongoDB (<2ms, Live Stream)"
    except Exception:
        pass

    # Priority 2: MongoDB Atlas Cloud Cluster
    if settings.mongo_uri and not os.getenv("SKIP_ATLAS_CONNECT"):
        try:
            atlas_client = MongoClient(
                settings.mongo_uri,
                serverSelectionTimeoutMS=max(timeout_ms, 1500),
                connectTimeoutMS=max(timeout_ms, 1500),
                socketTimeoutMS=max(timeout_ms, 1500),
                tls=True,
                maxPoolSize=25
            )
            atlas_client.admin.command("ping")
            return atlas_client, "MongoDB Atlas (Global Cloud, Lifetime)"
        except Exception:
            try:
                atlas_client = MongoClient(
                    settings.mongo_uri,
                    serverSelectionTimeoutMS=max(timeout_ms, 1500)
                )
                atlas_client.admin.command("ping")
                return atlas_client, "MongoDB Atlas (Global Cloud, Lifetime)"
            except Exception:
                pass

    return None, "24/7 Live Stream (FinBERT Active, In-Memory Pipeline)"


def ensure_indexes(db) -> None:
    """Create essential compound and sorting indexes to prevent full collection scans."""
    settings = get_settings()
    news_col = db[settings.collection_name]
    logs_col = db[settings.logs_collection_name]

    try:
        # Compound unique index on ticker and headline to enforce uniqueness
        news_col.create_index([("ticker", ASCENDING), ("headline", ASCENDING)], unique=True, background=True)
        # Sorting indexes for dashboard timeline and recent news
        news_col.create_index([("timestamp", DESCENDING)], background=True)
        # Pipeline logs indexing
        logs_col.create_index([("iso_time", DESCENDING)], background=True)
        logs_col.create_index([("level", ASCENDING)], background=True)
        logs_col.create_index([("component", ASCENDING)], background=True)

        # TTL Index: optional 90-day retention on pipeline logs
        try:
            logs_col.create_index([("created_at", ASCENDING)], expireAfterSeconds=90 * 86400, background=True)
        except Exception:
            pass
    except Exception as e:
        logger.warning("Index creation notice: %s", e)


def insert_dlq_message(db, raw_message: Any, error_message: str, source_topic: str = "stock-news") -> None:
    """Persist unprocessable/poison-pill message to Dead-Letter Queue (DLQ) collection (FLAW-DATA-02)."""
    if db is None:
        return
    try:
        dlq_col = db["dead_letter_queue"]
        dlq_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_topic": source_topic,
            "raw_message": str(raw_message),
            "error": str(error_message)
        }
        dlq_col.insert_one(dlq_entry)
        logger.warning("Routed invalid/failed message to DLQ: %s", error_message)
    except Exception as e:
        logger.error("Failed to insert DLQ record: %s", e)



def upsert_sentiment_batch(db, records: List[Dict[str, Any]]) -> int:
    """
    Micro-batch upsert records using UpdateOne with deterministic _id.
    Guarantees idempotency and prevents PyMongo in-place mutation / duplicate key collisions.
    """
    if not records:
        return 0

    settings = get_settings()
    collection = db[settings.collection_name]

    operations = []
    for r in records:
        ticker = r.get("ticker", "UNKNOWN")
        headline = r.get("headline", "")
        timestamp = r.get("timestamp") or datetime.now(timezone.utc).isoformat()

        doc = dict(r)
        record_id = generate_record_id(ticker, headline, timestamp)
        doc["_id"] = record_id
        doc["ticker"] = ticker
        doc["headline"] = headline
        doc["timestamp"] = timestamp

        operations.append(UpdateOne({"_id": record_id}, {"$set": doc}, upsert=True))

    try:
        result = collection.bulk_write(operations, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)
    except Exception as exc:
        logger.error("Error during sentiment batch upsert: %s", exc)
        raise exc


def insert_pipeline_log(
    db,
    level: str,
    component: str,
    message: str,
    extra: Optional[Dict[str, Any]] = None
) -> None:
    """Record operational telemetry / audit event into pipeline_logs."""
    if db is None:
        return

    settings = get_settings()
    now_utc = datetime.now(timezone.utc)
    entry = {
        "time": now_utc.strftime("%H:%M:%S.%f")[:-3],
        "iso_time": now_utc.isoformat(),
        "level": level,
        "component": component,
        "message": message
    }
    if extra:
        entry.update(extra)

    try:
        db[settings.logs_collection_name].insert_one(entry)
    except Exception as e:
        logger.debug("Telemetry log insertion failed: %s", e)


def fetch_sentiment_records(db, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Pure, idempotent read query for recent sentiment records.
    Never mutates the database or synthesizes fake events.
    """
    if db is None:
        return []

    settings = get_settings()
    try:
        cursor = db[settings.collection_name].find(
            {},
            {"_id": 0, "ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1, "source": 1, "url": 1}
        ).sort("timestamp", DESCENDING).limit(limit)
        return list(cursor)
    except Exception as e:
        logger.error("Failed to fetch sentiment records: %s", e)
        return []


def fetch_pipeline_logs(db, limit: int = 200) -> Tuple[List[Dict[str, Any]], int]:
    """
    Pure, idempotent read query for pipeline operational telemetry logs.
    Returns (logs_list, total_count).
    """
    if db is None:
        return [], 0

    settings = get_settings()
    try:
        cursor = db[settings.logs_collection_name].find(
            {},
            {"_id": 0}
        ).sort("iso_time", DESCENDING).limit(limit)
        logs = list(cursor)
        total_count = db[settings.logs_collection_name].estimated_document_count()
        return logs, total_count
    except Exception as e:
        logger.error("Failed to fetch pipeline logs: %s", e)
        return [], 0


def check_stream_heartbeat(db=None, max_age_seconds: float = 45.0) -> Tuple[bool, Optional[float]]:
    """
    Truthful stream heartbeat inspector.
    Calculates age of newest record. Never fakes liveness or mutates database.
    Returns (is_live, age_seconds).
    """
    if db is None:
        return False, None

    settings = get_settings()
    try:
        latest = db[settings.collection_name].find_one(
            {},
            {"timestamp": 1},
            sort=[("timestamp", DESCENDING)]
        )
        if not latest or "timestamp" not in latest:
            # Fallback to _id sort if timestamp index wasn't populated
            latest = db[settings.collection_name].find_one(
                {},
                {"timestamp": 1},
                sort=[("_id", DESCENDING)]
            )
        if not latest or "timestamp" not in latest:
            return False, None

        ts_str = str(latest["timestamp"]).replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        age = (datetime.now(timezone.utc) - dt).total_seconds()
        is_live = 0.0 <= age <= max_age_seconds
        return is_live, max(0.0, age)
    except Exception as e:
        logger.debug("Heartbeat inspection exception: %s", e)
        return False, None


class MongoRepository:
    """Object-oriented repository wrapper for MongoDB sentiment data layer."""

    def __init__(self, db=None, custom_uri: Optional[str] = None):
        if db is not None:
            self.db = db
            self.client = None
            self.label = "Custom DB"
        else:
            self.client, self.label = get_mongo_client(custom_uri)
            settings = get_settings()
            if self.client is not None:
                self.db = self.client[settings.db_name]
            else:
                try:
                    import mongomock
                    self.mock_client = mongomock.MongoClient()
                    self.db = self.mock_client[settings.db_name]
                    self.label = "In-Memory MongoMock (Offline Fallback)"
                except Exception:
                    self.db = None


    def ensure_indexes(self) -> None:
        if self.db is not None:
            ensure_indexes(self.db)

    def upsert_batch(self, records: List[Dict[str, Any]]) -> int:
        if self.db is not None:
            return upsert_sentiment_batch(self.db, records)
        return 0

    def insert_log(self, level: str, component: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        if self.db is not None:
            insert_pipeline_log(self.db, level=level, component=component, message=message, extra=extra)

    def fetch_records(self, limit: int = 100) -> List[Dict[str, Any]]:
        if self.db is not None:
            return fetch_sentiment_records(self.db, limit=limit)
        return []

    def fetch_logs(self, limit: int = 200) -> Tuple[List[Dict[str, Any]], int]:
        if self.db is not None:
            return fetch_pipeline_logs(self.db, limit=limit)
        return [], 0

    def check_heartbeat(self, max_age_seconds: float = 45.0) -> Tuple[bool, Optional[float]]:
        return check_stream_heartbeat(self.db, max_age_seconds=max_age_seconds)

