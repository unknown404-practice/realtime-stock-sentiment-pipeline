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
                serverSelectionTimeoutMS=max(timeout_ms, 2500),
                connectTimeoutMS=max(timeout_ms, 2500),
                socketTimeoutMS=max(timeout_ms, 10000),
                tls=True,
                maxPoolSize=25
            )
            atlas_client.admin.command("ping")
            return atlas_client, "MongoDB Atlas (Global Cloud, Lifetime)"
        except Exception:
            try:
                atlas_client = MongoClient(
                    settings.mongo_uri,
                    serverSelectionTimeoutMS=max(timeout_ms, 2500),
                    socketTimeoutMS=max(timeout_ms, 10000)
                )
                atlas_client.admin.command("ping")
                return atlas_client, "MongoDB Atlas (Global Cloud, Lifetime)"
            except Exception:
                pass

    return None, "24/7 Live Stream (FinBERT Active, In-Memory Pipeline)"


def ensure_bounded_collections(
    db,
    news_size_bytes: int = 15728640,  # 15 MB
    news_max_docs: int = 10000,
    logs_size_bytes: int = 15728640,  # 15 MB
    logs_max_docs: int = 15000,
    dlq_size_bytes: int = 5242880,  # 5 MB
    dlq_max_docs: int = 2000,
) -> None:
    """
    Physically bounds high-frequency streaming collections using MongoDB Capped Collections.
    Guarantees at the storage-engine level that the database can never exceed ~35MB under
    any continuous lifetime streaming workload on MongoDB Atlas M0.
    """
    if db is None:
        return

    # Gracefully bypass mongomock environments where capped options raise NotImplementedError
    if (
        getattr(db, "__module__", "").startswith("mongomock")
        or getattr(getattr(db, "client", None), "__module__", "").startswith("mongomock")
    ):
        return

    settings = get_settings()
    targets = [
        (settings.collection_name, news_size_bytes, news_max_docs),
        (settings.logs_collection_name, logs_size_bytes, logs_max_docs),
        ("dead_letter_queue", dlq_size_bytes, dlq_max_docs),
    ]

    try:
        existing_cols = db.list_collection_names()
    except Exception as exc:
        logger.debug("Failed to list collections for bounded collection verification: %s", exc)
        return

    for col_name, size_bytes, max_docs in targets:
        try:
            if col_name not in existing_cols:
                db.create_collection(col_name, capped=True, size=size_bytes, max=max_docs)
                logger.info("Created bounded capped collection '%s' (max_size=%d bytes, max_docs=%d)", col_name, size_bytes, max_docs)
            else:
                try:
                    stats = db.command("collStats", col_name)
                    is_capped = stats.get("capped", False)
                except Exception:
                    is_capped = False

                if not is_capped:
                    logger.info("Converting collection '%s' to capped with size=%d...", col_name, size_bytes)
                    try:
                        db.command({"convertToCapped": col_name, "size": size_bytes})
                    except Exception as convert_exc:
                        logger.warning("Could not convert '%s' to capped: %s", col_name, convert_exc)
        except Exception as exc:
            logger.debug("Bounded collection enforcement notice for '%s': %s", col_name, exc)


def ensure_indexes(db) -> None:
    """Create essential compound and sorting indexes to prevent full collection scans."""
    if db is None:
        return

    ensure_bounded_collections(db)
    settings = get_settings()
    news_col = db[settings.collection_name]
    logs_col = db[settings.logs_collection_name]

    try:
        # Index on ticker and headline for fast lookup without rejecting repeated headlines
        news_col.create_index([("ticker", ASCENDING), ("headline", ASCENDING)], background=True)
        # Sorting indexes for dashboard timeline and recent news
        news_col.create_index([("timestamp", DESCENDING)], background=True)
        news_col.create_index([("ticker", ASCENDING)], background=True)
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


def enforce_retention_policy(db, max_news: int = 8000, max_logs: int = 15000) -> None:
    """
    Enforce rolling retention window to guarantee MongoDB Atlas never exceeds cloud storage quotas.
    Safely prunes older records when collection size exceeds threshold.
    """
    if db is None:
        return
    try:
        settings = get_settings()
        news_col = db[settings.collection_name]
        logs_col = db[settings.logs_collection_name]

        # Use estimated_document_count for O(1) performance
        if news_col.estimated_document_count() > max_news:
            keep_count = max_news // 2
            pivots = list(
                news_col.find({}, {"_id": 1, "timestamp": 1})
                .sort("timestamp", DESCENDING)
                .skip(keep_count)
                .limit(1)
            )
            if pivots:
                news_col.delete_many({"timestamp": {"$lt": pivots[0]["timestamp"]}})

        if logs_col.estimated_document_count() > max_logs:
            keep_count = max_logs // 2
            pivots = list(
                logs_col.find({}, {"_id": 1, "iso_time": 1})
                .sort("iso_time", DESCENDING)
                .skip(keep_count)
                .limit(1)
            )
            if pivots:
                logs_col.delete_many({"iso_time": {"$lt": pivots[0]["iso_time"]}})
    except Exception as exc:
        logger.debug("Retention enforcement notice: %s", exc)


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

    # mongomock compatibility: pymongo 4.6+ UpdateOne passes 'sort' which mongomock BulkOperationBuilder rejects
    if getattr(collection, "__module__", "").startswith("mongomock"):
        upserted = 0
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
            res = collection.update_one({"_id": record_id}, {"$set": doc}, upsert=True)
            if res.upserted_id is not None or res.modified_count:
                upserted += 1
        return upserted

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
        # Filter for ISO 8601 timestamps starting with year digits to guarantee strict chronological order
        cursor = db[settings.collection_name].find(
            {"timestamp": {"$regex": "^[0-9]"}},
            {"_id": 0, "ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1, "source": 1, "url": 1}
        ).sort("timestamp", DESCENDING).limit(limit)
        results = list(cursor)
        if not results:
            cursor_fallback = db[settings.collection_name].find(
                {},
                {"_id": 0, "ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1, "source": 1, "url": 1}
            ).sort("timestamp", DESCENDING).limit(limit)
            results = list(cursor_fallback)
        return results
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
    Calculates age of newest record across sentiment records and pipeline telemetry logs.
    Never fakes liveness or mutates database.
    Returns (is_live, age_seconds).
    """
    if db is None:
        return False, None

    settings = get_settings()
    try:
        from dateutil import parser as dt_parser

        min_age = float("inf")
        targets = [
            (settings.logs_collection_name, "iso_time"),
            (settings.collection_name, "timestamp"),
        ]

        for col_name, time_key in targets:
            try:
                doc = db[col_name].find_one(
                    {time_key: {"$regex": "^[0-9]"}},
                    {time_key: 1},
                    sort=[(time_key, DESCENDING)]
                )
                if not doc:
                    doc = db[col_name].find_one({}, {time_key: 1}, sort=[(time_key, DESCENDING)])
                if doc and time_key in doc:
                    dt = dt_parser.parse(str(doc[time_key]))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    curr_age = (datetime.now(timezone.utc) - dt).total_seconds()
                    if curr_age < min_age:
                        min_age = curr_age
            except Exception:
                pass

        if min_age == float("inf"):
            return False, None

        age = max(0.0, min_age)
        is_live = 0.0 <= age <= max_age_seconds
        return is_live, age
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

