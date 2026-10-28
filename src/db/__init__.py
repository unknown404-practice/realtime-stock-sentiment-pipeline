"""
Database layer for MongoDB connection and repository operations.
"""

from src.db.mongo import (
    MongoRepository,
    check_stream_heartbeat,
    enforce_retention_policy,
    ensure_bounded_collections,
    ensure_indexes,
    fetch_pipeline_logs,
    fetch_sentiment_records,
    get_mongo_client,
    insert_dlq_message,
    insert_pipeline_log,
    upsert_sentiment_batch,
)

__all__ = [
    "MongoRepository",
    "check_stream_heartbeat",
    "enforce_retention_policy",
    "ensure_bounded_collections",
    "ensure_indexes",
    "fetch_pipeline_logs",
    "fetch_sentiment_records",
    "get_mongo_client",
    "insert_dlq_message",
    "insert_pipeline_log",
    "upsert_sentiment_batch",
]
