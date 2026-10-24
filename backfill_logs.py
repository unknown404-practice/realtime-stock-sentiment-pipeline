"""
backfill_logs.py
================
Utility to synchronize / backfill existing records from `StockDB.news_sentiment`
into `StockDB.pipeline_logs` for lifetime durability and instant historical visibility.
Supports both Local Docker MongoDB (primary high-speed) and MongoDB Atlas.
"""

from datetime import datetime, timezone

from pymongo import ASCENDING, DESCENDING, MongoClient

from src.config import get_settings


def get_mongo_client():
    settings = get_settings()
    local_uri = settings.local_mongo_uri
    try:
        c = MongoClient(local_uri, serverSelectionTimeoutMS=500)
        c.admin.command("ping")
        return c, local_uri, "Local Docker MongoDB (<2ms)"
    except Exception:
        pass

    if settings.mongo_uri:
        try:
            c = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
            c.admin.command("ping")
            return c, settings.mongo_uri, "MongoDB Atlas (Remote Cloud)"
        except Exception:
            pass

    return MongoClient(local_uri, serverSelectionTimeoutMS=1000), local_uri, "Fallback MongoDB"

def backfill_pipeline_logs(client=None, db_name="StockDB", force_target_uri=None):
    if force_target_uri:
        c = MongoClient(force_target_uri, serverSelectionTimeoutMS=2000)
        label = force_target_uri.split("@")[-1]
    elif client is not None:
        c = client
        label = "Custom Client"
    else:
        c, _, label = get_mongo_client()

    print(f"[Backfill] Target MongoDB: {label}")
    db = c[db_name]

    sentiment_col = db["news_sentiment"]
    logs_col = db["pipeline_logs"]

    # Ensure indexes
    print("[Backfill] Ensuring indexes on 'pipeline_logs'...")
    logs_col.create_index([("iso_time", DESCENDING)])
    logs_col.create_index([("level", ASCENDING)])
    logs_col.create_index([("component", ASCENDING)])

    total_sentiment = sentiment_col.count_documents({})
    total_logs = logs_col.count_documents({})
    print(f"[Backfill] Total news_sentiment records: {total_sentiment}")
    print(f"[Backfill] Current pipeline_logs records: {total_logs}")

    if total_logs >= total_sentiment and total_sentiment > 0:
        print("[Backfill] pipeline_logs is already up to date. No backfill needed.")
        return total_logs

    # Fetch existing sentiment documents in chronological order
    print(f"[Backfill] Fetching {total_sentiment} news_sentiment records for synchronization...")
    cursor = sentiment_col.find(
        {},
        {"ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1}
    ).sort("_id", ASCENDING)

    batch = []
    inserted_count = 0
    BATCH_SIZE = 1000

    existing_iso_times = set()
    if total_logs > 0:
        for doc in logs_col.find({}, {"iso_time": 1}):
            if "iso_time" in doc:
                existing_iso_times.add(doc["iso_time"])

    for doc in cursor:
        iso_time = doc.get("timestamp") or datetime.now(timezone.utc).isoformat()
        if iso_time in existing_iso_times:
            continue

        try:
            dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
            time_str = dt.strftime("%H:%M:%S.%f")[:-3]
        except Exception:
            time_str = iso_time[11:23] if len(iso_time) >= 23 else "00:00:00.000"

        ticker = doc.get("ticker", "TICKER")
        sentiment = doc.get("sentiment", "NEUTRAL")
        confidence = float(doc.get("confidence", 0.0))
        headline = doc.get("headline", "")
        preview = (headline[:55] + "...") if len(headline) > 55 else headline

        batch.append({
            "time": time_str,
            "iso_time": iso_time,
            "level": "INFO",
            "component": "HISTORICAL_BACKFILL",
            "message": f"Backfilled [{sentiment}] {ticker}: \"{preview}\" (conf: {confidence:.4f})",
            "ticker": ticker,
            "sentiment": sentiment,
            "confidence": confidence,
            "headline": headline
        })

        if len(batch) >= BATCH_SIZE:
            logs_col.insert_many(batch)
            inserted_count += len(batch)
            print(f"[Backfill] Synchronized {inserted_count}/{total_sentiment} logs...")
            batch = []

    if batch:
        logs_col.insert_many(batch)
        inserted_count += len(batch)

    final_count = logs_col.count_documents({})
    print(f"✅ [Backfill Complete] Inserted {inserted_count} new logs. Total pipeline_logs: {final_count}")
    return final_count

if __name__ == "__main__":
    backfill_pipeline_logs()
