"""
Instantaneous Collection Compact & Prune:
Preserves the newest records, drops the bloated collection, reinserts preserved records,
and re-applies all indexes to instantly reclaim ~200MB of Atlas storage.
"""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings
from pymongo import MongoClient, DESCENDING
from src.db.mongo import ensure_indexes

def compact_and_prune(keep_news: int = 5000, keep_logs: int = 5000):
    settings = get_settings()
    import os
    uri = settings.mongo_uri or os.getenv("MONGO_URI")
    if not uri:
        print("No MONGO_URI provided in environment or settings. Exiting.")
        return
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=30000)
    db = client[settings.db_name]
    print(f"Connected to Atlas {settings.db_name}")

    # 1. Compact news_sentiment
    print("Reading newest news records...")
    news_col = db[settings.collection_name]
    newest_news = list(news_col.find().sort("timestamp", DESCENDING).limit(keep_news))
    print(f"Preserved {len(newest_news)} newest news records.")
    
    if len(newest_news) > 0 and news_col.estimated_document_count() > keep_news:
        print("Dropping news collection to reclaim storage...")
        news_col.drop()
        print("Reinserting preserved news records...")
        db[settings.collection_name].insert_many(newest_news)
        print("Recreating news indexes...")

    # 2. Compact pipeline_logs
    print("Reading newest pipeline logs...")
    logs_col = db[settings.logs_collection_name]
    newest_logs = list(logs_col.find().sort("iso_time", DESCENDING).limit(keep_logs))
    print(f"Preserved {len(newest_logs)} newest logs.")
    
    if len(newest_logs) > 0 and logs_col.estimated_document_count() > keep_logs:
        print("Dropping logs collection to reclaim storage...")
        logs_col.drop()
        print("Reinserting preserved logs...")
        db[settings.logs_collection_name].insert_many(newest_logs)
        print("Recreating log indexes...")

    ensure_indexes(db)
    print("Compaction complete! Verifying counts:")
    print("news_sentiment:", db[settings.collection_name].count_documents({}))
    print("pipeline_logs:", db[settings.logs_collection_name].count_documents({}))

if __name__ == "__main__":
    compact_and_prune()
