#!/usr/bin/env python3
"""
Sync Local MongoDB Lifetime Data to MongoDB Atlas
=================================================
Migrates news_sentiment records and pipeline_logs from local Docker MongoDB
to MongoDB Atlas cloud cluster so global users on Streamlit Community Cloud
immediately see the full lifetime historical dataset.
"""

import sys
import os
from pymongo import MongoClient, DESCENDING, ReplaceOne

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

def resolve_atlas_uri():
    if os.getenv("MONGO_URI"):
        return os.getenv("MONGO_URI")
    secrets_path = os.path.join(".streamlit", "secrets.toml")
    if os.path.exists(secrets_path):
        try:
            with open(secrets_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith("MONGO_URI"):
                        parts = line.split("=", 1)
                        if len(parts) == 2:
                            return parts[1].strip().strip('"').strip("'")
        except Exception:
            pass
    return None

LOCAL_URI = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
ATLAS_URI = resolve_atlas_uri()
DB_NAME = os.getenv("DB_NAME", "StockDB")
COLLECTION_NEWS = os.getenv("COLLECTION_NAME", "news_sentiment")
COLLECTION_LOGS = os.getenv("LOGS_COLLECTION_NAME", "pipeline_logs")

def sync():
    if not ATLAS_URI:
        print("[Error] No MONGO_URI found in environment or .streamlit/secrets.toml")
        sys.exit(1)

    print("=" * 80)
    print(" Lifetime Data Sync: Local Docker MongoDB -> MongoDB Atlas Cloud")
    print(f"* Local Source  : {LOCAL_URI}")
    print(f"* Atlas Target  : {ATLAS_URI.split('@')[-1] if '@' in ATLAS_URI else ATLAS_URI}")
    print(f"* Database      : {DB_NAME}")
    print("=" * 80)

    try:
        local_client = MongoClient(LOCAL_URI, serverSelectionTimeoutMS=3000)
        local_client.admin.command("ping")
        local_db = local_client[DB_NAME]
        local_news_count = local_db[COLLECTION_NEWS].count_documents({})
        local_logs_count = local_db[COLLECTION_LOGS].count_documents({})
        print(f"[+] Connected to local MongoDB ({local_news_count:,} news, {local_logs_count:,} logs).")
    except Exception as e:
        print(f"[-] Failed to connect to local MongoDB: {e}")
        sys.exit(1)

    try:
        atlas_client = MongoClient(ATLAS_URI, serverSelectionTimeoutMS=8000)
        atlas_client.admin.command("ping")
        atlas_db = atlas_client[DB_NAME]
        print("[+] Connected to MongoDB Atlas Cloud.")
    except Exception as e:
        print(f"[-] Failed to connect to MongoDB Atlas: {e}")
        sys.exit(1)

    print(f"[*] Syncing {COLLECTION_NEWS}...")
    batch = []
    for doc in local_db[COLLECTION_NEWS].find():
        if doc.get("_id"):
            batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        if len(batch) >= 500:
            atlas_db[COLLECTION_NEWS].bulk_write(batch, ordered=False)
            batch = []
    if batch:
        atlas_db[COLLECTION_NEWS].bulk_write(batch, ordered=False)

    print(f"[*] Syncing {COLLECTION_LOGS}...")
    batch = []
    for doc in local_db[COLLECTION_LOGS].find():
        if doc.get("_id"):
            batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        if len(batch) >= 500:
            atlas_db[COLLECTION_LOGS].bulk_write(batch, ordered=False)
            batch = []
    if batch:
        atlas_db[COLLECTION_LOGS].bulk_write(batch, ordered=False)

    final_news = atlas_db[COLLECTION_NEWS].count_documents({})
    final_logs = atlas_db[COLLECTION_LOGS].count_documents({})
    print("=" * 80)
    print(f"✅ Sync Complete: {final_news:,} news, {final_logs:,} logs now in Atlas.")
    print("=" * 80)

if __name__ == "__main__":
    sync()
