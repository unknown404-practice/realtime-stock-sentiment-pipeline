#!/usr/bin/env python3
"""
Sync Local MongoDB Lifetime Data to MongoDB Atlas
=================================================
Migrates news_sentiment records and pipeline_logs from local Docker MongoDB
to MongoDB Atlas cloud cluster using standard configuration and idempotent bulk writes.
"""

import os
import sys
from pymongo import MongoClient, ReplaceOne

# Ensure robust UTF-8 printing on Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from src.config import get_settings

settings = get_settings()

LOCAL_URI = settings.local_mongo_uri
ATLAS_URI = settings.mongo_uri
DB_NAME = settings.db_name
COLLECTION_NEWS = settings.collection_name
COLLECTION_LOGS = settings.logs_collection_name


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
