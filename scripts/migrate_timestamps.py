"""
Migration script: Normalize legacy RFC 2822 timestamps in news_sentiment to ISO 8601.
Resolves lexicographical string sorting issues so live stream articles sort to the top.
"""

from datetime import timezone
from dateutil import parser as dt_parser
from pymongo import MongoClient, UpdateOne, DESCENDING
import tomli

with open(".streamlit/secrets.toml", "rb") as f:
    sec = tomli.load(f)

client = MongoClient(sec["MONGO_URI"], serverSelectionTimeoutMS=5000)
db = client["StockDB"]
coll = db["news_sentiment"]

print("Connected to Atlas StockDB. Scanning for legacy timestamps...")

# Find all documents with legacy format containing a comma (e.g., 'Wed, 16 Sep 2026...')
cursor = coll.find({"timestamp": {"$regex": ","}}, {"_id": 1, "timestamp": 1})

ops = []
count = 0
total_migrated = 0

for doc in cursor:
    old_ts = doc.get("timestamp")
    try:
        dt = dt_parser.parse(old_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        new_ts = dt.isoformat()
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"timestamp": new_ts}}))
        count += 1
        if len(ops) >= 1000:
            coll.bulk_write(ops, ordered=False)
            total_migrated += len(ops)
            print(f"Migrated {total_migrated} documents...")
            ops = []
    except Exception as exc:
        print(f"Failed to parse timestamp {old_ts}: {exc}")

if ops:
    coll.bulk_write(ops, ordered=False)
    total_migrated += len(ops)

print(f"Migration completed! Total migrated: {total_migrated}")

print("\n--- Verifying top 5 newest articles after migration ---")
for doc in coll.find({}, {"_id": 0, "timestamp": 1, "ticker": 1, "headline": 1}).sort("timestamp", DESCENDING).limit(5):
    print(doc.get("timestamp"), doc.get("ticker"), doc.get("headline")[:60])
