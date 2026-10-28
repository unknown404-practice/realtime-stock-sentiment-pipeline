import argparse
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings
from pymongo import MongoClient, DESCENDING, ASCENDING
from src.db.mongo import ensure_bounded_collections, ensure_indexes


def compact_and_prune(keep_news: int = 5000, keep_logs: int = 5000, force: bool = False):
    settings = get_settings()
    uri = settings.mongo_uri or os.getenv("MONGO_URI")
    if not uri:
        print("No MONGO_URI provided in environment or settings. Exiting.")
        return
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=30000)
    db = client[settings.db_name]
    print(f"Connected to Atlas {settings.db_name}")
    print("Initial dbStats:", db.command("dbStats"))

    # Targets: (collection_name, keep_count, time_key, max_size, max_docs, index_specs)
    news_indexes = [
        [("ticker", ASCENDING), ("headline", ASCENDING)],
        [("timestamp", DESCENDING)],
        [("ticker", ASCENDING)],
    ]
    logs_indexes = [
        [("iso_time", DESCENDING)],
        [("level", ASCENDING)],
        [("component", ASCENDING)],
    ]

    targets = [
        (settings.collection_name, keep_news, "timestamp", 15728640, 10000, news_indexes),
        (settings.logs_collection_name, keep_logs, "iso_time", 15728640, 15000, logs_indexes),
    ]

    # Ensure DLQ collection is also capped
    ensure_bounded_collections(db)

    for col_name, keep_count, time_key, max_size, max_docs, idx_specs in targets:
        print(f"\nEvaluating collection '{col_name}'...")
        col = db[col_name]
        try:
            stats = db.command("collStats", col_name)
        except Exception:
            stats = {}

        is_capped = stats.get("capped", False)
        current_max = stats.get("max", 0)
        doc_count = col.estimated_document_count()

        needs_compaction = (
            force
            or not is_capped
            or current_max == 0
            or doc_count > keep_count
        )

        if not needs_compaction:
            print(f"Collection '{col_name}' is already properly capped (capped={is_capped}, max={current_max}, count={doc_count}). Skipping.")
            continue

        print(f"Compacting '{col_name}' (capped={is_capped}, max={current_max}, count={doc_count})...")
        newest_records = list(col.find().sort(time_key, DESCENDING).limit(keep_count))
        print(f"Preserved {len(newest_records)} newest records from '{col_name}'.")

        # Atomic swap using temporary capped collection:
        # Prevents race conditions with live background workers and preserves insertion order
        temp_name = f"_temp_compact_{col_name}"
        try:
            db.drop_collection(temp_name)
        except Exception:
            pass

        db.create_collection(temp_name, capped=True, size=max_size, max=max_docs)

        if newest_records:
            # Reverse to chronological order (oldest to newest) to preserve FIFO circular eviction
            newest_records.reverse()
            db[temp_name].insert_many(newest_records)

        # Recreate indexes on temp collection
        for idx in idx_specs:
            try:
                db[temp_name].create_index(idx, background=True)
            except Exception as e:
                print(f"Notice creating index {idx} on '{temp_name}': {e}")

        # Atomic swap
        print(f"Performing atomic swap '{temp_name}' -> '{col_name}'...")
        db[temp_name].rename(col_name, dropTarget=True)
        print(f"Atomic swap complete for '{col_name}'.")

    ensure_indexes(db)
    print("\nCompaction complete! Verifying counts and storage:")
    for name in [settings.collection_name, settings.logs_collection_name, "dead_letter_queue"]:
        st = db.command("collStats", name)
        print(f"{name}: capped={st.get('capped')}, max={st.get('max')}, maxSize={st.get('maxSize')}, count={st.get('count')}, size={st.get('size')}, storageSize={st.get('storageSize')}")
    print("Final dbStats:", db.command("dbStats"))


def parse_args():
    parser = argparse.ArgumentParser(description="Prune and compact MongoDB Atlas collections into strict capped buffers.")
    parser.add_argument("--keep-news", type=int, default=5000, help="Number of news records to keep (default: 5000)")
    parser.add_argument("--keep-logs", type=int, default=5000, help="Number of logs to keep (default: 5000)")
    parser.add_argument("--force", action="store_true", help="Force atomic recreation of capped collections even if within limits")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    compact_and_prune(keep_news=args.keep_news, keep_logs=args.keep_logs, force=args.force)

