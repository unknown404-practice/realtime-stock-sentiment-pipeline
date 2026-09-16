# Design Specification: Lifetime Pipeline Logs Persistence & Real-Time Synchronization

**Date:** 2026-09-16  
**Status:** Approved  
**Topic:** Lifetime Log Persistence & Real-Time Streaming for Stock Sentiment Pipeline  

---

## 1. Overview & Objectives

In the current pipeline architecture, logs in [`app.py`](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/app.py) are stored strictly in an in-memory session buffer (`st.session_state["pipeline_logs"]`) capped at 200 items. When the browser tab is refreshed or reopened, the log history is lost. Meanwhile, [`Consumer.ipynb`](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/Consumer.ipynb) writes logs into `StockDB.pipeline_logs`, but the dashboard never queries or displays them.

The objective is to:
1. **Enforce Lifetime Persistence:** Store all execution logs indefinitely in MongoDB (`StockDB.pipeline_logs`) with no TTL/expiration, ensuring history survives session restarts, dashboard reloads, and container lifecycles.
2. **Backfill Existing Data:** Populate `StockDB.pipeline_logs` with historical events derived from the existing 6,369+ records in `StockDB.news_sentiment`.
3. **Real-Time Live Synchronization:** Ensure both **Tab 1 (Live Feed Terminal)** and **Tab 2 (Pipeline Execution Logs)** continuously stream and display new logs live from MongoDB.
4. **Export Lifetime Data:** Provide full-history log exports (TXT and JSON) directly from MongoDB.

---

## 2. Architecture & Data Schema

### 2.1 MongoDB Collection: `StockDB.pipeline_logs`
Each log document conforms to the following schema:

```json
{
  "_id": ObjectId("..."),
  "time": "17:16:30.123",
  "iso_time": "2026-09-16T11:46:30.123456+00:00",
  "level": "FINBERT",
  "component": "KAFKA_CONSUMER",
  "message": "[POSITIVE] AAPL: \"AAPL reports quarterly earnings...\" (conf: 0.9654)",
  "ticker": "AAPL",
  "sentiment": "POSITIVE"
}
```

* **Index:** `{ "iso_time": -1 }` and default `{ "_id": -1 }` for `< 2ms` indexed descending queries.
* **Retention Policy:** Lifetime (no TTL index, no capped collection limit).

### 2.2 Component Roles
- **[`Consumer.ipynb`](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/Consumer.ipynb):** Inserts FinBERT classification logs with ISO timestamps directly into `pipeline_logs`.
- **[`Producer.ipynb`](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/Producer.ipynb):** Inserts Kafka ingestion logs (`KAFKA_PRODUCER` / `INGEST`) into `pipeline_logs`.
- **[`app.py`](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/app.py):**
  - Reads the latest logs directly from `StockDB.pipeline_logs` during each refresh cycle.
  - Persists system/boot/warning logs to `StockDB.pipeline_logs`.
  - Serves live updates in Tab 1 and Tab 2.

---

## 3. Detailed Component Design

### 3.1 Historical Backfill
A dedicated initialization/backfill helper in `app.py` (or utility script):
- Checks if `StockDB.pipeline_logs` has documents.
- If empty (or significantly behind `news_sentiment`), it reads existing records from `StockDB.news_sentiment` and inserts corresponding structured log entries into `pipeline_logs`.
- Guarantees immediate visibility of past events upon first launch.

### 3.2 Live Log Streaming in `app.py`
- Update `fetch_data_and_logs(limit=100)`:
  - Fetches latest records from `StockDB.news_sentiment`.
  - Queries `StockDB.pipeline_logs.find().sort("_id", -1).limit(limit)`.
  - Combines with session fallback if database is offline.
- Update `add_local_log(level, component, message)`:
  - Writes to MongoDB `pipeline_logs` (with non-blocking exception handling).
  - Also maintains session cache for instant UI feedback.

### 3.3 Dashboard Tabs Architecture
- **Tab 1 (Live Market Stream):**
  - Continues using high-frequency fragment for low latency.
  - Terminal console displays latest 15-30 events from MongoDB `pipeline_logs`.
- **Tab 2 (Pipeline Execution Logs):**
  - Wrapped in a Streamlit fragment `@st.fragment(run_every="2s")` (or tied to sidebar auto-refresh interval).
  - Interactive filter controls (Level, Component, Search).
  - Log Export: Allows downloading the filtered logs or full lifetime logs.

---

## 4. Verification & Testing Plan

1. **Backfill Verification:** Run backfill script and verify document count in `StockDB.pipeline_logs` matches `StockDB.news_sentiment`.
2. **Lifetime Persistence Test:** Add log entry, restart Python process / session, confirm log entry is still present and loaded from database.
3. **Live Refresh Test:** Simulate a new log insertion into `StockDB.pipeline_logs` and verify it displays automatically in both Tab 1 and Tab 2.
4. **AppTest Suite:** Run automated tests via `test_dashboard.py` to ensure zero UI exceptions and smooth theme/tab operations.
