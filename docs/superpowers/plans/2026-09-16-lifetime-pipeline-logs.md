# Lifetime Pipeline Logs Persistence & Real-Time Synchronization Implementation Plan

Persist all execution, ingestion, classification, and system logs indefinitely in MongoDB (`StockDB.pipeline_logs`) with no expiration (lifetime retention), backfill the initial logs from existing records, and synchronize both Tab 1 and Tab 2 in real-time.

## User Review Required

> [!NOTE]
> MongoDB is running on `localhost:27017` with 6,369 records in `StockDB.news_sentiment`. We will backfill initial log entries from these existing records so your lifetime log history is immediately visible upon start.

- No breaking schema changes: `StockDB.pipeline_logs` follows the existing JSON structure defined in `Consumer.ipynb` (`time`, `iso_time`, `level`, `component`, `message`).
- Backward compatibility: If MongoDB is momentarily unreachable, the system automatically falls back to the in-memory session buffer so dashboard operation is never interrupted.

## Open Questions

None. The user has explicitly selected Approach A (MongoDB lifetime persistence + live auto-refresh + backfill).

## Proposed Changes

### MongoDB Infrastructure & Backfill Utility

#### [NEW] [backfill_logs.py](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/backfill_logs.py)
- A standalone utility script to inspect `StockDB.news_sentiment` and create initial lifetime entries in `StockDB.pipeline_logs`.
- Can be invoked independently or called once on boot if `pipeline_logs` is empty.
- Creates optimal index `{ iso_time: -1 }` and `{ _id: -1 }` on `StockDB.pipeline_logs`.

---

### Streamlit Dashboard Application

#### [MODIFY] [app.py](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/app.py)
- **Persistent Logger (`add_local_log`)**:
  - Insert log entries directly into `StockDB.pipeline_logs` alongside the local memory buffer.
  - Non-blocking execution so UI rendering latency remains sub-5ms.
- **Lifetime Data Fetcher (`fetch_data_and_logs`)**:
  - Query `StockDB.pipeline_logs` directly using fast indexed sort (`{"_id": -1}`).
  - Maintain synchronization between new incoming headlines and persistent log events.
- **Tab 1: Live Feed Terminal**:
  - Displays real-time lifetime stream events from MongoDB.
- **Tab 2: Pipeline Execution Logs**:
  - Wrap in `@st.fragment` with auto-refresh so Tab 2 updates dynamically in real-time.
  - Support full lifetime export: Export filtered view or complete lifetime MongoDB history as TXT and JSON.

---

### Verification & Automated Testing

#### [MODIFY] [test_dashboard.py](file:///c:/Users/RANADEEP/Documents/stock-sentiment-pipeline/test_dashboard.py)
- Add unit tests verifying:
  - Database connection to `pipeline_logs`
  - Insertion and retrieval of persistent logs
  - Live fragment execution without exceptions
  - Export functionality

---

## Verification Plan

### Automated Tests
- Run unit test suite:
  ```powershell
  .\.venv\Scripts\python.exe -m unittest test_dashboard.py
  ```
- Run backfill script:
  ```powershell
  .\.venv\Scripts\python.exe backfill_logs.py
  ```
- Verify document count in `StockDB.pipeline_logs`:
  ```powershell
  .\.venv\Scripts\python.exe -c "from pymongo import MongoClient; c=MongoClient('mongodb://localhost:27017/'); print('pipeline_logs count:', c['StockDB']['pipeline_logs'].count_documents({}))"
  ```

### Manual Verification
- Launch Streamlit dashboard on port 8502:
  ```powershell
  .\.venv\Scripts\streamlit.exe run app.py --server.port 8502
  ```
- Confirm both Tab 1 (Terminal console) and Tab 2 (Execution Logs) display lifetime events from MongoDB and update dynamically.
