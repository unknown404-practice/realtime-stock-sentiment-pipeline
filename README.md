# 📈 Real-Time Stock-News Sentiment Pipeline

A production-grade, end-to-end data engineering and AI pipeline that streams real-time financial market news through **Apache Kafka (KRaft mode)** or **autonomous 24/7 cloud ingestion**, classifies sentiment using a **local FinBERT model** (`ProsusAI/finbert`), persists enriched records and distributed execution logs in **MongoDB (Local or Atlas Cloud)**, and visualizes market sentiment live in an interactive, **media-responsive Streamlit dashboard** with a built-in **real-time terminal log console**.

> **100% Free & Open-Source**: Runs locally with Docker & PyTorch, and supports 24/7 autonomous uptime on Streamlit Community Cloud with MongoDB Atlas without paid APIs or external services.

---

## 🏛 Architecture Overview

```
[Producer.ipynb]  (JupyterLab / Local)
       │  Streams real Yahoo Finance RSS news to Kafka topic 'stock-news'
       ▼  Logs to MongoDB Atlas (StockDB.pipeline_logs)
[Apache Kafka]   (Docker, KRaft mode, localhost:9092)
       │  Streams raw events
       ▼  Consumes streaming news
[Consumer.ipynb]  (JupyterLab / Local)
       │  Local FinBERT model (ProsusAI/finbert via PyTorch/Transformers)
       │  Enriches with sentiment (POSITIVE/NEGATIVE/NEUTRAL) & confidence
       ▼  Inserts enriched records & logs to MongoDB
[MongoDB (Atlas/Local)] (StockDB.news_sentiment & StockDB.pipeline_logs)
       ▲                               ▲
       │ Persists records & logs       │ Real-time log sync
       │                               │
[Autonomous 24/7 Cloud Engine] ────────┘
       │  Direct Yahoo RSS & FinBERT in-app execution (when laptop is off)
       ▼
[Streamlit Dashboard (app.py)] (Desktop / Tablet / Mobile)
       ├── Top Real-Time Activity Ticker (Pulsing live feed)
       ├── Split View: News Cards + Live Terminal Console
       ├── Full Pipeline Execution Log Center (Filtering, Search, TXT/JSON export)
       └── Real-time KPI Metric Cards & Sentiment Analytics
```

---

## 📁 Project Structure

```
stock-sentiment-pipeline/
│
├── .venv/                 # Dedicated Python virtual environment (Python 3.10+)
├── .streamlit/
│   └── secrets.toml       # MongoDB Atlas connection URI (configured & gitignored)
├── docker-compose.yml     # Docker services: Kafka in KRaft mode & MongoDB
├── requirements.txt       # Python dependencies (kafka-python, transformers, torch, etc.)
├── Producer.ipynb         # Jupyter notebook streaming Yahoo market news to Kafka & Atlas
├── Consumer.ipynb         # Jupyter notebook classifying headlines with FinBERT & saving to MongoDB
├── app.py                 # Streamlit real-time dashboard with Live Terminal & 24/7 Cloud engine
└── README.md              # Full documentation, architecture & run instructions
```

---

## 📋 Prerequisites

Before running the project, ensure you have:
1. **Python 3.10+**: (Python 3.10 is recommended for optimal PyTorch and Kafka driver compatibility).
2. **Docker Desktop**: With Docker Engine and Docker Compose v2 enabled and running.
3. **JupyterLab Desktop** (or standard JupyterLab in your browser).
4. **Internet connection**: Needed only on first run to download Docker images and FinBERT model weights (~438MB). Subsequent runs run entirely offline.

---

## 🚀 Step-by-Step Setup & Execution Guide

Follow these instructions in exact sequential order:

### 1. Create and Activate the Virtual Environment

Open PowerShell (Windows) or Terminal (macOS/Linux) in the project root:

```bash
# Windows (using Python 3.10 or py launcher):
py -3.10 -m venv .venv
.\.venv\Scripts\activate

# macOS / Linux:
python3 -m venv .venv
source .venv/bin/activate
```

---

### 2. Install Python Dependencies & Register Jupyter Kernel

With the virtual environment activated:

```bash
pip install --upgrade pip
pip install -r requirements.txt

# Register the environment as a selectable Jupyter kernel
python -m ipykernel install --user --name stock-sentiment-pipeline --display-name "Python (Stock Sentiment)"
```

---

### 3. Start Docker Infrastructure (Kafka + MongoDB)

Start the local KRaft Apache Kafka broker and MongoDB database:

```bash
# Start Kafka and MongoDB in background
docker compose up -d

# Verify both containers are running (stock-kafka and stock-mongodb)
docker compose ps

# Follow container logs (optional)
docker compose logs -f

# When you want to stop containers later:
# docker compose down
```

> **Kafka Broker**: `localhost:9092` (Auto-creates topic `stock-news` on demand)  
> **MongoDB**: `localhost:27017` (Database: `StockDB`, Collection: `news_sentiment`)

---

### 4. Run the Producer (`Producer.ipynb`)

1. Launch **JupyterLab Desktop** and open the project directory `stock-sentiment-pipeline`.
2. Open [`Producer.ipynb`](file:///C:/Users/RANADEEP/Documents/stock-sentiment-pipeline/Producer.ipynb).
3. Ensure the kernel in the top-right corner is set to **`Python (Stock Sentiment)`**.
4. Run all cells (`Shift + Enter`).
5. The producer will create the Kafka topic `stock-news` (if not already present) and begin emitting realistic stock headlines every 2 seconds:
   ```text
   [0001] Sent -> NVDA: "NVDA unveils next-generation enterprise AI accelerator chips..." | 2026-09-16T...
   [0002] Sent -> TSLA: "TSLA faces new antitrust investigation from regulators..." | 2026-09-16T...
   ```
6. **To Stop**: Click the **Square Stop Button (Interrupt kernel)** on the toolbar.

---

### 5. Run the Consumer (`Consumer.ipynb`)

1. In JupyterLab, open [`Consumer.ipynb`](file:///C:/Users/RANADEEP/Documents/stock-sentiment-pipeline/Consumer.ipynb).
2. Set the kernel to **`Python (Stock Sentiment)`**.
3. Run all cells (`Shift + Enter`).
4. On the first run, the local **ProsusAI/finbert** model weights will be downloaded to your local cache.
5. The consumer will connect to Kafka at `localhost:9092`, run FinBERT text classification on each headline, store the enriched document in MongoDB, and print live logs:
   ```text
   [POSITIVE] NVDA: NVDA unveils next-generation enterprise AI accelerator chips... (confidence: 0.9421)
   [NEGATIVE] TSLA: TSLA faces new antitrust investigation from regulators... (confidence: 0.8874)
   ```
6. **To Stop**: Click the **Square Stop Button (Interrupt kernel)** on the toolbar.

---

### 6. Launch the Streamlit Live Dashboard (`app.py`)

Open a terminal window in the project folder with `.venv` activated:

```bash
streamlit run app.py
```

- Streamlit will open automatically in your browser at `http://localhost:8501`.
- **Key Dashboard Features**:
  - 🟢 **Top Real-Time Activity Ribbon**: Displays latest live stream event (ingest, inference, database sync) with pulsing status.
  - ⚡ **Multi-View Modes**:
    - `⚡ Split View (Feed + Live Terminal)`: Side-by-side news feed and real-time live terminal console.
    - `📱 Mobile Card Feed`: Clean responsive cards with live console.
    - `💻 Data Table`: Color-coded analytical table with live console.
  - 🖥️ **Full Pipeline Execution Logs Tab**:
    - Live counters: Total Events, Ingest & Kafka Events, FinBERT Inferences, Database Syncs.
    - Filter by level (`ALL`, `FINBERT`, `INGEST`, `PRODUCER`, `CONSUMER`, `DATABASE`, `ATLAS_SYNC`, `WARN`, `ERROR`).
    - Filter by component & instant keyword search.
    - Export logs as `.txt` or `.json`.
  - 🌐 **24/7 Autonomous Cloud Engine**: Automatically fetches Yahoo Finance RSS news and executes FinBERT classifications even if local notebooks are stopped.

---

### 7. Deploy 24/7 Free to Streamlit Community Cloud

Anyone in the world can access your dashboard anytime, even when your laptop is turned off:

1. **Push your code to GitHub**:
   ```bash
   git branch -M main
   git remote add origin https://github.com/<YOUR_USERNAME>/stock-sentiment-pipeline.git
   git push -u origin main
   ```
2. **Deploy on Streamlit Community Cloud**:
   - Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
   - Click **"New app"** -> Select your repository `stock-sentiment-pipeline` -> Branch `main` -> Main file `app.py`.
   - Click **"Advanced settings..."** -> Under **Secrets**, add your MongoDB Atlas URI:
     ```toml
     MONGO_URI = "mongodb+srv://ranadeep2021saha_db_user:StockPass2026@cluster0.tq1iqxk.mongodb.net/?appName=Cluster0"
     DB_NAME = "StockDB"
     COLLECTION_NAME = "news_sentiment"
     ```
   - Click **"Deploy"**!
3. Your public live URL (e.g. `https://stock-sentiment-pipeline.streamlit.app`) is now 24/7 active with live real-time financial news, FinBERT AI inference, and execution logs visible to everyone.

---

## 🛠 Troubleshooting

| Issue | Cause | Solution |
|---|---|---|
| `docker compose` fails: `failed to connect to the docker API` | Docker Desktop is closed or starting up | Open the Docker Desktop app, verify the engine icon shows green (running), and re-run `docker compose up -d`. |
| Port conflict on `9092` or `27017` | Another Kafka or MongoDB instance is using the port | Run `netstat -ano \| findstr :9092` or change port mappings in `docker-compose.yml`. |
| `NoBrokersAvailable` error in Notebooks | Kafka container is still booting | Wait 10-15 seconds for KRaft quorum initialization, then re-run the notebook cell. |
| FinBERT download slow or interrupted | Hugging Face network latency on first run | Run `python -c "from transformers import pipeline; pipeline('text-classification', model='ProsusAI/finbert')"` in terminal to pre-cache the model. |
| Notebook cannot import `kafka` or `transformers` | Wrong Jupyter kernel selected | Change kernel in top-right corner to `Python (Stock Sentiment)`. |
| MongoDB shows 0 records in Streamlit | Producer or Consumer not yet running | The autonomous cloud engine will automatically start ingesting Yahoo news within 3 seconds. |

---

## 🔒 Security & Local Execution Guarantee

- **No Paid APIs**: FinBERT and Yahoo RSS feeds run 100% free with zero paid API subscriptions.
- **No Ollama**: FinBERT executes natively inside PyTorch via Hugging Face Transformers.
- **Protected Secrets**: `.streamlit/secrets.toml` is included in `.gitignore` to keep credentials secure.
