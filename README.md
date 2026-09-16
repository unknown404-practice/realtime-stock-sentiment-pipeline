# 📈 Real-Time Stock-News Sentiment Pipeline

A production-style, end-to-end data engineering and machine learning pipeline that streams simulated real-time financial market news through **Apache Kafka (KRaft mode)**, classifies sentiment using a **local FinBERT model** (`ProsusAI/finbert`), persists enriched records in **MongoDB**, and visualizes market sentiment live in a **Streamlit** dashboard.

> **100% Local & Free**: Runs completely on your local workstation without Ollama, without paid APIs, and without external cloud dependencies.

---

## 🏛 Architecture Overview

```
[Producer.ipynb]  (JupyterLab)
       │  Generates realistic stock news JSON (AAPL, TSLA, NVDA, etc.)
       ▼  Publishes to Kafka topic 'stock-news' every 2s
[Apache Kafka]   (Docker, KRaft mode, localhost:9092)
       │  Streams raw events
       ▼  Consumes streaming news
[Consumer.ipynb]  (JupyterLab)
       │  Local FinBERT model (ProsusAI/finbert via PyTorch/Transformers)
       │  Enriches with sentiment (POSITIVE/NEGATIVE/NEUTRAL) & confidence
       ▼  Inserts enriched JSON records
[MongoDB]         (Docker, localhost:27017, DB: StockDB, Collection: news_sentiment)
       │  Persists latest sentiment stream
       ▼  Queries latest 100 records
[Streamlit App]   (app.py, localhost:8501)
          Displays live metrics, Pos/Neg ratios, and color-coded sentiment feed
```

---

## 📁 Project Structure

```
stock-sentiment-pipeline/
│
├── .venv/                 # Dedicated Python virtual environment (Python 3.10+)
├── docker-compose.yml     # Docker services: Kafka in KRaft mode & MongoDB
├── requirements.txt       # Python dependencies (kafka-python, transformers, torch, etc.)
├── Producer.ipynb         # Jupyter notebook simulating & streaming stock news to Kafka
├── Consumer.ipynb         # Jupyter notebook classifying headlines with FinBERT & saving to MongoDB
├── app.py                 # Streamlit real-time interactive sentiment dashboard
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

Open a new terminal window in the project folder with `.venv` activated:

```bash
streamlit run app.py
```

- Streamlit will open automatically in your browser at `http://localhost:8501`.
- Features:
  - **Live Auto-Refresh**: Configurable refresh interval (default: 3 seconds).
  - **Key Sentiment Metrics**: Positive Count, Negative Count, Neutral Count, and Pos/Neg Ratio.
  - **Filter by Ticker**: Filter incoming headlines by ticker (`AAPL`, `TSLA`, `NVDA`, etc.).
  - **Color-Coded Sentiment Table**: Soft green for POSITIVE, soft red for NEGATIVE, soft gray for NEUTRAL.

---

## 🛠 Troubleshooting

| Issue | Cause | Solution |
|---|---|---|
| `docker compose` fails: `failed to connect to the docker API` | Docker Desktop is closed or starting up | Open the Docker Desktop app, verify the engine icon shows green (running), and re-run `docker compose up -d`. |
| Port conflict on `9092` or `27017` | Another Kafka or MongoDB instance is using the port | Run `netstat -ano \| findstr :9092` or change port mappings in `docker-compose.yml`. |
| `NoBrokersAvailable` error in Notebooks | Kafka container is still booting | Wait 10-15 seconds for KRaft quorum initialization, then re-run the notebook cell. |
| FinBERT download slow or interrupted | Hugging Face network latency on first run | Run `python -c "from transformers import pipeline; pipeline('text-classification', model='ProsusAI/finbert')"` in terminal to pre-cache the model. |
| Notebook cannot import `kafka` or `transformers` | Wrong Jupyter kernel selected | Change kernel in top-right corner to `Python (Stock Sentiment)`. |
| MongoDB shows 0 records in Streamlit | Producer or Consumer not yet running | Start both `Producer.ipynb` and `Consumer.ipynb` to feed records into MongoDB. |

---

## 🔒 Security & Local Execution Guarantee

- **No API Keys**: No OpenAI, Anthropic, or external API keys required.
- **No Ollama**: FinBERT executes natively inside PyTorch via Hugging Face Transformers.
- **Data Privacy**: All simulated market data and predictions stay within your local machine.
