# 📈 Real-Time Stock News Sentiment Pipeline

> **End-to-end AI data engineering pipeline** · Apache Kafka · FinBERT · MongoDB Atlas · Streamlit · 24/7 Global Cloud

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![MongoDB Atlas](https://img.shields.io/badge/MongoDB-Atlas-green?logo=mongodb)](https://www.mongodb.com/atlas)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-231F20?logo=apachekafka)](https://kafka.apache.org/)

---

## 🧠 What This Is

A **production-grade, end-to-end data engineering and NLP pipeline** that:

- 🔴 **Streams** real-time financial market headlines via **Apache Kafka (KRaft mode)**
- 🤖 **Classifies** sentiment using a **local FinBERT model** (`ProsusAI/finbert`) with zero paid APIs
- 🗄️ **Persists** enriched records + distributed execution logs in **MongoDB (Local Docker or Atlas Cloud)**
- 📊 **Visualizes** live market sentiment in an interactive **Streamlit dashboard** with a real-time terminal log console
- ☁️ **Runs 24/7 autonomously** on Streamlit Community Cloud + MongoDB Atlas — even when your laptop is off

> **100% Free & Open-Source** — no paid APIs, no Ollama, no cloud billing. Runs locally with Docker + PyTorch and deploys globally free via Streamlit Community Cloud.

---

## 🏛️ Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     STREAMING LAYER                          │
│                                                              │
│  [producer.py / Producer.ipynb]                              │
│   ↓ Streams 10-ticker financial headlines @ 0.8s intervals   │
│   ↓                                                          │
│  [Apache Kafka - KRaft Mode]  localhost:9092                 │
│   Topic: stock-news                                          │
│   ↓                                                          │
│  [consumer.py / Consumer.ipynb]                              │
│   ↓ Local FinBERT (ProsusAI/finbert, PyTorch CPU/GPU)       │
│   ↓ Enriches: POSITIVE / NEGATIVE / NEUTRAL + confidence    │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER                            │
│                                                              │
│  [MongoDB] (Local Docker localhost:27017 OR Atlas Cloud)     │
│   ├── StockDB.news_sentiment   ← enriched FinBERT records   │
│   └── StockDB.pipeline_logs   ← lifetime execution audit    │
└──────────────────────────┬───────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│                   VISUALIZATION LAYER                        │
│                                                              │
│  [app.py — Streamlit Dashboard]                              │
│   ├── 🟢 Real-Time Activity Ribbon (pulsing live status)     │
│   ├── ⚡ Split View: News Cards + Live Terminal Console      │
│   ├── 📋 Lifetime Pipeline Log Center (filter/search/export) │
│   └── 🌐 24/7 Autonomous Cloud Engine (auto-RSS + FinBERT)  │
└──────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

| Feature | Description |
|---|---|
| ⚡ **Real-Time Streaming** | Kafka KRaft mode — no ZooKeeper, sub-second message delivery |
| 🤖 **Local FinBERT AI** | `ProsusAI/finbert` — finance-domain BERT, runs 100% offline after first download |
| 📊 **Live Dashboard** | Streamlit app with 3 view modes: Split, Mobile Cards, Data Table |
| 🟢 **Live Heartbeat** | Dynamic `STREAM: LIVE FAST` / `STREAM: IDLE` badge with real-time age |
| 🖥️ **Terminal Console** | Built-in execution log terminal with level filtering + keyword search |
| 📤 **Log Export** | Download pipeline logs as `.txt` or `.json` |
| ☁️ **24/7 Cloud Mode** | Streamlit Cloud + MongoDB Atlas for global 24/7 uptime, free tier |
| 🔒 **Zero Credential Leaks** | `secrets.toml` gitignored; secrets injected via Streamlit Cloud UI |
| 🧩 **Demo Mode** | Fallback realistic seed data when DB is empty — no blank screens |

---

## 📁 Project Structure

```
realtime-stock-sentiment-pipeline/
│
├── .streamlit/
│   ├── config.toml              # UI theme, server & anti-dimming config
│   ├── secrets.toml             # 🔒 GITIGNORED — private MongoDB Atlas URI
│   └── secrets.toml.example     # Template for contributors
│
├── docker-compose.yml           # Kafka (KRaft) + MongoDB local containers
├── requirements.txt             # Lean: streamlit, pandas, pymongo, dnspython
├── requirements-pipeline.txt    # Full: + kafka-python, torch, transformers
│
├── app.py                       # Streamlit real-time dashboard (1500+ lines)
├── producer.py                  # CLI producer → streams headlines to Kafka
├── consumer.py                  # CLI FinBERT consumer → enriches & stores
├── run_pipeline.py              # Unified process supervisor (producer + consumer)
├── start_pipeline.bat           # 1-click Windows launcher
├── start_pipeline.ps1           # PowerShell launcher
│
├── Producer.ipynb               # Interactive Jupyter producer notebook
├── Consumer.ipynb               # Interactive Jupyter FinBERT notebook
├── sync_to_atlas.py             # Local → Atlas migration utility
├── backfill_logs.py             # One-time audit log backfill utility
├── test_dashboard.py            # Unit test suite (14 test cases, all passing)
└── README.md                    # This file
```

---

## 📋 Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10+ | 3.10 recommended for PyTorch + Kafka compatibility |
| Docker Desktop | Latest | Engine + Compose v2 enabled and running |
| JupyterLab | Optional | Only needed for notebook mode |
| Internet | First run only | Downloads Docker images + FinBERT weights (~438MB) |

---

## 🚀 Quick Start (Local)

### Step 1 — Clone & Create Virtual Environment

```bash
git clone https://github.com/unknown404-practice/realtime-stock-sentiment-pipeline.git
cd realtime-stock-sentiment-pipeline

# Windows
py -3.10 -m venv .venv
.\.venv\Scripts\activate

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

### Step 2 — Install Dependencies

```bash
pip install --upgrade pip

# For Streamlit dashboard only (cloud hosting):
pip install -r requirements.txt

# For full local pipeline (Kafka + FinBERT):
pip install -r requirements-pipeline.txt
```

### Step 3 — Start Docker Infrastructure

```bash
# Start Kafka (KRaft) + MongoDB
docker compose up -d

# Verify containers are running
docker compose ps
```

> **Kafka Broker**: `localhost:9092` (auto-creates topic `stock-news`)  
> **MongoDB**: `localhost:27017` (database: `StockDB`)

### Step 4 — Start Streaming Workers

**Option A — CLI Supervisor (Recommended)**
```bash
# Windows
.\.venv\Scripts\python.exe run_pipeline.py --interval 0.8

# macOS / Linux
python3 run_pipeline.py --interval 0.8

# Or use 1-click Windows launcher:
.\start_pipeline.bat
```
Press `Ctrl+C` to cleanly shut down both producer and consumer.

**Option B — Interactive Jupyter Notebooks**
1. Run all cells in `Producer.ipynb` → streams headlines to Kafka
2. Run all cells in `Consumer.ipynb` → runs FinBERT, stores to MongoDB

### Step 5 — Launch the Dashboard

```bash
streamlit run app.py
```

Open → **http://localhost:8501**

---

## ☁️ Deploy Free to Streamlit Community Cloud (24/7 Global)

Anyone in the world can access your dashboard anytime, even when your laptop is off:

1. **Push to GitHub** (already done if you're reading this!)

2. **Go to [share.streamlit.io](https://share.streamlit.io)** → Sign in with GitHub

3. **New App** → Select:
   - Repository: `unknown404-practice/realtime-stock-sentiment-pipeline`
   - Branch: `main`
   - Main file: `app.py`

4. **Advanced Settings → Secrets** — add your MongoDB Atlas URI:
   ```toml
   MONGO_URI = "mongodb+srv://<username>:<password>@cluster0.xxxxxx.mongodb.net/?appName=Cluster0"
   DB_NAME = "StockDB"
   COLLECTION_NAME = "news_sentiment"
   LOGS_COLLECTION_NAME = "pipeline_logs"
   ```

5. Click **Deploy** 🚀

Your public live URL (e.g. `https://realtime-stock-sentiment-pipeline.streamlit.app`) will be live 24/7 with real-time AI sentiment analysis.

### MongoDB Atlas Setup (Free Tier)
1. Create a free cluster at [mongodb.com/atlas](https://www.mongodb.com/atlas)
2. Create database user with read/write access
3. Whitelist `0.0.0.0/0` in Network Access (for Streamlit Cloud)
4. Copy the connection string into your Streamlit secrets

---

## 🔧 Configuration Reference

### `.streamlit/secrets.toml` (local — gitignored)

Copy `secrets.toml.example` and fill in your values:

```toml
MONGO_URI = "mongodb+srv://<username>:<password>@cluster0.xxxxxx.mongodb.net/?appName=Cluster0"
DB_NAME = "StockDB"
COLLECTION_NAME = "news_sentiment"
LOGS_COLLECTION_NAME = "pipeline_logs"
```

### Pipeline Interval

Control the streaming speed via `--interval` flag (seconds between headlines):

```bash
python run_pipeline.py --interval 0.8   # fast (default)
python run_pipeline.py --interval 2.0   # slower / conservative
```

---

## 🧪 Running Tests

```bash
python -m pytest test_dashboard.py -v
```

All **14 unit tests** cover:
- MongoDB connection handling (cloud, local, offline)
- FinBERT sentiment classification
- Heartbeat detection logic
- Demo/offline fallback mode
- Log parsing and export

---

## 🛠️ Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| `docker compose` fails: `failed to connect to Docker API` | Docker Desktop not running | Open Docker Desktop, wait for green engine icon, retry |
| Port conflict on `9092` or `27017` | Another Kafka/MongoDB instance | `netstat -ano \| findstr :9092` — change ports in `docker-compose.yml` |
| `NoBrokersAvailable` error | Kafka still initializing | Wait 10–15s for KRaft quorum, retry |
| FinBERT download slow/fails | Hugging Face network | Pre-cache: `python -c "from transformers import pipeline; pipeline('text-classification', model='ProsusAI/finbert')"` |
| Wrong Jupyter kernel | `.venv` not registered | Run: `python -m ipykernel install --user --name stock-sentiment --display-name "Python (Stock Sentiment)"` |
| Dashboard shows 0 records | Workers not running | Start `run_pipeline.py` — demo seed data shows automatically in offline mode |
| `STREAM: IDLE` badge | No new records in 45s | Pipeline workers stopped — restart `run_pipeline.py` |

---

## 🔒 Security

- **No Paid APIs**: FinBERT + Yahoo RSS = 100% free
- **No Ollama**: FinBERT runs natively via PyTorch / Hugging Face Transformers
- **Protected Secrets**: `.streamlit/secrets.toml` is in `.gitignore` — never committed
- **Secret Scanning**: All tracked files verified clean of plaintext credentials before publishing

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m 'feat: add your feature'`
4. Push to branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for details.

---

## 👤 Creator & Contact

**Ranadeep Saha**  
*Member, Google Developer Group*

[![GitHub](https://img.shields.io/badge/GitHub-unknown404--practice-181717?logo=github)](https://github.com/unknown404-practice)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Ranadeep%20Saha-0A66C2?logo=linkedin)](https://www.linkedin.com/in/ranadeep-saha-a03296404/)
[![Email](https://img.shields.io/badge/Email-ranadeep2021saha%40gmail.com-D14836?logo=gmail)](mailto:ranadeep2021saha@gmail.com)

| | |
|---|---|
| 🐙 **GitHub** | [github.com/unknown404-practice](https://github.com/unknown404-practice) |
| 💼 **LinkedIn** | [linkedin.com/in/ranadeep-saha-a03296404](https://www.linkedin.com/in/ranadeep-saha-a03296404/) |
| 📧 **Email** | [ranadeep2021saha@gmail.com](mailto:ranadeep2021saha@gmail.com) |

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/unknown404-practice">Ranadeep Saha</a> · Member, Google Developer Group
</p>
