"""
Real-Time Stock News Sentiment Analysis Dashboard
==================================================

How to run:
    1. Activate your virtual environment:
       .\\.venv\\Scripts\\activate  (Windows)
       source .venv/bin/activate    (Linux/macOS)
    2. Start Streamlit:
       streamlit run app.py

Configuration:
    - MongoDB Connection String: mongodb://localhost:27017/
    - Target Database: StockDB
    - Target Collection: news_sentiment
"""

import os
import time
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import streamlit as st
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError

# ==============================================================================
# 1. Page Configuration & Custom CSS
# ==============================================================================
st.set_page_config(
    page_title="Stock Sentiment Live Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling for metrics and cards
st.markdown("""
<style>
    .reportview-container .main .block-container {
        padding-top: 1.5rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 15px;
        border: 1px solid #e9ecef;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. Database Connection Helper (Supports Local Docker & MongoDB Atlas Cloud)
# ==============================================================================
try:
    if "MONGO_URI" in st.secrets:
        MONGO_URI = st.secrets["MONGO_URI"]
    else:
        MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
except Exception:
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

DB_NAME = os.getenv("DB_NAME", "StockDB")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "news_sentiment")

@st.cache_resource
def get_mongo_client():
    """Create and cache the MongoDB client connection (supports local & cloud Atlas)."""
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)

def fetch_latest_sentiment_data(limit=100):
    """
    Fetch the latest records from StockDB.news_sentiment ordered by insertion (_id descending).
    Returns a pandas DataFrame or None if connection fails.
    """
    try:
        client = get_mongo_client()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Fetch latest records
        cursor = collection.find(
            {},
            {"_id": 0, "ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1}
        ).sort("_id", DESCENDING).limit(limit)

        records = list(cursor)
        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)
    except (ServerSelectionTimeoutError, PyMongoError) as err:
        st.error(f"⚠️ Could not connect to MongoDB at `{MONGO_URI}`. Error: {err}")
        return None

# ==============================================================================
# 3. Sentiment Row Styling Function
# ==============================================================================
def highlight_sentiment(row):
    """
    Color-codes table rows based on the sentiment value:
    - POSITIVE: Soft Green
    - NEGATIVE: Soft Red
    - NEUTRAL:  Soft Gray
    """
    sentiment = str(row.get("sentiment", "")).upper()
    if sentiment == "POSITIVE":
        return ["background-color: #d4edda; color: #155724; font-weight: 500;"] * len(row)
    elif sentiment == "NEGATIVE":
        return ["background-color: #f8d7da; color: #721c24; font-weight: 500;"] * len(row)
    elif sentiment == "NEUTRAL":
        return ["background-color: #e2e3e5; color: #383d41;"] * len(row)
    return [""] * len(row)

# ==============================================================================
# 4. Sidebar Controls
# ==============================================================================
st.sidebar.title("⚙️ Pipeline Controls")
st.sidebar.markdown(f"**Database:** `{DB_NAME}`\n\n**Collection:** `{COLLECTION_NAME}`")

auto_refresh = st.sidebar.toggle("Auto-Refresh Dashboard", value=True)
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", min_value=1, max_value=10, value=3)

selected_ticker = st.sidebar.selectbox(
    "Filter by Ticker:",
    options=["ALL", "AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "JPM", "BAC"]
)

if st.sidebar.button("🔄 Manual Refresh"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info(
    "💡 **Architecture**\n\n"
    "Producer.ipynb (Kafka) ➔ Consumer.ipynb (FinBERT) ➔ MongoDB ➔ Streamlit Dashboard"
)

# ==============================================================================
# 5. Header & Description
# ==============================================================================
st.title("📈 Real-Time Stock News Sentiment Pipeline")
st.markdown(
    "Live sentiment analysis powered locally by **ProsusAI/finbert**, streamed through **Apache Kafka** "
    "(KRaft mode), persisted in **MongoDB**, and visualized in real-time. "
    "*No external paid APIs, no Ollama — 100% local inference.*"
)

# ==============================================================================
# 6. Data Fetching & Visualization
# ==============================================================================
df = fetch_latest_sentiment_data(limit=100)

if df is None:
    st.warning("Please verify that your MongoDB container is running: `docker compose up -d`")
elif df.empty:
    st.info(
        "ℹ️ **No sentiment records found in database yet.**\n\n"
        "To begin streaming data into this dashboard:\n"
        "1. Start Docker containers: `docker compose up -d`\n"
        "2. Open and run `Producer.ipynb` in JupyterLab (publishes to Kafka).\n"
        "3. Open and run `Consumer.ipynb` in JupyterLab (enriches with FinBERT and stores in MongoDB)."
    )
else:
    # Filter by ticker if selected
    display_df = df.copy()
    if selected_ticker != "ALL":
        display_df = display_df[display_df["ticker"] == selected_ticker]

    # Calculate sentiment distribution metrics from latest 100 records
    total_records = len(df)
    positive_count = int((df["sentiment"] == "POSITIVE").sum())
    negative_count = int((df["sentiment"] == "NEGATIVE").sum())
    neutral_count = int((df["sentiment"] == "NEUTRAL").sum())

    # Ratio calculation (Positive vs Negative)
    if negative_count > 0:
        pos_neg_ratio_str = f"{positive_count / negative_count:.2f} : 1"
    else:
        pos_neg_ratio_str = f"{positive_count} : 0"

    # Net Sentiment Score: (Positive - Negative) / Total * 100
    net_sentiment_score = ((positive_count - negative_count) / total_records) * 100 if total_records > 0 else 0

    # Display Top-Level Metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric(label="Latest Records", value=total_records)
    with col2:
        st.metric(label="🟢 Positive Count", value=positive_count)
    with col3:
        st.metric(label="🔴 Negative Count", value=negative_count)
    with col4:
        st.metric(label="⚪ Neutral Count", value=neutral_count)
    with col5:
        st.metric(
            label="⚖️ Pos / Neg Ratio",
            value=pos_neg_ratio_str,
            delta=f"{net_sentiment_score:+.1f}% Net Sentiment"
        )

    st.markdown("---")

    # Display Table with color coding
    st.subheader(f"Latest Ingested News ({len(display_df)} shown)")

    # Ensure column order
    cols_order = ["ticker", "headline", "sentiment", "confidence", "timestamp"]
    available_cols = [c for c in cols_order if c in display_df.columns]
    styled_df = display_df[available_cols].style.apply(highlight_sentiment, axis=1)

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "headline": st.column_config.TextColumn("News Headline", width="large"),
            "sentiment": st.column_config.TextColumn("FinBERT Sentiment", width="medium"),
            "confidence": st.column_config.NumberColumn("Confidence", format="%.4f", width="small"),
            "timestamp": st.column_config.TextColumn("Timestamp (UTC)", width="medium"),
        }
    )

# ==============================================================================
# 7. Auto-Refresh Loop
# ==============================================================================
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
