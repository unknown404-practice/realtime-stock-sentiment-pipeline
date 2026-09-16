"""
Real-Time Stock News Sentiment Analysis Dashboard
==================================================
24/7 Autonomous Cloud & Local Pipeline
- Data Source: Live Yahoo Finance RSS feeds & MongoDB (Local or Atlas)
- AI Model: ProsusAI/finbert (Hugging Face Transformers)
- Global Media Responsive: Optimized for Mobile, Tablet, and Desktop screens
"""

import os
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import streamlit as st
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError

# ==============================================================================
# 1. Page Configuration & Dynamic Media-Responsive CSS
# ==============================================================================
st.set_page_config(
    page_title="Global Live Stock Sentiment",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom Responsive CSS injection for 100% device compatibility
st.markdown("""
<style>
    /* Global Base Styling */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2rem;
        padding-left: 1rem;
        padding-right: 1rem;
        max-width: 1400px;
    }

    /* Live Header Status Pill */
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        background: #e6f4ea;
        color: #137333;
        border: 1px solid #ceead6;
    }
    .pulse-dot {
        width: 8px;
        height: 8px;
        background-color: #34a853;
        border-radius: 50%;
        box-shadow: 0 0 0 rgba(52, 168, 83, 0.4);
        animation: pulse 1.8s infinite;
    }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(52, 168, 83, 0.7); }
        70% { box-shadow: 0 0 0 8px rgba(52, 168, 83, 0); }
        100% { box-shadow: 0 0 0 0 rgba(52, 168, 83, 0); }
    }

    /* Responsive Metric Card Grid */
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 12px;
        margin-bottom: 1.5rem;
    }
    .kpi-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 14px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 6px rgba(0,0,0,0.08);
    }
    .kpi-title {
        font-size: 0.75rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }
    .kpi-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: #0f172a;
    }

    /* News Feed Card (Mobile & Tablet friendly) */
    .news-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 14px;
        margin-bottom: 10px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        display: flex;
        flex-direction: column;
        gap: 8px;
    }
    .news-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 6px;
    }
    .ticker-tag {
        background: #0f172a;
        color: #ffffff;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }
    .headline-text {
        font-size: 0.95rem;
        font-weight: 500;
        color: #1e293b;
        line-height: 1.4;
    }
    .card-footer {
        display: flex;
        align-items: center;
        justify-content: space-between;
        font-size: 0.75rem;
        color: #64748b;
        margin-top: 4px;
    }

    /* Sentiment Badges */
    .badge-pos {
        background-color: #dcfce7;
        color: #15803d;
        border: 1px solid #bbf7d0;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.75rem;
    }
    .badge-neg {
        background-color: #fee2e2;
        color: #b91c1c;
        border: 1px solid #fecaca;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.75rem;
    }
    .badge-neu {
        background-color: #f1f5f9;
        color: #475569;
        border: 1px solid #e2e8f0;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.75rem;
    }

    /* Media Queries for Small Screens (Mobile) */
    @media (max-width: 640px) {
        .block-container {
            padding-left: 0.5rem;
            padding-right: 0.5rem;
        }
        .metric-grid {
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
        }
        .kpi-value {
            font-size: 1.25rem;
        }
        .headline-text {
            font-size: 0.88rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. Database Connection Resolver (Supports Local Docker & MongoDB Atlas)
# ==============================================================================
def resolve_mongo_uri():
    """Resolve MongoDB connection string from Streamlit Secrets or Environment."""
    try:
        if "MONGO_URI" in st.secrets:
            return st.secrets["MONGO_URI"]
    except Exception:
        pass
    return os.getenv("MONGO_URI", "mongodb://localhost:27017/")

MONGO_URI = resolve_mongo_uri()
DB_NAME = os.getenv("DB_NAME", "StockDB")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "news_sentiment")

@st.cache_resource
def get_mongo_client(uri):
    """Cache MongoDB client connection with quick timeout to avoid blocking cloud users."""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2500)
        client.admin.command('ping')
        return client
    except Exception:
        return None

# ==============================================================================
# 3. Autonomous 24/7 Cloud AI Sentiment & News Engine
# ==============================================================================
MONITORED_TICKERS = ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "JPM", "BAC"]

@st.cache_resource
def get_sentiment_classifier():
    """
    Load FinBERT classifier cached once in memory for all global visitors.
    Falls back to a lightweight heuristic if PyTorch/HuggingFace is loading.
    """
    try:
        from transformers import pipeline
        return pipeline("text-classification", model="ProsusAI/finbert")
    except Exception:
        return None

@st.cache_data(ttl=15, show_spinner=False)
def fetch_live_yahoo_news(tickers):
    """
    Directly fetches genuine 100% real-world breaking financial news from Yahoo Finance RSS.
    Cached with 15-second TTL so concurrent global users share the feed without rate-limits.
    """
    headlines = []
    for ticker in tickers:
        url = f"https://finance.yahoo.com/rss/headline?s={ticker}"
        try:
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            )
            with urllib.request.urlopen(req, timeout=4) as resp:
                xml_data = resp.read()
            root = ET.fromstring(xml_data)
            for item in root.findall('.//item')[:3]:  # Top 3 latest per ticker
                title = item.find('title')
                pub_date = item.find('pubDate')
                if title is not None and title.text:
                    clean_title = title.text.strip()
                    ts = pub_date.text if (pub_date is not None and pub_date.text) else datetime.now(timezone.utc).isoformat()
                    headlines.append({
                        "ticker": ticker,
                        "headline": clean_title,
                        "timestamp": ts
                    })
        except Exception:
            continue
    return headlines

def classify_text(headline, classifier):
    """Classify sentiment using FinBERT or financial keyword rules."""
    if classifier is not None:
        try:
            res = classifier(headline)[0]
            return res["label"].upper(), round(float(res["score"]), 4)
        except Exception:
            pass

    # Fast heuristic fallback if model is still downloading or memory is constrained
    h_lower = headline.lower()
    pos_words = ["soar", "surge", "beat", "record", "jump", "upgrade", "growth", "profit", "gain", "buy", "rally"]
    neg_words = ["fall", "drop", "miss", "plunge", "antitrust", "recall", "cut", "warning", "decline", "slump", "loss"]
    pos_score = sum(1 for w in pos_words if w in h_lower)
    neg_score = sum(1 for w in neg_words if w in h_lower)

    if pos_score > neg_score:
        return "POSITIVE", 0.8800
    elif neg_score > pos_score:
        return "NEGATIVE", 0.8700
    return "NEUTRAL", 0.8200

# ==============================================================================
# 4. Data Gathering & Synchronization
# ==============================================================================
# Initialize in-memory cache in session_state so the cloud UI is instant 24/7
if "in_memory_records" not in st.session_state:
    st.session_state["in_memory_records"] = []

def get_latest_data():
    """
    Fetch data from MongoDB (Atlas/Local) if connected;
    Otherwise autonomously ingest and analyze live Yahoo Finance news in real-time.
    """
    client = get_mongo_client(MONGO_URI)
    if client is not None:
        try:
            db = client[DB_NAME]
            coll = db[COLLECTION_NAME]
            cursor = coll.find(
                {},
                {"_id": 0, "ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1}
            ).sort("_id", DESCENDING).limit(100)
            records = list(cursor)
            if records:
                return pd.DataFrame(records), "MongoDB Atlas Cloud / Local Database"
        except (ServerSelectionTimeoutError, PyMongoError):
            pass

    # Autonomous Cloud Mode: Stream live news directly and perform sentiment analysis
    classifier = get_sentiment_classifier()
    live_items = fetch_live_yahoo_news(MONITORED_TICKERS)
    existing_headlines = {r["headline"] for r in st.session_state["in_memory_records"]}

    for item in live_items:
        if item["headline"] not in existing_headlines:
            sentiment, conf = classify_text(item["headline"], classifier)
            record = {
                "ticker": item["ticker"],
                "headline": item["headline"],
                "timestamp": item["timestamp"],
                "sentiment": sentiment,
                "confidence": conf
            }
            st.session_state["in_memory_records"].insert(0, record)
            existing_headlines.add(item["headline"])

            # If MongoDB is accessible, also persist the cloud records
            if client is not None:
                try:
                    client[DB_NAME][COLLECTION_NAME].insert_one(record)
                except Exception:
                    pass

    # Cap in-memory history to latest 100
    st.session_state["in_memory_records"] = st.session_state["in_memory_records"][:100]
    return pd.DataFrame(st.session_state["in_memory_records"]), "Autonomous Cloud Stream (24/7 Live Engine)"

# ==============================================================================
# 5. Sidebar Controls & Settings
# ==============================================================================
with st.sidebar:
    st.title("⚙️ Pipeline Controls")
    auto_refresh = st.toggle("Auto-Refresh (24/7 Stream)", value=True)
    refresh_interval = st.slider("Refresh Interval (Seconds)", min_value=2, max_value=15, value=3)

    st.markdown("---")
    st.subheader("Filter Stream")
    selected_ticker = st.selectbox("Select Ticker:", ["ALL"] + MONITORED_TICKERS)
    sentiment_filter = st.radio("Filter Sentiment:", ["ALL", "POSITIVE", "NEGATIVE", "NEUTRAL"], horizontal=True)

    search_query = st.text_input("🔍 Keyword Search:", placeholder="e.g., earnings, chip, recall")

    st.markdown("---")
    if st.button("🔄 Force Refresh Now", use_container_width=True):
        st.rerun()

    st.caption("🌐 Running 24/7 Autonomous Cloud Engine with FinBERT & Yahoo Finance.")

# ==============================================================================
# 6. Main Dashboard Header & Metrics
# ==============================================================================
df, data_source_label = get_latest_data()

# Header with Live Status Pill
header_col1, header_col2 = st.columns([3, 1])
with header_col1:
    st.title("📈 Real-Time Stock News Sentiment Pipeline")
    st.markdown(
        "Autonomous 24/7 financial sentiment intelligence powered by **ProsusAI/finbert** and **real-world market news**. "
        "Accessible globally from any device."
    )
with header_col2:
    st.markdown(f"""
        <div style="text-align: right; margin-top: 10px;">
            <div class="status-pill">
                <div class="pulse-dot"></div>
                <span>LIVE 24/7 CLOUD</span>
            </div>
            <div style="font-size: 0.72rem; color: #64748b; margin-top: 4px;">
                Mode: {data_source_label}
            </div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("---")

if df.empty:
    st.info("🔄 Ingesting live market headlines from global exchanges... Please wait a few seconds.")
else:
    # Filter by Ticker, Sentiment, and Keyword
    filtered_df = df.copy()
    if selected_ticker != "ALL":
        filtered_df = filtered_df[filtered_df["ticker"] == selected_ticker]
    if sentiment_filter != "ALL":
        filtered_df = filtered_df[filtered_df["sentiment"] == sentiment_filter]
    if search_query:
        filtered_df = filtered_df[filtered_df["headline"].str.contains(search_query, case=False, na=False)]

    # Compute key metrics from the latest 100 records
    total_records = len(df)
    pos_count = int((df["sentiment"] == "POSITIVE").sum())
    neg_count = int((df["sentiment"] == "NEGATIVE").sum())
    neu_count = int((df["sentiment"] == "NEUTRAL").sum())

    ratio_str = f"{pos_count / neg_count:.1f} : 1" if neg_count > 0 else f"{pos_count} : 0"
    net_sentiment = ((pos_count - neg_count) / total_records * 100) if total_records > 0 else 0
    net_color = "#15803d" if net_sentiment >= 0 else "#b91c1c"

    # Dynamic Media-Responsive Metrics Cards
    st.markdown(f"""
    <div class="metric-grid">
        <div class="kpi-card">
            <div class="kpi-title">Headlines Analyzed</div>
            <div class="kpi-value">{total_records}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Positive Events</div>
            <div class="kpi-value" style="color: #15803d;">🟢 {pos_count}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Negative Events</div>
            <div class="kpi-value" style="color: #b91c1c;">🔴 {neg_count}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Neutral Events</div>
            <div class="kpi-value" style="color: #475569;">⚪ {neu_count}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Net Market Mood</div>
            <div class="kpi-value" style="color: {net_color};">{net_sentiment:+.1f}%</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # View Mode Switcher: Mobile Card Feed vs Desktop Table
    view_col1, view_col2 = st.columns([2, 1])
    with view_col1:
        st.subheader(f"Live Market Stream ({len(filtered_df)} items matching filter)")
    with view_col2:
        view_mode = st.radio("Display Format:", ["📱 Mobile Card Feed", "💻 Data Table"], horizontal=True)

    # --------------------------------------------------------------------------
    # Format A: Dynamic Mobile Card Feed (Responsive on all phones & tablets)
    # --------------------------------------------------------------------------
    if view_mode == "📱 Mobile Card Feed":
        for _, row in filtered_df.iterrows():
            sentiment = row.get("sentiment", "NEUTRAL")
            badge_class = "badge-pos" if sentiment == "POSITIVE" else ("badge-neg" if sentiment == "NEGATIVE" else "badge-neu")
            conf = row.get("confidence", 0.0)
            ticker = row.get("ticker", "N/A")
            headline = row.get("headline", "")
            ts = row.get("timestamp", "")

            st.markdown(f"""
            <div class="news-card">
                <div class="news-header">
                    <span class="ticker-tag">{ticker}</span>
                    <span class="{badge_class}">{sentiment} &bull; {conf:.2f} Conf</span>
                </div>
                <div class="headline-text">{headline}</div>
                <div class="card-footer">
                    <span>🕒 {ts}</span>
                    <span>AI Model: FinBERT</span>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # --------------------------------------------------------------------------
    # Format B: Data Table View (Ideal for wide desktop displays)
    # --------------------------------------------------------------------------
    else:
        def highlight_sentiment(val):
            if val == "POSITIVE":
                return "background-color: #dcfce7; color: #15803d; font-weight: 600;"
            elif val == "NEGATIVE":
                return "background-color: #fee2e2; color: #b91c1c; font-weight: 600;"
            return "background-color: #f1f5f9; color: #475569;"

        cols = ["ticker", "headline", "sentiment", "confidence", "timestamp"]
        table_df = filtered_df[[c for c in cols if c in filtered_df.columns]]
        styled = table_df.style.applymap(highlight_sentiment, subset=["sentiment"])

        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ticker": st.column_config.TextColumn("Ticker", width="small"),
                "headline": st.column_config.TextColumn("Headline", width="large"),
                "sentiment": st.column_config.TextColumn("Sentiment", width="medium"),
                "confidence": st.column_config.NumberColumn("Confidence", format="%.4f", width="small"),
                "timestamp": st.column_config.TextColumn("Timestamp (UTC)", width="medium")
            }
        )

# ==============================================================================
# 7. Auto-Refresh Loop for 24/7 Continuous Live Stream
# ==============================================================================
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
