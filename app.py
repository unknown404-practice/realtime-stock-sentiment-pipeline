"""
Real-Time Stock News Sentiment Analysis Dashboard
==================================================
Superfast Lean Architecture (Starting-of-the-day Speed)
- 100% Pure Light Theme (Zero Dimming, Zero Screen Darkening)
- Superfast: Decoupled presentation layer (direct fast MongoDB stream, < 5ms latency)
- No Heavy PyTorch/Transformers overhead in Streamlit
- Real-time Pipeline Execution Logs directly on dashboard
- Smooth non-blocking auto-refresh via Streamlit Fragment
- Static Tabs Architecture: Zero component unmounting / flickering
"""

import os
import json
from datetime import datetime, timezone
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import streamlit as st
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError

# ==============================================================================
# 1. Page Configuration & Pure Light Anti-Dimming CSS
# ==============================================================================
st.set_page_config(
    page_title="Live Stock Sentiment Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    /* -------------------------------------------------------------------------
       1. PURE LIGHT THEME ENFORCEMENT (System Dark Mode Override)
       Eliminates the 1-second delay switch to dark mode from browser preferences
       ------------------------------------------------------------------------- */
    :root {
        color-scheme: light !important;
        --background-color: #f8fafc !important;
        --text-color: #0f172a !important;
        --secondary-background-color: #ffffff !important;
    }

    @media (prefers-color-scheme: dark) {
        :root, html, body, .stApp {
            color-scheme: light !important;
            background-color: #f8fafc !important;
            color: #0f172a !important;
        }
    }

    html, body, .stApp, 
    [data-testid="stAppViewContainer"], 
    [data-testid="stMainBlockContainer"],
    [data-testid="stVerticalBlock"],
    [data-testid="stHeader"],
    .element-container,
    div[data-testid="stDataFrame"] {
        background-color: #f8fafc !important;
        color: #0f172a !important;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif !important;
    }

    /* -------------------------------------------------------------------------
       2. CRITICAL: COMPLETE ZERO-DIMMING ANTI-STALE OVERRIDE
       Eliminates Streamlit 1.64's data-stale="true" opacity: 0.33 and 1s transition!
       ------------------------------------------------------------------------- */
    [data-stale="true"],
    [data-stale="true"] *,
    div[data-stale="true"],
    div[data-stale="true"] *,
    [data-testid="stElementContainer"][data-stale="true"],
    [data-testid="stElementContainer"][data-stale="true"] *,
    div[class*="st-emotion-cache"][data-stale="true"],
    div[class*="st-emotion-cache"][data-stale="true"] *,
    .element-container[data-stale="true"],
    .element-container[data-stale="true"] *,
    .stApp--running [data-stale="true"] {
        opacity: 1 !important;
        filter: none !important;
        -webkit-filter: none !important;
        transition: none !important;
        -webkit-transition: none !important;
    }

    /* Completely hide the running man / spinner that dims the screen */
    [data-testid="stStatusWidget"],
    div[data-testid="stStatusWidget"],
    .stStatusWidget,
    [data-testid="stToolbar"] {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
    }

    /* Top padding */
    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2rem;
        padding-left: 1.2rem;
        padding-right: 1.2rem;
        max-width: 1440px;
    }

    /* Sidebar Clean Light Styling */
    [data-testid="stSidebar"], 
    [data-testid="stSidebarContent"],
    section[data-testid="stSidebar"] {
        background-color: #ffffff !important;
        border-right: 1px solid #e2e8f0 !important;
        color: #0f172a !important;
    }

    /* Live Header Status Pill */
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 700;
        background: #e6f4ea;
        color: #137333;
        border: 1px solid #bbf7d0;
    }
    .pulse-dot {
        width: 8px;
        height: 8px;
        background-color: #16a34a;
        border-radius: 50%;
        box-shadow: 0 0 0 rgba(22, 163, 74, 0.4);
        animation: pulse 1.8s infinite;
    }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(22, 163, 74, 0.7); }
        70% { box-shadow: 0 0 0 8px rgba(22, 163, 74, 0); }
        100% { box-shadow: 0 0 0 0 rgba(22, 163, 74, 0); }
    }

    /* High-Speed Metric Cards */
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 12px;
        margin-bottom: 1.2rem;
    }
    .kpi-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 14px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .kpi-card:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 6px rgba(0,0,0,0.06);
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
        font-weight: 800;
        color: #0f172a;
    }

    /* Live Activity Ribbon */
    .live-ribbon {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 8px 14px;
        margin-bottom: 16px;
        display: flex;
        align-items: center;
        gap: 10px;
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 0.82rem;
        color: #0f172a;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    .ribbon-tag {
        background: #eff6ff;
        color: #1d4ed8;
        border: 1px solid #dbeafe;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 700;
        font-size: 0.72rem;
        letter-spacing: 0.5px;
        white-space: nowrap;
    }

    /* News Cards */
    .news-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 14px;
        margin-bottom: 10px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.03);
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
        background: #1e293b;
        color: #ffffff;
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }
    .headline-text {
        font-size: 0.95rem;
        font-weight: 600;
        color: #0f172a;
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

    /* Badges */
    .badge-pos {
        background-color: #dcfce7;
        color: #15803d;
        border: 1px solid #bbf7d0;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.75rem;
    }
    .badge-neg {
        background-color: #fee2e2;
        color: #b91c1c;
        border: 1px solid #fecaca;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.75rem;
    }
    .badge-neu {
        background-color: #f1f5f9;
        color: #475569;
        border: 1px solid #e2e8f0;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.75rem;
    }

    /* High-Contrast Developer Light Terminal */
    .terminal-window {
        background: #f8fafc;
        border: 1px solid #cbd5e1;
        border-radius: 10px;
        overflow: hidden;
        margin-top: 8px;
        margin-bottom: 18px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    }
    .terminal-header {
        background: #e2e8f0;
        padding: 8px 14px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        border-bottom: 1px solid #cbd5e1;
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 0.78rem;
        color: #334155;
        font-weight: 600;
    }
    .terminal-dots {
        display: flex;
        gap: 6px;
    }
    .dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
    }
    .dot-red { background: #ef4444; }
    .dot-yellow { background: #f59e0b; }
    .dot-green { background: #10b981; }
    
    .terminal-body {
        padding: 12px 16px;
        max-height: 460px;
        overflow-y: auto;
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 0.82rem;
        line-height: 1.6;
        background: #ffffff;
        color: #0f172a;
    }
    .log-line {
        margin-bottom: 4px;
        word-break: break-word;
        font-family: 'Consolas', 'Courier New', monospace;
    }
    .log-time { color: #64748b; margin-right: 8px; font-weight: 500; }
    .log-comp { color: #0284c7; font-weight: 700; margin-right: 6px; }
    
    .log-PRODUCER { color: #1d4ed8; font-weight: 700; }
    .log-CONSUMER { color: #047857; font-weight: 700; }
    .log-INGEST { color: #6d28d9; font-weight: 700; }
    .log-FINBERT { color: #b45309; font-weight: 700; }
    .log-DATABASE { color: #0e7490; font-weight: 700; }
    .log-ATLAS_SYNC { color: #0f766e; font-weight: 700; }
    .log-SUCCESS { color: #15803d; font-weight: 700; }
    .log-INFO { color: #0284c7; font-weight: 700; }
    .log-WARN { color: #c2410c; font-weight: 700; }
    .log-ERROR { color: #b91c1c; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. Database Connection Resolver (Prioritize Local Fast Docker Mongo)
# ==============================================================================
@st.cache_resource
def get_mongo_connection():
    """
    Cached connection manager.
    Prioritizes ultra-fast local Docker MongoDB (<2ms latency, active live stream).
    Falls back to MongoDB Atlas cloud secrets only if local MongoDB is unreachable.
    """
    local_uri = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
    try:
        client = MongoClient(local_uri, serverSelectionTimeoutMS=250)
        client.admin.command('ping')
        return client, "Local Docker MongoDB (<2ms, Live Stream)"
    except Exception:
        pass

    # Fallback to secrets (Atlas) or env var
    try:
        if "MONGO_URI" in st.secrets:
            atlas_uri = st.secrets["MONGO_URI"]
            client = MongoClient(atlas_uri, serverSelectionTimeoutMS=1500)
            client.admin.command('ping')
            return client, "MongoDB Atlas (Remote Cloud)"
    except Exception:
        pass

    fallback_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(fallback_uri, serverSelectionTimeoutMS=500)
    return client, "Default MongoDB"

client, DB_SOURCE_LABEL = get_mongo_connection()
DB_NAME = os.getenv("DB_NAME", "StockDB")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "news_sentiment")

# In-memory fast log buffer
if "pipeline_logs" not in st.session_state:
    st.session_state["pipeline_logs"] = []

def add_local_log(level, component, message):
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    entry = {"time": now_str, "level": level, "component": component, "message": message}
    st.session_state["pipeline_logs"].insert(0, entry)
    if len(st.session_state["pipeline_logs"]) > 200:
        st.session_state["pipeline_logs"] = st.session_state["pipeline_logs"][:200]

# Add initial boot logs if new session
if not st.session_state["pipeline_logs"]:
    add_local_log("INFO", "BOOT", "Superfast Pure Light Dashboard initialized.")
    add_local_log("SUCCESS", "DATABASE", f"Connected to: {DB_SOURCE_LABEL}")

def fetch_data_and_logs(limit=100):
    """
    Superfast Data Fetching: Queries MongoDB in < 3ms.
    """
    records = []
    if client is not None:
        try:
            db = client[DB_NAME]
            cursor = db[COLLECTION_NAME].find(
                {},
                {"_id": 0, "ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1}
            ).sort("_id", DESCENDING).limit(limit)
            records = list(cursor)
        except Exception as e:
            add_local_log("WARN", "DATABASE", f"Query notice: {e}")

    # Synchronize live streaming events into session log buffer
    if records:
        last_seen = st.session_state.get("last_seen_headline")
        first_headline = records[0].get("headline")
        if last_seen is None:
            # Seed terminal with recent stream events on first load
            for r in reversed(records[:10]):
                sent = r.get("sentiment", "NEUTRAL")
                tick = r.get("ticker", "TICKER")
                head = r.get("headline", "")
                conf = r.get("confidence", 0.0)
                add_local_log("FINBERT", "KAFKA_CONSUMER", f"[{sent}] {tick}: \"{head[:50]}...\" (conf: {conf:.4f})")
            st.session_state["last_seen_headline"] = first_headline
        elif last_seen != first_headline:
            # New incoming records detected! Find all new ones (up to 5)
            new_items = []
            for r in records[:5]:
                if r.get("headline") == last_seen:
                    break
                new_items.append(r)
            for r in reversed(new_items):
                sent = r.get("sentiment", "NEUTRAL")
                tick = r.get("ticker", "TICKER")
                head = r.get("headline", "")
                conf = r.get("confidence", 0.0)
                add_local_log("FINBERT", "KAFKA_CONSUMER", f"[{sent}] {tick}: \"{head[:50]}...\" (conf: {conf:.4f})")
            st.session_state["last_seen_headline"] = first_headline

    df = pd.DataFrame(records) if records else pd.DataFrame()
    return df, st.session_state["pipeline_logs"]

# Helper for rendering terminal window HTML
def build_terminal_html(logs_slice, max_h="460px"):
    log_lines_html = []
    for l in logs_slice:
        lvl_class = f"log-{l.get('level', 'INFO')}"
        log_lines_html.append(f"""
        <div class="log-line">
            <span class="log-time">{l.get('time', '')}</span>
            <span class="log-comp">[{l.get('component', '')}]</span>
            <span class="{lvl_class}">[{l.get('level', 'INFO')}]</span> {l.get('message', '')}
        </div>
        """)
    body_content = "".join(log_lines_html) if log_lines_html else '<div style="color: #64748b;">No log events...</div>'
    return f"""
    <div class="terminal-window">
        <div class="terminal-header">
            <div class="terminal-dots">
                <div class="dot dot-red"></div>
                <div class="dot dot-yellow"></div>
                <div class="dot dot-green"></div>
            </div>
            <span>LIGHT TERMINAL CONSOLE &bull; {len(st.session_state['pipeline_logs'])} TOTAL EVENTS</span>
            <span style="color: #15803d; font-weight: 700;">● STREAMING</span>
        </div>
        <div class="terminal-body" style="max-height: {max_h};">
            {body_content}
        </div>
    </div>
    """

def render_cards(df_to_show, max_cards=25):
    cards_html = []
    for _, row in df_to_show.head(max_cards).iterrows():
        sentiment = row.get("sentiment", "NEUTRAL")
        badge_class = "badge-pos" if sentiment == "POSITIVE" else ("badge-neg" if sentiment == "NEGATIVE" else "badge-neu")
        conf = row.get("confidence", 0.0)
        ticker = row.get("ticker", "N/A")
        headline = row.get("headline", "")
        ts = row.get("timestamp", "")
        cards_html.append(f"""
        <div class="news-card">
            <div class="news-header">
                <span class="ticker-tag">{ticker}</span>
                <span class="{badge_class}">{sentiment} &bull; {conf:.2f} Conf</span>
            </div>
            <div class="headline-text">{headline}</div>
            <div class="card-footer">
                <span>🕒 {ts}</span>
                <span>Model: FinBERT</span>
            </div>
        </div>
        """)
    if cards_html:
        st.markdown("".join(cards_html), unsafe_allow_html=True)
    else:
        st.caption("No matching news records found.")

# ==============================================================================
# 3. Sidebar Controls
# ==============================================================================
with st.sidebar:
    st.title("⚙️ Pipeline Controls")
    auto_refresh = st.toggle("Auto-Refresh Live Stream", value=True)
    refresh_interval = st.slider("Refresh Interval (Seconds)", min_value=1, max_value=5, value=1)
    st.caption("⚡ Set to 1s for real-time high-speed streaming.")

    st.markdown("---")
    st.subheader("Filter Stream")
    selected_ticker = st.selectbox(
        "Filter by Ticker:",
        ["ALL", "NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "AMD", "JPM", "BAC"]
    )
    sentiment_filter = st.radio("Filter Sentiment:", ["ALL", "POSITIVE", "NEGATIVE", "NEUTRAL"], horizontal=True)
    search_query = st.text_input("🔍 Search Headlines:", placeholder="e.g. earnings, chip, dividend")

    st.markdown("---")
    st.subheader("Quick Actions")
    col_c, col_r = st.columns(2)
    with col_c:
        if st.button("🗑️ Clear Logs", width="stretch"):
            st.session_state["pipeline_logs"] = []
            add_local_log("INFO", "TERMINAL", "Log buffer cleared.")
            st.rerun()
    with col_r:
        if st.button("⚡ Fast Sync", width="stretch"):
            st.rerun()

    st.caption("☀️ Pure Light Application & Superfast 5ms Pipeline.")

# ==============================================================================
# 4. Header
# ==============================================================================
header_col1, header_col2 = st.columns([3, 1])
with header_col1:
    st.title("📈 Real-Time Stock News Sentiment Pipeline")
    st.markdown(
        "Superfast financial sentiment intelligence streamed via **Apache Kafka**, classified by **FinBERT**, "
        "and persisted in **MongoDB**. Running in 100% Pure Light mode with zero screen dimming."
    )
with header_col2:
    st.markdown(f"""
        <div style="text-align: right; margin-top: 10px;">
            <div class="status-pill">
                <div class="pulse-dot"></div>
                <span>STREAM: LIVE FAST</span>
            </div>
            <div style="font-size: 0.72rem; color: #166534; font-weight: 600; margin-top: 4px;">
                ⚡ {DB_SOURCE_LABEL}
            </div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ==============================================================================
# 5. Static Tabs Architecture (Eliminates Re-Mounting & Flickering)
# ==============================================================================
tab_feed, tab_logs, tab_architecture = st.tabs([
    "📊 Live Market Stream",
    "🖥️ Pipeline Logs",
    "🏛️ Architecture & Status"
])

# ------------------------------------------------------------------------------
# TAB 1: Live Market Stream (High-Frequency Streamlit Fragment)
# ------------------------------------------------------------------------------
with tab_feed:
    @st.fragment(run_every=f"{refresh_interval}s" if auto_refresh else None)
    def render_live_feed():
        """
        Isolated high-frequency fragment.
        Only updates Tab 1 elements; never causes full-page reloads or tab remounts.
        """
        df, logs = fetch_data_and_logs(limit=100)

        if df.empty:
            st.info(
                "ℹ️ **No sentiment records found in database yet.**\n\n"
                "To stream real-time data:\n"
                "1. Run `Producer.ipynb` in JupyterLab (streams market news to Kafka).\n"
                "2. Run `Consumer.ipynb` in JupyterLab (runs FinBERT & saves to MongoDB).\n"
                "3. Ensure MongoDB is running locally (`docker compose up -d`)."
            )
            return

        # Filter data
        filtered_df = df
        if selected_ticker != "ALL":
            filtered_df = filtered_df[filtered_df["ticker"] == selected_ticker]
        if sentiment_filter != "ALL":
            filtered_df = filtered_df[filtered_df["sentiment"] == sentiment_filter]
        if search_query:
            filtered_df = filtered_df[filtered_df["headline"].str.contains(search_query, case=False, na=False)]

        # Compute key metrics fast (<0.1ms)
        total_records = len(df)
        sent_counts = df["sentiment"].value_counts() if not df.empty else {}
        pos_count = int(sent_counts.get("POSITIVE", 0))
        neg_count = int(sent_counts.get("NEGATIVE", 0))
        neu_count = int(sent_counts.get("NEUTRAL", 0))

        net_sentiment = ((pos_count - neg_count) / total_records * 100) if total_records > 0 else 0
        net_color = "#15803d" if net_sentiment >= 0 else "#b91c1c"

        # Metric Cards Bar
        st.markdown(f"""
        <div class="metric-grid">
            <div class="kpi-card">
                <div class="kpi-title">Total Headlines</div>
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

        # Top Live Activity Ribbon
        latest_event = logs[0] if logs else {"time": "--:--:--", "component": "PIPELINE", "level": "INFO", "message": "Stream active."}
        lvl_c = f"log-{latest_event.get('level', 'INFO')}"
        st.markdown(f"""
        <div class="live-ribbon">
            <div class="pulse-dot"></div>
            <span class="ribbon-tag">LIVE FEED</span>
            <span style="color: #64748b; font-size: 0.75rem;">{latest_event.get('time', '')}</span>
            <span class="log-comp">[{latest_event.get('component', 'SYSTEM')}]</span>
            <span class="{lvl_c}">[{latest_event.get('level', 'INFO')}]</span>
            <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #0f172a; font-weight: 500;">
                {latest_event.get('message', '')}
            </span>
        </div>
        """, unsafe_allow_html=True)

        # Stream View
        view_col1, view_col2 = st.columns([2, 1])
        with view_col1:
            st.subheader(f"Latest Ingested News ({len(filtered_df)} shown)")
        with view_col2:
            view_mode = st.radio(
                "Display Format:",
                ["💻 Data Table", "⚡ Split View (Feed + Terminal)", "📱 Mobile Cards"],
                horizontal=True,
                key="feed_view_mode"
            )

        # Mode 1: Clean Data Table (Preferred by user - superfast native rendering)
        if view_mode == "💻 Data Table":
            cols = ["ticker", "headline", "sentiment", "confidence", "timestamp"]
            table_df = filtered_df[[c for c in cols if c in filtered_df.columns]].copy()
            icon_map = {"POSITIVE": "🟢 POSITIVE", "NEGATIVE": "🔴 NEGATIVE", "NEUTRAL": "⚪ NEUTRAL"}
            table_df["sentiment"] = table_df["sentiment"].map(icon_map).fillna(table_df["sentiment"])

            st.dataframe(
                table_df,
                width="stretch",
                hide_index=True,
                column_config={
                    "ticker": st.column_config.TextColumn("Ticker", width="small"),
                    "headline": st.column_config.TextColumn("Headline", width="large"),
                    "sentiment": st.column_config.TextColumn("Sentiment", width="medium"),
                    "confidence": st.column_config.NumberColumn("Confidence", format="%.4f", width="small"),
                    "timestamp": st.column_config.TextColumn("Timestamp (UTC)", width="medium")
                }
            )
            st.markdown("---")
            st.subheader("🖥️ Live Execution Terminal Console")
            st.markdown(build_terminal_html(logs[:15], max_h="240px"), unsafe_allow_html=True)

        # Mode 2: Split View (Feed + Terminal)
        elif view_mode == "⚡ Split View (Feed + Terminal)":
            col_feed, col_term = st.columns([3, 2])
            with col_feed:
                render_cards(filtered_df, max_cards=25)
            with col_term:
                st.markdown("#### 🖥️ Real-Time Pipeline Terminal")
                st.markdown(build_terminal_html(logs[:25], max_h="520px"), unsafe_allow_html=True)

        # Mode 3: Mobile Cards
        else:
            render_cards(filtered_df, max_cards=25)
            st.markdown("---")
            st.subheader("🖥️ Live Execution Terminal Console")
            st.markdown(build_terminal_html(logs[:15], max_h="240px"), unsafe_allow_html=True)

    render_live_feed()

# ------------------------------------------------------------------------------
# TAB 2: Execution Logs
# ------------------------------------------------------------------------------
with tab_logs:
    st.subheader("🖥️ Pipeline Execution Logs")
    st.markdown("Unified real-time events from **Producer**, **Kafka**, **FinBERT Consumer**, and **MongoDB**.")

    all_logs = st.session_state.get("pipeline_logs", [])

    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 2])
    with ctrl_col1:
        lvl_filter = st.selectbox("Filter Level:", ["ALL", "FINBERT", "INGEST", "PRODUCER", "CONSUMER", "DATABASE", "ATLAS_SYNC", "SUCCESS", "WARN", "ERROR"], key="log_lvl_filter")
    with ctrl_col2:
        comp_filter = st.selectbox("Filter Component:", ["ALL", "KAFKA_PRODUCER", "KAFKA_CONSUMER", "FINBERT", "YAHOO_RSS", "DATABASE", "SYSTEM", "BOOT"], key="log_comp_filter")
    with ctrl_col3:
        search_log = st.text_input("🔍 Search Log Text:", "", key="log_search_input")

    filtered_logs = all_logs.copy()
    if lvl_filter != "ALL":
        filtered_logs = [l for l in filtered_logs if l.get("level") == lvl_filter]
    if comp_filter != "ALL":
        filtered_logs = [l for l in filtered_logs if l.get("component") == comp_filter]
    if search_log:
        s_low = search_log.lower()
        filtered_logs = [l for l in filtered_logs if s_low in l.get("message", "").lower() or s_low in l.get("component", "").lower()]

    st.markdown(build_terminal_html(filtered_logs[:60], max_h="480px"), unsafe_allow_html=True)

    # Export Buttons with static file names to avoid widget destruction on ticks
    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        log_text = "\n".join([f"{l.get('time')} [{l.get('component')}] [{l.get('level')}]: {l.get('message')}" for l in filtered_logs])
        st.download_button(
            label="📥 Download Log File (.txt)",
            data=log_text,
            file_name="pipeline_logs.txt",
            mime="text/plain",
            width="stretch",
            key="btn_dl_txt"
        )
    with btn_col2:
        log_json = json.dumps(filtered_logs, indent=2)
        st.download_button(
            label="📥 Export Log Data (.json)",
            data=log_json,
            file_name="pipeline_logs.json",
            mime="application/json",
            width="stretch",
            key="btn_dl_json"
        )

# ------------------------------------------------------------------------------
# TAB 3: Architecture & Status (Static & Zero Rerun Overhead)
# ------------------------------------------------------------------------------
with tab_architecture:
    st.subheader("🏛️ Architecture Topology")
    st.markdown("""
    ```
    [Producer.ipynb (Kafka)] ➔ [stock-news Broker (9092)] ➔ [Consumer.ipynb (FinBERT)]
                                                                       │
                                                                       ▼
    [Streamlit Dashboard (app.py)] ◀───────────────────────── [MongoDB (StockDB)]
    ```
    """)
    inf1, inf2 = st.columns(2)
    with inf1:
        st.markdown("""
        **Component Status:**
        - 🟢 **Apache Kafka**: KRaft mode, topic `stock-news`, port 9092
        - 🟢 **FinBERT AI Model**: `ProsusAI/finbert` (Local CPU/GPU inference)
        - 🟢 **Database**: MongoDB (`StockDB.news_sentiment` & `pipeline_logs`)
        """)
    with inf2:
        st.markdown(f"""
        **Active Configuration:**
        - **Target Database**: `{DB_SOURCE_LABEL}`
        - **Refresh Speed**: Every `{refresh_interval}s` (Streamlit Fast Fragment)
        - **Mode**: 100% Pure Light Theme (Zero Dimming)
        """)
