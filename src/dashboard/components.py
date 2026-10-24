"""
Modular UI Components for Stock Sentiment Dashboard.
Decoupled presentation layer with zero data mutation side-effects.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st


def load_css(is_dark: bool = False) -> str:
    """Load external CSS stylesheet and apply dynamic theme root variable."""
    css_path = Path(__file__).resolve().parent / "styles.css"
    css_content = ""
    if css_path.exists():
        with open(css_path, "r", encoding="utf-8") as f:
            css_content = f.read()

    theme_attr = 'data-theme="dark"' if is_dark else 'data-theme="light"'
    return f"<style>\n:root {{\n    {theme_attr};\n}}\n{css_content}\n</style>"


def render_header(db_label: str, is_live: bool, age_sec: Optional[float]):
    """Render truthful top header with genuine pipeline heartbeat."""
    col1, col2 = st.columns([3, 1], vertical_alignment="center")

    with col1:
        st.title("📈 Real-Time Stock News Sentiment Pipeline")
        st.caption(
            "High-throughput financial news stream via **Apache Kafka**, classified by **FinBERT**, "
            "persisted into **MongoDB** with zero-dimming real-time visualization."
        )

    with col2:
        pill_cls = "status-pill status-live" if is_live else ("status-pill status-idle" if age_sec is not None and age_sec < 300 else "status-pill status-offline")
        dot_cls = "pulse-dot" if is_live else ("pulse-dot-idle" if age_sec is not None and age_sec < 300 else "pulse-dot-offline")

        if is_live:
            badge_text = "STREAM: LIVE FAST"
        elif age_sec is not None:
            badge_text = f"STREAM: IDLE ({int(age_sec)}s ago)"
        else:
            badge_text = "STREAM: OFFLINE / PAUSED"

        st.markdown(
            f"""
            <div style="text-align: right; display: flex; flex-direction: column; align-items: flex-end; gap: 0.35rem;">
                <div class="{pill_cls}">
                    <span class="{dot_cls}"></span>
                    <span>{badge_text}</span>
                </div>
                <div style="font-size: 0.76rem; color: var(--text-muted); font-family: monospace;">
                    DB: {db_label}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )


def render_metrics(df: pd.DataFrame):
    """Render top operational KPI metric cards."""
    total_records = len(df)
    if total_records == 0:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Streamed Records", "0")
        c2.metric("Positive Sentiment", "0%")
        c3.metric("Negative Sentiment", "0%")
        c4.metric("Avg FinBERT Confidence", "0.00")
        return

    pos_count = (df["sentiment"] == "POSITIVE").sum()
    neg_count = (df["sentiment"] == "NEGATIVE").sum()
    neu_count = (df["sentiment"] == "NEUTRAL").sum()

    pos_pct = round((pos_count / total_records) * 100, 1)
    neg_pct = round((neg_count / total_records) * 100, 1)
    neu_pct = round((neu_count / total_records) * 100, 1)

    avg_conf = df["confidence"].mean() if "confidence" in df.columns else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Streamed Headlines", f"{total_records:,}")
    c2.metric("Positive", f"{pos_pct}%", delta=f"{pos_count} items")
    c3.metric("Negative", f"{neg_pct}%", delta=f"-{neg_count} items", delta_color="inverse")
    c4.metric("Neutral", f"{neu_pct}%", delta=f"{neu_count} items", delta_color="off")
    c5.metric("Avg Confidence", f"{avg_conf:.3f}")


def render_news_feed(df: pd.DataFrame, ticker: str = "ALL", sentiment: str = "ALL", search: str = ""):
    """Render filtered live financial headlines table."""
    filtered_df = df.copy()

    if ticker != "ALL":
        filtered_df = filtered_df[filtered_df["ticker"] == ticker]

    if sentiment != "ALL":
        filtered_df = filtered_df[filtered_df["sentiment"] == sentiment]

    if search.strip():
        q = search.strip().lower()
        filtered_df = filtered_df[
            filtered_df["headline"].str.lower().str.contains(q, na=False) |
            filtered_df["ticker"].str.lower().str.contains(q, na=False)
        ]

    st.subheader(f"Live Market Feed ({len(filtered_df)} articles)")

    if filtered_df.empty:
        st.info("No matching headlines found.")
        return

    # Present formatted table
    display_df = filtered_df[["timestamp", "ticker", "sentiment", "confidence", "headline"]].copy()
    display_df["timestamp"] = display_df["timestamp"].astype(str).str.slice(0, 19).str.replace("T", " ")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "timestamp": st.column_config.TextColumn("Time (UTC)", width="small"),
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "sentiment": st.column_config.TextColumn("Sentiment", width="small"),
            "confidence": st.column_config.NumberColumn("Confidence", format="%.4f", width="small"),
            "headline": st.column_config.TextColumn("Financial Headline", width="large")
        }
    )


def render_logs_table(logs: List[Dict[str, Any]], total_count: int):
    """Render real-time pipeline audit logs with download buttons."""
    st.subheader(f"Pipeline Telemetry & Execution Logs ({total_count:,} total)")

    if not logs:
        st.info("No telemetry logs recorded yet.")
        return

    logs_df = pd.DataFrame(logs)
    cols = [c for c in ["time", "level", "component", "message"] if c in logs_df.columns]
    st.dataframe(logs_df[cols], use_container_width=True, hide_index=True)

    # Lifetime download button
    csv_bytes = logs_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download Lifetime Logs (CSV)",
        data=csv_bytes,
        file_name="pipeline_logs.csv",
        mime="text/csv",
        key="btn_download_lifetime_logs"
    )
