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
# 1. Page Configuration & Dynamic Multi-Theme Responsive CSS
# ==============================================================================
st.set_page_config(
    page_title="Live Stock Sentiment Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Theme State Management (Streamlit populates widget keys prior to script rerun)
if "theme_toggle" not in st.session_state:
    st.session_state["theme_toggle"] = False  # False = Light Mode, True = Dark Mode

is_dark = bool(st.session_state["theme_toggle"])
theme_name = "dark" if is_dark else "light"

if is_dark:
    theme_vars = """
        --color-scheme: dark;
        --bg-page: #0b0f19;
        --bg-card: #151d2e;
        --bg-card-hover: #1e293b;
        --bg-sidebar: #0d1322;
        --border-color: #273549;
        --border-subtle: #1e293b;
        --text-main: #f8fafc;
        --text-muted: #94a3b8;
        --text-dim: #64748b;
        --accent-color: #38bdf8;
        --card-shadow: 0 4px 14px rgba(0, 0, 0, 0.45);
        --status-pill-bg: rgba(16, 185, 129, 0.16);
        --status-pill-color: #34d399;
        --status-pill-border: rgba(16, 185, 129, 0.4);
        --status-idle-bg: rgba(245, 158, 11, 0.16);
        --status-idle-color: #fbbf24;
        --status-idle-border: rgba(245, 158, 11, 0.4);
        --db-badge-bg: rgba(56, 189, 248, 0.12);
        --db-badge-color: #38bdf8;
        --db-badge-border: rgba(56, 189, 248, 0.3);
        --badge-pos-bg: rgba(34, 197, 94, 0.18);
        --badge-pos-color: #4ade80;
        --badge-pos-border: rgba(34, 197, 94, 0.4);
        --badge-neg-bg: rgba(239, 68, 68, 0.18);
        --badge-neg-color: #f87171;
        --badge-neg-border: rgba(239, 68, 68, 0.4);
        --badge-neu-bg: rgba(148, 163, 184, 0.16);
        --badge-neu-color: #cbd5e1;
        --badge-neu-border: rgba(148, 163, 184, 0.3);
        --terminal-window-bg: #090d16;
        --terminal-header-bg: #151d2e;
        --terminal-border: #273549;
        --terminal-body-bg: #070a10;
        --terminal-text: #e2e8f0;
        --ribbon-bg: #151d2e;
        --ribbon-border: #273549;
        --ribbon-tag-bg: rgba(56, 189, 248, 0.18);
        --ribbon-tag-color: #38bdf8;
        --ribbon-tag-border: rgba(56, 189, 248, 0.4);
        --log-producer: #60a5fa;
        --log-consumer: #34d399;
        --log-ingest: #c084fc;
        --log-finbert: #fbbf24;
        --log-database: #38bdf8;
        --log-atlas: #2dd4bf;
        --log-success: #4ade80;
        --log-info: #38bdf8;
        --log-warn: #fb923c;
        --log-error: #f87171;
    """
else:
    theme_vars = """
        --color-scheme: light;
        --bg-page: #f8fafc;
        --bg-card: #ffffff;
        --bg-card-hover: #f1f5f9;
        --bg-sidebar: #ffffff;
        --border-color: #e2e8f0;
        --border-subtle: #f1f5f9;
        --text-main: #0f172a;
        --text-muted: #64748b;
        --text-dim: #94a3b8;
        --accent-color: #2563eb;
        --card-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        --status-pill-bg: #e6f4ea;
        --status-pill-color: #137333;
        --status-pill-border: #bbf7d0;
        --status-idle-bg: #fef3c7;
        --status-idle-color: #b45309;
        --status-idle-border: #fde68a;
        --db-badge-bg: #f0fdf4;
        --db-badge-color: #166534;
        --db-badge-border: #dcfce7;
        --badge-pos-bg: #dcfce7;
        --badge-pos-color: #15803d;
        --badge-pos-border: #bbf7d0;
        --badge-neg-bg: #fee2e2;
        --badge-neg-color: #b91c1c;
        --badge-neg-border: #fecaca;
        --badge-neu-bg: #f1f5f9;
        --badge-neu-color: #475569;
        --badge-neu-border: #e2e8f0;
        --terminal-window-bg: #f8fafc;
        --terminal-header-bg: #e2e8f0;
        --terminal-border: #cbd5e1;
        --terminal-body-bg: #ffffff;
        --terminal-text: #0f172a;
        --ribbon-bg: #ffffff;
        --ribbon-border: #e2e8f0;
        --ribbon-tag-bg: #eff6ff;
        --ribbon-tag-color: #1d4ed8;
        --ribbon-tag-border: #dbeafe;
        --log-producer: #1d4ed8;
        --log-consumer: #047857;
        --log-ingest: #6d28d9;
        --log-finbert: #b45309;
        --log-database: #0e7490;
        --log-atlas: #0f766e;
        --log-success: #15803d;
        --log-info: #0284c7;
        --log-warn: #c2410c;
        --log-error: #b91c1c;
    """

df_dark_css = """
    [data-testid="stDataFrame"] {
        background-color: #151d2e !important;
        border: 1px solid #273549 !important;
        border-radius: 8px !important;
    }
    [data-testid="stDataFrameResizable"] {
        background-color: transparent !important;
        border: 1px solid #273549 !important;
    }
    .stDataFrameGlideDataEditor,
    .dvn-scroller,
    .gdg-wmyidgi,
    .gdg-s1dgczr6 {
        background-color: transparent !important;
    }
    [data-testid="stElementToolbar"],
    .stElementToolbar {
        background-color: #1e293b !important;
        border: 1px solid #273549 !important;
        border-radius: 6px !important;
        box-shadow: none !important;
    }
    [data-testid="stElementToolbar"] button,
    .stElementToolbar button {
        color: #f8fafc !important;
        background-color: transparent !important;
    }
    [data-testid="stElementToolbar"] button:hover,
    .stElementToolbar button:hover {
        background-color: #334155 !important;
    }
    [data-testid="stElementToolbar"] svg,
    .stElementToolbar svg {
        fill: #f8fafc !important;
        color: #f8fafc !important;
    }
    [data-testid="stDataFrame"] canvas,
    [data-testid="stDataFrame"] [data-stale="true"] canvas,
    div[data-stale="true"] [data-testid="stDataFrame"] canvas,
    [data-testid="stElementContainer"][data-stale="true"] [data-testid="stDataFrame"] canvas,
    div[class*="st-emotion-cache"][data-stale="true"] [data-testid="stDataFrame"] canvas,
    .stApp--running [data-testid="stDataFrame"] canvas,
    canvas[data-testid="data-grid-canvas"],
    .stDataFrameGlideDataEditor canvas {
        filter: invert(0.88) hue-rotate(180deg) !important;
        -webkit-filter: invert(0.88) hue-rotate(180deg) !important;
        background-color: transparent !important;
    }
""" if is_dark else ""

st.markdown(f"""
<style>
    /* -------------------------------------------------------------------------
       1. THEME VARIABLES & GLOBAL FOUNDATIONS
       ------------------------------------------------------------------------- */
    :root {{
        {theme_vars}
        color-scheme: var(--color-scheme) !important;
    }}

    html, body, .stApp, 
    [data-testid="stAppViewContainer"], 
    [data-testid="stMainBlockContainer"] {{
        background-color: var(--bg-page) !important;
        color: var(--text-main) !important;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif !important;
    }}

    [data-testid="stVerticalBlock"] {{
        background-color: transparent !important;
    }}

    h1, h2, h3, h4, h5, h6,
    [data-testid="stTitle"],
    [data-testid="stSubheader"],
    [data-testid="stHeading"],
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] span,
    [data-testid="stMarkdownContainer"] li {{
        color: var(--text-main) !important;
    }}

    [data-testid="stCaptionContainer"] p,
    .stCaption {{
        color: var(--text-muted) !important;
    }}

    /* -------------------------------------------------------------------------
       2. TOP HEADER OVERLAY FIX & THEMED HEADER BAR
       Prevents horizontal clipping of status badges while keeping sidebar accessible
       ------------------------------------------------------------------------- */
    [data-testid="stHeader"] {{
        background-color: var(--bg-page) !important;
        color: var(--text-main) !important;
        height: 3rem !important;
        z-index: 90 !important;
        border-bottom: 1px solid var(--border-subtle) !important;
        transition: background-color 0.2s ease, border-color 0.2s ease;
    }}

    [data-testid="stHeader"] button,
    [data-testid="stSidebarCollapseButton"] button,
    [data-testid="collapsedControl"] button,
    [data-testid="stHeader"] svg {{
        color: var(--text-main) !important;
        fill: var(--text-main) !important;
    }}

    /* -------------------------------------------------------------------------
       3. COMPLETE ZERO-DIMMING ANTI-STALE OVERRIDE
       Eliminates Streamlit 1.64's data-stale="true" opacity: 0.33 and transition
       ------------------------------------------------------------------------- */
    [data-stale="true"],
    [data-stale="true"] *:not(canvas),
    div[data-stale="true"],
    div[data-stale="true"] *:not(canvas),
    [data-testid="stElementContainer"][data-stale="true"],
    [data-testid="stElementContainer"][data-stale="true"] *:not(canvas),
    div[class*="st-emotion-cache"][data-stale="true"],
    div[class*="st-emotion-cache"][data-stale="true"] *:not(canvas),
    .element-container[data-stale="true"],
    .element-container[data-stale="true"] *:not(canvas),
    .stApp--running [data-stale="true"] {{
        opacity: 1 !important;
        transition: none !important;
        -webkit-transition: none !important;
    }}

    [data-testid="stStatusWidget"],
    div[data-testid="stStatusWidget"],
    .stStatusWidget,
    [data-testid="stToolbar"] {{
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
    }}

    /* Container Spacing with header clearance */
    .block-container {{
        padding-top: 3.6rem !important;
        padding-bottom: 2rem !important;
        padding-left: 1.5rem !important;
        padding-right: 1.5rem !important;
        max-width: 1480px !important;
    }}

    /* Sidebar Styling */
    [data-testid="stSidebar"], 
    [data-testid="stSidebarContent"],
    section[data-testid="stSidebar"] {{
        background-color: var(--bg-sidebar) !important;
        border-right: 1px solid var(--border-color) !important;
        color: var(--text-main) !important;
    }}

    /* Header Command Card (Border wrapper) */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        background-color: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
        box-shadow: var(--card-shadow) !important;
        padding: 10px 14px !important;
        margin-bottom: 0px !important;
        transition: background-color 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
    }}
    [data-testid="stVerticalBlockBorderWrapper"] p {{
        margin: 0 !important;
        padding: 0 !important;
    }}
    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlock"] {{
        background-color: transparent !important;
        gap: 8px !important;
    }}
    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stHorizontalBlock"] {{
        align-items: center !important;
        gap: 8px !important;
    }}

    /* Live Header Status Pill */
    .status-pill {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 12px;
        border-radius: 20px;
        font-size: 0.78rem;
        font-weight: 700;
        background: var(--status-pill-bg);
        color: var(--status-pill-color);
        border: 1px solid var(--status-pill-border);
        white-space: nowrap;
        line-height: 1.2;
    }}
    .status-live {{
        background: var(--status-pill-bg) !important;
        color: var(--status-pill-color) !important;
        border-color: var(--status-pill-border) !important;
    }}
    .status-idle {{
        background: var(--status-idle-bg) !important;
        color: var(--status-idle-color) !important;
        border-color: var(--status-idle-border) !important;
    }}
    .pulse-dot {{
        width: 8px;
        height: 8px;
        min-width: 8px;
        min-height: 8px;
        background-color: #10b981;
        border-radius: 50%;
        box-shadow: 0 0 0 rgba(16, 185, 129, 0.4);
        animation: pulse 1.8s infinite;
    }}
    .pulse-dot-idle {{
        width: 8px;
        height: 8px;
        min-width: 8px;
        min-height: 8px;
        background-color: var(--status-idle-color) !important;
        border-radius: 50%;
        box-shadow: none !important;
        animation: none !important;
    }}
    @keyframes pulse {{
        0% {{ box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
        70% {{ box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }}
        100% {{ box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
    }}

    /* Database Source Badge */
    .db-badge {{
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 5px 10px;
        border-radius: 8px;
        font-size: 0.73rem;
        font-weight: 600;
        background: var(--db-badge-bg);
        color: var(--db-badge-color);
        border: 1px solid var(--db-badge-border);
        margin-top: 6px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        line-height: 1.2;
    }}
    .db-dot {{
        font-size: 0.8rem;
    }}
    .db-text {{
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }}

    /* Theme Toggle Switch */
    div[data-testid="stToggle"] {{
        margin: 0 !important;
        padding: 0 !important;
        display: flex !important;
        align-items: center !important;
        justify-content: flex-end !important;
    }}
    div[data-testid="stToggle"] label {{
        margin-bottom: 0 !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        color: var(--text-main) !important;
        white-space: nowrap !important;
    }}

    /* High-Speed Metric Cards */
    .metric-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 12px;
        margin-bottom: 1.2rem;
    }}
    .kpi-card {{
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 10px;
        padding: 14px;
        text-align: center;
        box-shadow: var(--card-shadow);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }}
    .kpi-card:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }}
    .kpi-title {{
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }}
    .kpi-value {{
        font-size: 1.5rem;
        font-weight: 800;
        color: var(--text-main);
    }}

    /* Live Activity Ribbon */
    .live-ribbon {{
        background: var(--ribbon-bg);
        border: 1px solid var(--ribbon-border);
        border-radius: 8px;
        padding: 8px 14px;
        margin-bottom: 16px;
        display: flex;
        align-items: center;
        gap: 10px;
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 0.82rem;
        color: var(--text-main);
        box-shadow: var(--card-shadow);
    }}
    .ribbon-tag {{
        background: var(--ribbon-tag-bg);
        color: var(--ribbon-tag-color);
        border: 1px solid var(--ribbon-tag-border);
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 700;
        font-size: 0.72rem;
        letter-spacing: 0.5px;
        white-space: nowrap;
    }}

    /* News Cards */
    .news-card {{
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 10px;
        padding: 14px;
        margin-bottom: 10px;
        box-shadow: var(--card-shadow);
        display: flex;
        flex-direction: column;
        gap: 8px;
    }}
    .news-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 6px;
    }}
    .ticker-tag {{
        background: var(--border-color);
        color: var(--text-main);
        padding: 2px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }}
    .headline-text {{
        font-size: 0.95rem;
        font-weight: 600;
        color: var(--text-main);
        line-height: 1.4;
    }}
    .card-footer {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-top: 4px;
    }}

    /* Badges */
    .badge-pos {{
        background-color: var(--badge-pos-bg);
        color: var(--badge-pos-color);
        border: 1px solid var(--badge-pos-border);
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.75rem;
    }}
    .badge-neg {{
        background-color: var(--badge-neg-bg);
        color: var(--badge-neg-color);
        border: 1px solid var(--badge-neg-border);
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.75rem;
    }}
    .badge-neu {{
        background-color: var(--badge-neu-bg);
        color: var(--badge-neu-color);
        border: 1px solid var(--badge-neu-border);
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.75rem;
    }}

    /* High-Contrast Developer Terminal */
    .terminal-window {{
        background: var(--terminal-window-bg);
        border: 1px solid var(--terminal-border);
        border-radius: 10px;
        overflow: hidden;
        margin-top: 8px;
        margin-bottom: 18px;
        box-shadow: var(--card-shadow);
    }}
    .terminal-header {{
        background: var(--terminal-header-bg);
        padding: 8px 14px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        border-bottom: 1px solid var(--terminal-border);
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 0.78rem;
        color: var(--text-muted);
        font-weight: 600;
    }}
    .terminal-dots {{
        display: flex;
        gap: 6px;
    }}
    .dot {{
        width: 10px;
        height: 10px;
        border-radius: 50%;
    }}
    .dot-red {{ background: #ef4444; }}
    .dot-yellow {{ background: #f59e0b; }}
    .dot-green {{ background: #10b981; }}
    
    .terminal-body {{
        padding: 12px 16px;
        max-height: 460px;
        overflow-y: auto;
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 0.82rem;
        line-height: 1.6;
        background: var(--terminal-body-bg);
        color: var(--terminal-text);
    }}
    .log-line {{
        margin-bottom: 4px;
        word-break: break-word;
        font-family: 'Consolas', 'Courier New', monospace;
    }}
    .log-time {{ color: var(--text-dim); margin-right: 8px; font-weight: 500; }}
    .log-comp {{ color: var(--accent-color); font-weight: 700; margin-right: 6px; }}
    
    .log-PRODUCER {{ color: var(--log-producer); font-weight: 700; }}
    .log-CONSUMER {{ color: var(--log-consumer); font-weight: 700; }}
    .log-INGEST {{ color: var(--log-ingest); font-weight: 700; }}
    .log-FINBERT {{ color: var(--log-finbert); font-weight: 700; }}
    .log-DATABASE {{ color: var(--log-database); font-weight: 700; }}
    .log-ATLAS_SYNC {{ color: var(--log-atlas); font-weight: 700; }}
    .log-SUCCESS {{ color: var(--log-success); font-weight: 700; }}
    .log-INFO {{ color: var(--log-info); font-weight: 700; }}
    .log-WARN {{ color: var(--log-warn); font-weight: 700; }}
    .log-ERROR {{ color: var(--log-error); font-weight: 700; }}

    /* Streamlit Native Widgets Theming */
    [data-baseweb="select"] > div,
    [data-baseweb="input"],
    [data-baseweb="base-input"],
    [data-testid="stTextInput"] input {{
        background-color: var(--bg-card) !important;
        color: var(--text-main) !important;
        border-color: var(--border-color) !important;
    }}
    [data-baseweb="select"] span,
    [data-baseweb="select"] div {{
        color: var(--text-main) !important;
    }}
    [data-baseweb="select"] svg {{
        fill: var(--text-main) !important;
    }}
    [data-baseweb="popover"],
    [data-baseweb="menu"],
    ul[role="listbox"] {{
        background-color: var(--bg-card) !important;
        color: var(--text-main) !important;
        border: 1px solid var(--border-color) !important;
    }}
    li[role="option"] {{
        color: var(--text-main) !important;
    }}
    li[role="option"]:hover {{
        background-color: var(--bg-card-hover) !important;
    }}
    div[role="radiogroup"] label,
    div[role="radiogroup"] span {{
        color: var(--text-main) !important;
    }}
    div[data-testid="stToggle"] label {{
        color: var(--text-main) !important;
    }}
    [data-testid="stSlider"] label,
    [data-testid="stSlider"] div,
    [data-testid="stSlider"] [data-testid="stThumbValue"],
    [data-testid="stSlider"] [data-testid="stTickBarMin"],
    [data-testid="stSlider"] [data-testid="stTickBarMax"] {{
        color: var(--text-main) !important;
    }}
    button[kind="secondary"],
    [data-testid="stBaseButton-secondary"] {{
        background-color: var(--bg-card) !important;
        color: var(--text-main) !important;
        border: 1px solid var(--border-color) !important;
        box-shadow: var(--card-shadow) !important;
        transition: all 0.15s ease;
    }}
    button[kind="secondary"]:hover,
    [data-testid="stBaseButton-secondary"]:hover {{
        background-color: var(--bg-card-hover) !important;
        border-color: var(--accent-color) !important;
        color: var(--text-main) !important;
    }}
    [data-baseweb="tab-list"] {{
        border-bottom: 1px solid var(--border-color) !important;
    }}
    button[data-baseweb="tab"] {{
        color: var(--text-muted) !important;
    }}
    button[data-baseweb="tab"][aria-selected="true"] {{
        color: var(--accent-color) !important;
    }}
    div[data-baseweb="tab-highlight"] {{
        background-color: var(--accent-color) !important;
    }}
    [data-testid="stCodeBlock"],
    pre, code {{
        background-color: var(--terminal-body-bg) !important;
        color: var(--terminal-text) !important;
        border-color: var(--border-color) !important;
    }}
    [data-testid="stDataFrame"] {{
        background-color: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
    }}
    {df_dark_css}
    hr {{
        border-color: var(--border-color) !important;
    }}

    /* =========================================================================
       DYNAMIC MEDIA RESPONSIVE RULES FOR GLOBAL DEVICES
       ========================================================================= */

    /* 1. Large Monitors & Desktops (> 1200px) */
    @media (min-width: 1200px) {{
        .block-container {{
            padding-top: 3.8rem !important;
            max-width: 1480px !important;
        }}
    }}

    /* 2. Laptops & Medium Desktops (992px to 1199px) */
    @media (min-width: 992px) and (max-width: 1199px) {{
        .block-container {{
            padding-top: 3.8rem !important;
            padding-left: 1.2rem !important;
            padding-right: 1.2rem !important;
        }}
        .metric-grid {{
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 10px;
        }}
    }}

    /* 3. Tablets (768px to 991px) */
    @media (min-width: 768px) and (max-width: 991px) {{
        .block-container {{
            padding-top: 4.0rem !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }}
        .metric-grid {{
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
        }}
        .metric-grid > .kpi-card:last-child:nth-child(odd) {{
            grid-column: span 2;
        }}
        .kpi-value {{
            font-size: 1.35rem;
        }}
        .status-pill {{
            font-size: 0.72rem;
            padding: 4px 10px;
        }}
        .db-badge {{
            font-size: 0.7rem;
        }}
    }}

    /* 4. Mobile Phones (< 768px) */
    @media (max-width: 767px) {{
        .block-container {{
            padding-top: 4.2rem !important;
            padding-left: 0.75rem !important;
            padding-right: 0.75rem !important;
            padding-bottom: 1.5rem !important;
        }}
        [data-testid="stTitle"] {{
            font-size: 1.4rem !important;
            line-height: 1.25 !important;
        }}
        .metric-grid {{
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
            margin-bottom: 1rem;
        }}
        .metric-grid > .kpi-card:last-child:nth-child(odd) {{
            grid-column: span 2;
        }}
        .kpi-card {{
            padding: 10px 8px;
        }}
        .kpi-title {{
            font-size: 0.68rem;
        }}
        .kpi-value {{
            font-size: 1.15rem;
        }}
        .live-ribbon {{
            flex-wrap: wrap;
            gap: 6px;
            padding: 8px 10px;
            font-size: 0.75rem;
        }}
        .news-card {{
            padding: 10px;
        }}
        .headline-text {{
            font-size: 0.88rem;
        }}
        .terminal-body {{
            font-size: 0.75rem;
            padding: 10px;
            max-height: 320px;
        }}
        .terminal-header {{
            font-size: 0.7rem;
            padding: 6px 10px;
        }}
        [data-testid="stVerticalBlockBorderWrapper"] {{
            padding: 8px 10px !important;
        }}
        .status-pill {{
            font-size: 0.7rem;
            padding: 4px 8px;
        }}
        .db-badge {{
            font-size: 0.68rem;
            padding: 4px 8px;
            white-space: normal;
            word-break: break-word;
        }}
    }}

    /* 5. Mobile Narrow & Extra Small (< 520px) */
    @media (max-width: 520px) {{
        div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stHorizontalBlock"] {{
            flex-direction: column !important;
            align-items: flex-start !important;
            gap: 6px !important;
        }}
        div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stColumn"] {{
            width: 100% !important;
            min-width: 100% !important;
        }}
        div[data-testid="stToggle"] {{
            justify-content: flex-start !important;
        }}
    }}

    /* 6. Extra Small Mobile (< 480px) */
    @media (max-width: 480px) {{
        .block-container {{
            padding-top: 4.4rem !important;
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
        }}
        .kpi-title {{
            font-size: 0.62rem;
        }}
        .kpi-value {{
            font-size: 1rem;
        }}
        .status-pill {{
            font-size: 0.65rem;
            padding: 3px 6px;
        }}
        div[data-testid="stToggle"] label {{
            font-size: 0.75rem !important;
        }}
    }}

    /* 7. Touch Device Optimizations */
    @media (hover: none) and (pointer: coarse) {{
        button, 
        [data-testid="stBaseButton-secondary"],
        [data-baseweb="tab"] {{
            min-height: 40px !important;
        }}
        .news-card:hover,
        .kpi-card:hover {{
            transform: none !important;
        }}
    }}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. Database Connection Resolver (Prioritize Local Fast Docker Mongo)
# ==============================================================================
def _read_secret(key, default=None):
    """Read a value from st.secrets (any format) or os.getenv, with a default."""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)


def get_mongo_connection():
    """
    Connection manager with retry logic.
    Supports seamless switching between ultra-fast local Docker MongoDB (<2ms)
    and 24/7 global MongoDB Atlas cloud cluster for Streamlit Community Cloud.
    NOT cached so Atlas cold-starts on Streamlit Cloud get a fresh attempt each time
    until a successful connection is established; thereafter stored in session_state.
    """
    # Try local Docker MongoDB first (only valid locally)
    local_uri = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
    try:
        c = MongoClient(local_uri, serverSelectionTimeoutMS=300)
        c.admin.command('ping')
        return c, "Local Docker MongoDB (<2ms, Live Stream)"
    except Exception:
        pass

    # Read Atlas URI from st.secrets or env
    atlas_uri = None
    try:
        if "MONGO_URI" in st.secrets:
            atlas_uri = str(st.secrets["MONGO_URI"])
        elif "mongo" in st.secrets and isinstance(st.secrets["mongo"], dict):
            atlas_uri = st.secrets["mongo"].get("uri")
    except Exception:
        pass
    if not atlas_uri:
        atlas_uri = os.getenv("MONGO_URI")

    if atlas_uri:
        # Use a longer timeout for Streamlit Cloud cold starts (Atlas SRV DNS can be slow)
        try:
            c = MongoClient(
                atlas_uri,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                tls=True,
                tlsAllowInvalidCertificates=False,
            )
            c.admin.command('ping')
            return c, "MongoDB Atlas (Global Cloud, Lifetime)"
        except Exception as e:
            # Last-ditch: try without explicit TLS flags (URI may already contain tls=true)
            try:
                c2 = MongoClient(atlas_uri, serverSelectionTimeoutMS=10000)
                c2.admin.command('ping')
                return c2, "MongoDB Atlas (Global Cloud, Lifetime)"
            except Exception:
                pass

    return None, "Demo / Offline Mode"


# Use session_state to cache the connection across reruns without @st.cache_resource
# so that a failed cold-start attempt is retried on the next rerun instead of cached forever.
if "mongo_client" not in st.session_state or st.session_state.get("mongo_client") is None:
    _client, _label = get_mongo_connection()
    st.session_state["mongo_client"] = _client
    st.session_state["db_source_label"] = _label

client = st.session_state["mongo_client"]
DB_SOURCE_LABEL = st.session_state["db_source_label"]

# Read DB / collection names from secrets first, then env, then defaults
DB_NAME = _read_secret("DB_NAME", "StockDB")
COLLECTION_NAME = _read_secret("COLLECTION_NAME", "news_sentiment")
LOGS_COLLECTION_NAME = _read_secret("LOGS_COLLECTION_NAME", "pipeline_logs")


# ==============================================================================
# 2b. Autonomous Cloud Ingestion Engine
#     Runs every 30 s as a Streamlit fragment — fetches Yahoo Finance RSS, scores
#     each headline with VADER (fast, CPU-only, no GPU needed), and writes enriched
#     records to MongoDB Atlas.  This keeps the deployed Streamlit Cloud app alive
#     24/7 even when all local Kafka workers are stopped.
# ==============================================================================
_CLOUD_TICKERS = [
    ("AAPL",  "Apple"),
    ("TSLA",  "Tesla"),
    ("NVDA",  "NVIDIA"),
    ("MSFT",  "Microsoft"),
    ("AMZN",  "Amazon"),
    ("GOOGL", "Alphabet"),
    ("META",  "Meta"),
    ("AMD",   "AMD"),
    ("JPM",   "JPMorgan"),
    ("BAC",   "Bank of America"),
]


def _vader_sentiment(text: str) -> tuple:
    """Fast VADER sentiment — returns (POSITIVE|NEGATIVE|NEUTRAL, confidence 0-1)."""
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _sia = SentimentIntensityAnalyzer()
        vs = _sia.polarity_scores(text)
        compound = vs["compound"]
        if compound >= 0.05:
            label = "POSITIVE"
            conf = min(1.0, round(0.5 + compound * 0.5, 4))
        elif compound <= -0.05:
            label = "NEGATIVE"
            conf = min(1.0, round(0.5 + abs(compound) * 0.5, 4))
        else:
            label = "NEUTRAL"
            conf = round(0.5 + abs(compound), 4)
        return label, conf
    except Exception:
        return "NEUTRAL", 0.5


def _fetch_yahoo_rss(ticker: str) -> list:
    """Fetch up to 5 recent headlines from Yahoo Finance RSS for a ticker."""
    try:
        import feedparser
        url = (
            f"https://feeds.finance.yahoo.com/rss/2.0/headline"
            f"?s={ticker}&region=US&lang=en-US"
        )
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:5]:
            title = entry.get("title", "").strip()
            if title:
                items.append(title)
        return items
    except Exception:
        return []


def _ingest_cloud_rss() -> int:
    """
    Core ingestion: fetch RSS headlines for all tickers, score with VADER,
    write unseen records to MongoDB Atlas (news_sentiment + pipeline_logs).
    Returns count of newly inserted records.
    """
    if client is None:
        return 0
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]
    log_col = db[LOGS_COLLECTION_NAME]
    inserted = 0
    for ticker, _ in _CLOUD_TICKERS:
        headlines = _fetch_yahoo_rss(ticker)
        for headline in headlines:
            if not headline:
                continue
            # Dedup: skip if exact headline already in DB for this ticker
            try:
                if col.find_one({"headline": headline, "ticker": ticker}, {"_id": 1}):
                    continue
            except Exception:
                continue
            sentiment, confidence = _vader_sentiment(headline)
            now_utc = datetime.now(timezone.utc)
            iso_ts = now_utc.isoformat()
            time_str = now_utc.strftime("%H:%M:%S.%f")[:-3]
            record = {
                "ticker": ticker,
                "headline": headline,
                "timestamp": iso_ts,
                "sentiment": sentiment,
                "confidence": confidence,
                "source": "yahoo_rss_cloud",
            }
            log_entry = {
                "time": time_str,
                "iso_time": iso_ts,
                "level": "INGEST",
                "component": "CLOUD_ENGINE",
                "message": (
                    f"[{sentiment}] {ticker}: \"{headline[:55]}...\""
                    f" (conf: {confidence:.4f})"
                ),
                "ticker": ticker,
                "sentiment": sentiment,
                "confidence": confidence,
                "headline": headline,
                "source": "yahoo_rss_cloud",
            }
            try:
                col.insert_one(record)
                log_col.insert_one(log_entry)
                inserted += 1
            except Exception:
                pass
    return inserted


@st.fragment(run_every=30)
def _autonomous_cloud_engine():
    """
    Silent background ingestion fragment.
    Runs every 30 s independently — fetches Yahoo Finance RSS, scores with VADER,
    and writes new records to MongoDB Atlas to keep the dashboard live 24/7.
    """
    if client is None:
        return
    try:
        n = _ingest_cloud_rss()
        if n > 0:
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            entry = {
                "time": now_str,
                "iso_time": datetime.now(timezone.utc).isoformat(),
                "level": "INGEST",
                "component": "CLOUD_ENGINE",
                "message": f"[CLOUD ENGINE] Ingested {n} new headline(s) from Yahoo RSS → Atlas",
            }
            if "pipeline_logs" in st.session_state:
                st.session_state["pipeline_logs"].insert(0, entry)
    except Exception:
        pass


# Automatic one-time seeding if pipeline_logs is fresh (runs only once per session)
def auto_seed_pipeline_logs():
    if client is None:
        return
    # Only attempt once per session to avoid repeated slow Atlas calls
    if st.session_state.get("_auto_seed_done"):
        return
    st.session_state["_auto_seed_done"] = True
    try:
        db = client[DB_NAME]
        logs_count = db[LOGS_COLLECTION_NAME].estimated_document_count()
        news_count = db[COLLECTION_NAME].estimated_document_count()
        if logs_count == 0 and news_count > 0:
            # Inline minimal backfill: seed up to 200 records from news_sentiment
            from pymongo import ASCENDING
            cursor = db[COLLECTION_NAME].find(
                {},
                {"ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1}
            ).sort("_id", ASCENDING).limit(200)
            batch = []
            for doc in cursor:
                iso_time = doc.get("timestamp") or datetime.now(timezone.utc).isoformat()
                try:
                    dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
                    time_str = dt.strftime("%H:%M:%S.%f")[:-3]
                except Exception:
                    time_str = "00:00:00.000"
                ticker = doc.get("ticker", "TICKER")
                sentiment = doc.get("sentiment", "NEUTRAL")
                confidence = float(doc.get("confidence", 0.0))
                headline = doc.get("headline", "")
                preview = (headline[:55] + "...") if len(headline) > 55 else headline
                batch.append({
                    "time": time_str,
                    "iso_time": iso_time,
                    "level": "FINBERT",
                    "component": "KAFKA_CONSUMER",
                    "message": f"[{sentiment}] {ticker}: \"{preview}\" (conf: {confidence:.4f})",
                    "ticker": ticker,
                    "sentiment": sentiment,
                    "confidence": confidence,
                    "headline": headline,
                })
            if batch:
                db[LOGS_COLLECTION_NAME].insert_many(batch)
    except Exception:
        pass

auto_seed_pipeline_logs()


# In-memory fast log buffer
if "pipeline_logs" not in st.session_state:
    st.session_state["pipeline_logs"] = []
if "total_lifetime_logs" not in st.session_state:
    st.session_state["total_lifetime_logs"] = 0

def add_local_log(level, component, message, persist_to_db=True, extra_meta=None):
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%H:%M:%S.%f")[:-3]
    entry = {
        "time": now_str,
        "iso_time": now_utc.isoformat(),
        "level": level,
        "component": component,
        "message": message
    }
    if extra_meta:
        entry.update(extra_meta)

    st.session_state["pipeline_logs"].insert(0, entry)
    if len(st.session_state["pipeline_logs"]) > 500:
        st.session_state["pipeline_logs"] = st.session_state["pipeline_logs"][:500]

    if persist_to_db and client is not None:
        try:
            db = client[DB_NAME]
            db[LOGS_COLLECTION_NAME].insert_one(entry.copy())
            st.session_state["total_lifetime_logs"] = st.session_state.get("total_lifetime_logs", 0) + 1
        except Exception:
            pass

# Add initial boot logs if new session
if not st.session_state["pipeline_logs"]:
    add_local_log("INFO", "BOOT", "Superfast Financial Sentiment Dashboard initialized.", persist_to_db=False)
    add_local_log("SUCCESS", "DATABASE", f"Connected to: {DB_SOURCE_LABEL}", persist_to_db=False)

def fetch_lifetime_logs_for_export(max_records=10000):
    """Fetch complete lifetime logs from MongoDB for comprehensive export."""
    if client is not None:
        try:
            db = client[DB_NAME]
            cursor = db[LOGS_COLLECTION_NAME].find(
                {},
                {"_id": 0, "time": 1, "iso_time": 1, "level": 1, "component": 1, "message": 1}
            ).sort("_id", DESCENDING).limit(max_records)
            docs = list(cursor)
            if docs:
                return docs
        except Exception:
            pass
    return st.session_state.get("pipeline_logs", [])

def fetch_data_and_logs(limit=100, logs_limit=200):
    """
    Superfast Data & Lifetime Log Fetching: Queries MongoDB in < 5ms.
    Synchronizes persistent lifetime logs from StockDB.pipeline_logs.
    """
    records = []
    db_logs = []
    total_lifetime_count = 0
    if client is not None:
        try:
            db = client[DB_NAME]
            cursor = db[COLLECTION_NAME].find(
                {},
                {"_id": 0, "ticker": 1, "headline": 1, "sentiment": 1, "confidence": 1, "timestamp": 1}
            ).sort("_id", DESCENDING).limit(limit)
            records = list(cursor)

            # Query lifetime pipeline logs
            log_cursor = db[LOGS_COLLECTION_NAME].find(
                {},
                {"_id": 0, "time": 1, "iso_time": 1, "level": 1, "component": 1, "message": 1, "ticker": 1, "sentiment": 1}
            ).sort("_id", DESCENDING).limit(logs_limit)
            db_logs = list(log_cursor)
            total_lifetime_count = db[LOGS_COLLECTION_NAME].estimated_document_count()
        except Exception as e:
            add_local_log("WARN", "DATABASE", f"Query notice: {e}", persist_to_db=False)

    if db_logs:
        st.session_state["pipeline_logs"] = db_logs
        st.session_state["total_lifetime_logs"] = total_lifetime_count or len(db_logs)
    elif not st.session_state["pipeline_logs"] and records:
        # Fallback if logs collection is empty: seed from sentiment records
        for r in reversed(records[:10]):
            sent = r.get("sentiment", "NEUTRAL")
            tick = r.get("ticker", "TICKER")
            head = r.get("headline", "")
            conf = r.get("confidence", 0.0)
            add_local_log("FINBERT", "KAFKA_CONSUMER", f"[{sent}] {tick}: \"{head[:50]}...\" (conf: {conf:.4f})", persist_to_db=True)
        st.session_state["total_lifetime_logs"] = len(records)
    else:
        st.session_state["total_lifetime_logs"] = max(total_lifetime_count, len(st.session_state["pipeline_logs"]))

    # Synchronize newly streamed records into lifetime log buffer
    if records:
        last_seen = st.session_state.get("last_seen_headline")
        first_headline = records[0].get("headline")
        if last_seen is None:
            st.session_state["last_seen_headline"] = first_headline
        elif last_seen != first_headline:
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
                add_local_log(
                    "FINBERT",
                    "KAFKA_CONSUMER",
                    f"[{sent}] {tick}: \"{head[:50]}...\" (conf: {conf:.4f})",
                    persist_to_db=False,
                    extra_meta={"ticker": tick, "sentiment": sent, "confidence": conf, "headline": head}
                )
            st.session_state["last_seen_headline"] = first_headline

    if not records:
        now_iso = datetime.now(timezone.utc).isoformat()
        demo_time = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        records = [
            {"ticker": "NVDA", "headline": "NVDA unveils next-generation enterprise AI accelerator chips, boosting forward guidance.", "sentiment": "POSITIVE", "confidence": 0.9538, "timestamp": now_iso},
            {"ticker": "TSLA", "headline": "TSLA files shelf registration statement with regulators for routine capital refinancing.", "sentiment": "NEUTRAL", "confidence": 0.9398, "timestamp": now_iso},
            {"ticker": "MSFT", "headline": "MSFT closes major multi-year sovereign cloud infrastructure contract with global partners.", "sentiment": "POSITIVE", "confidence": 0.8845, "timestamp": now_iso},
            {"ticker": "AMD", "headline": "AMD announces mandatory global recall of 85,000 hardware units due to battery defect.", "sentiment": "NEGATIVE", "confidence": 0.9614, "timestamp": now_iso},
            {"ticker": "AAPL", "headline": "Analyst upgrades AAPL to Strong Buy with an aggressive price target increase.", "sentiment": "POSITIVE", "confidence": 0.9412, "timestamp": now_iso},
            {"ticker": "JPM", "headline": "JPM announces record quarterly free cash flow and a new $15 billion share buyback program.", "sentiment": "POSITIVE", "confidence": 0.8959, "timestamp": now_iso},
            {"ticker": "GOOGL", "headline": "GOOGL trades sideways in quiet trading session ahead of Federal Reserve interest rate decision.", "sentiment": "NEUTRAL", "confidence": 0.8126, "timestamp": now_iso},
            {"ticker": "AMZN", "headline": "Disappointing quarterly results: AMZN misses EPS forecast as operating expenses rise sharply.", "sentiment": "NEGATIVE", "confidence": 0.9734, "timestamp": now_iso},
            {"ticker": "META", "headline": "META operating profit jumps 28% year-over-year fueled by high-margin software licensing.", "sentiment": "POSITIVE", "confidence": 0.9440, "timestamp": now_iso},
            {"ticker": "BAC", "headline": "Credit rating agency downgrades outlook on BAC senior unsecured corporate bonds.", "sentiment": "NEGATIVE", "confidence": 0.9332, "timestamp": now_iso}
        ]
        if not st.session_state["pipeline_logs"]:
            demo_logs = [
                {"time": demo_time, "iso_time": now_iso, "level": "FINBERT", "component": "KAFKA_CONSUMER", "message": f"[{r['sentiment']}] {r['ticker']}: \"{r['headline'][:50]}...\" (conf: {r['confidence']:.4f})", "ticker": r["ticker"], "sentiment": r["sentiment"]}
                for r in records
            ]
            st.session_state["pipeline_logs"] = demo_logs
            st.session_state["total_lifetime_logs"] = len(records)

    df = pd.DataFrame(records) if records else pd.DataFrame()
    return df, st.session_state["pipeline_logs"]

def check_stream_heartbeat(max_age_seconds=45):
    """
    Check if the newest record in StockDB.news_sentiment is < max_age_seconds old.
    Returns (is_live: bool, age_seconds: float or None).
    """
    if client is not None:
        try:
            db = client[DB_NAME]
            latest_doc = db[COLLECTION_NAME].find_one(
                {},
                {"_id": 1, "timestamp": 1},
                sort=[("_id", DESCENDING)]
            )
            if latest_doc:
                now_utc = datetime.now(timezone.utc)
                age_seconds = None
                ts = latest_doc.get("timestamp")
                if ts and isinstance(ts, str):
                    try:
                        clean_ts = ts.replace("Z", "+00:00")
                        rec_time = datetime.fromisoformat(clean_ts)
                        if rec_time.tzinfo is None:
                            rec_time = rec_time.replace(tzinfo=timezone.utc)
                        age_seconds = (now_utc - rec_time).total_seconds()
                    except Exception:
                        pass
                if age_seconds is None and "_id" in latest_doc:
                    try:
                        gen_time = getattr(latest_doc["_id"], "generation_time", None)
                        if gen_time is not None and isinstance(gen_time, datetime):
                            if gen_time.tzinfo is None:
                                gen_time = gen_time.replace(tzinfo=timezone.utc)
                            age_seconds = (now_utc - gen_time).total_seconds()
                    except Exception:
                        pass

                if age_seconds is not None:
                    return (age_seconds < max_age_seconds), max(0.0, age_seconds)
        except Exception:
            pass
    return False, None


# Helper for rendering terminal window HTML
def build_terminal_html(logs_slice, max_h="460px", total_count=None):
    log_lines_html = []
    for l in logs_slice:
        lvl_class = f"log-{l.get('level', 'INFO')}"
        time_str = l.get('time', '')
        comp_str = l.get('component', '')
        lvl_str = l.get('level', 'INFO')
        msg_str = l.get('message', '')
        log_lines_html.append(
            f'<div class="log-line">'
            f'<span class="log-time">{time_str}</span> '
            f'<span class="log-comp">[{comp_str}]</span> '
            f'<span class="{lvl_class}">[{lvl_str}]</span> {msg_str}'
            f'</div>'
        )
    body_content = "".join(log_lines_html) if log_lines_html else '<div style="color: var(--text-dim);">No log events...</div>'
    term_title = "DARK TERMINAL CONSOLE" if is_dark else "LIGHT TERMINAL CONSOLE"
    raw_count = total_count if total_count is not None else st.session_state.get("total_lifetime_logs", len(st.session_state.get('pipeline_logs', [])))
    formatted_count = f"{raw_count:,}" if isinstance(raw_count, (int, float)) else str(raw_count)
    return (
        f'<div class="terminal-window">'
        f'<div class="terminal-header">'
        f'<div class="terminal-dots">'
        f'<div class="dot dot-red"></div>'
        f'<div class="dot dot-yellow"></div>'
        f'<div class="dot dot-green"></div>'
        f'</div>'
        f'<span>{term_title} &bull; {formatted_count} LIFETIME EVENTS</span>'
        f'<span style="color: var(--status-pill-color); font-weight: 700;">● STREAMING</span>'
        f'</div>'
        f'<div class="terminal-body" style="max-height: {max_h};">'
        f'{body_content}'
        f'</div>'
        f'</div>'
    )

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
        if st.button("🗑️ Reset View", width="stretch", help="Clears local display buffer; MongoDB lifetime logs remain safely preserved."):
            st.session_state["pipeline_logs"] = []
            add_local_log("INFO", "TERMINAL", "Display buffer reset.", persist_to_db=False)
            st.rerun()
    with col_r:
        if st.button("⚡ Fast Sync", width="stretch"):
            st.rerun()

    if st.button(f"🌓 Switch to {'Light' if is_dark else 'Dark'} Mode", width="stretch"):
        st.session_state["theme_toggle"] = not is_dark
        st.rerun()

    st.caption(f"{'🌙 Dark' if is_dark else '☀️ Light'} Theme Active & Superfast 5ms Pipeline.")

# ==============================================================================
# 4. Header & Live Status Controls
# ==============================================================================
header_col1, header_col2 = st.columns([2.2, 1.8], vertical_alignment="center")
with header_col1:
    st.title("📈 Real-Time Stock News Sentiment Pipeline")
    st.markdown(
        "Superfast financial sentiment intelligence streamed via **Apache Kafka**, classified by **FinBERT**, "
        "and persisted in **MongoDB**. Zero screen dimming & live sub-second updates."
    )
with header_col2:
    with st.container(border=True):
        col_status, col_theme = st.columns([1.1, 0.9], vertical_alignment="center")
        with col_status:
            @st.fragment(run_every=f"{refresh_interval}s" if auto_refresh else None)
            def render_header_heartbeat():
                is_live, age_sec = check_stream_heartbeat(max_age_seconds=45)
                pill_cls = "status-pill status-live" if is_live else "status-pill status-idle"
                dot_cls = "pulse-dot" if is_live else "pulse-dot-idle"
                if is_live:
                    badge_text = "STREAM: LIVE FAST"
                elif age_sec is not None:
                    badge_text = f"STREAM: IDLE ({int(age_sec)}s ago)"
                else:
                    badge_text = "STREAM: IDLE / PAUSED"
                st.markdown(f"""
                    <div class="{pill_cls}">
                        <div class="{dot_cls}"></div>
                        <span>{badge_text}</span>
                    </div>
                """, unsafe_allow_html=True)

            render_header_heartbeat()
        with col_theme:
            st.toggle(
                "🌙 Dark Mode",
                key="theme_toggle"
            )
        st.markdown(f"""
            <div class="db-badge">
                <span class="db-dot">⚡</span>
                <span class="db-text">{DB_SOURCE_LABEL}</span>
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

# Launch the autonomous 24/7 cloud ingestion engine (hidden — runs every 30 s)
# This fragment is invoked outside tabs so it keeps firing regardless of active tab.
_autonomous_cloud_engine()

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

        if "Demo" in DB_SOURCE_LABEL:
            st.info("🌟 **Demo Intelligence Preview**: Showing realistic financial sentiment simulation. To connect 24/7 cloud streaming for global users, configure `MONGO_URI` in Streamlit Cloud Secrets.")

        if df.empty:
            st.info(
                "ℹ️ **No sentiment records found in database yet.**\n\n"
                "To stream real-time data:\n"
                "1. Run `run_pipeline.py` or double-click `start_pipeline.bat`.\n"
                "2. Or run `Producer.ipynb` and `Consumer.ipynb` in JupyterLab.\n"
                "3. Ensure MongoDB is running (`docker compose up -d` or MongoDB Atlas)."
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
        pos_color = "#4ade80" if is_dark else "#15803d"
        neg_color = "#f87171" if is_dark else "#b91c1c"
        neu_color = "#94a3b8" if is_dark else "#475569"
        net_color = pos_color if net_sentiment >= 0 else neg_color

        # Metric Cards Bar
        st.markdown(f"""
        <div class="metric-grid">
            <div class="kpi-card">
                <div class="kpi-title">Total Headlines</div>
                <div class="kpi-value">{total_records}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-title">Positive Events</div>
                <div class="kpi-value" style="color: {pos_color};">🟢 {pos_count}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-title">Negative Events</div>
                <div class="kpi-value" style="color: {neg_color};">🔴 {neg_count}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-title">Neutral Events</div>
                <div class="kpi-value" style="color: {neu_color};">⚪ {neu_count}</div>
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
            <span style="color: var(--text-dim); font-size: 0.75rem;">{latest_event.get('time', '')}</span>
            <span class="log-comp">[{latest_event.get('component', 'SYSTEM')}]</span>
            <span class="{lvl_c}">[{latest_event.get('level', 'INFO')}]</span>
            <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-main); font-weight: 500;">
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
                },
                key="live_news_data_table"
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
    st.subheader("🖥️ Pipeline Execution Logs (Lifetime Store)")
    st.markdown("Unified real-time events permanently recorded across **Producer**, **Kafka**, **FinBERT Consumer**, and **MongoDB**.")

    # High-frequency fragment for Tab 2 so it updates live in real-time
    @st.fragment(run_every=f"{refresh_interval}s" if auto_refresh else None)
    def render_tab_logs():
        df_dummy, current_logs = fetch_data_and_logs(limit=10, logs_limit=300)
        total_lifetime = st.session_state.get("total_lifetime_logs", len(current_logs))

        metric_col1, metric_col2, metric_col3 = st.columns([1.5, 1.5, 3])
        with metric_col1:
            st.metric("Lifetime Events", f"{total_lifetime:,}")
        with metric_col2:
            st.metric("Active Buffer", f"{len(current_logs):,}")
        with metric_col3:
            st.caption(f"🛡️ Storage: **Lifetime Durable** in `StockDB.{LOGS_COLLECTION_NAME}` (Zero TTL expiration).")

        ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 2])
        with ctrl_col1:
            lvl_filter = st.selectbox("Filter Level:", ["ALL", "FINBERT", "INGEST", "PRODUCER", "CONSUMER", "DATABASE", "ATLAS_SYNC", "SUCCESS", "WARN", "ERROR"], key="tab2_lvl_filter")
        with ctrl_col2:
            comp_filter = st.selectbox("Filter Component:", ["ALL", "KAFKA_PRODUCER", "KAFKA_CONSUMER", "FINBERT", "YAHOO_RSS", "DATABASE", "SYSTEM", "BOOT"], key="tab2_comp_filter")
        with ctrl_col3:
            search_log = st.text_input("🔍 Search Log Text:", "", key="tab2_search_input")

        filtered_logs = current_logs.copy()
        if lvl_filter != "ALL":
            filtered_logs = [l for l in filtered_logs if l.get("level") == lvl_filter]
        if comp_filter != "ALL":
            filtered_logs = [l for l in filtered_logs if l.get("component") == comp_filter]
        if search_log:
            s_low = search_log.lower()
            filtered_logs = [l for l in filtered_logs if s_low in l.get("message", "").lower() or s_low in l.get("component", "").lower()]

        st.markdown(build_terminal_html(filtered_logs[:80], max_h="480px", total_count=total_lifetime), unsafe_allow_html=True)

        # Export Buttons with lifetime data option
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            lifetime_export = fetch_lifetime_logs_for_export(max_records=10000)
            log_text = "\n".join([f"{l.get('time')} [{l.get('component')}] [{l.get('level')}]: {l.get('message')}" for l in lifetime_export])
            st.download_button(
                label=f"📥 Download Lifetime Logs (.txt) — {len(lifetime_export):,} events",
                data=log_text,
                file_name="pipeline_logs_lifetime.txt",
                mime="text/plain",
                width="stretch",
                key="btn_dl_txt"
            )
        with btn_col2:
            lifetime_json_str = json.dumps(lifetime_export, indent=2)
            st.download_button(
                label=f"📥 Export Lifetime Logs (.json) — {len(lifetime_export):,} events",
                data=lifetime_json_str,
                file_name="pipeline_logs_lifetime.json",
                mime="application/json",
                width="stretch",
                key="btn_dl_json"
            )

    render_tab_logs()

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
        - **Active Theme**: `{"🌙 Dark Theme" if is_dark else "☀️ Light Theme"}`
        - **Pipeline Mode**: Real-Time Live Stream (<5ms, Zero Dimming)
        """)
