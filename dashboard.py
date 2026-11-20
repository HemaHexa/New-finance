from __future__ import annotations
 
import json
from pathlib import Path
 
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
 
ROOT = Path(__file__).resolve().parent
 
JOINED_DIR = ROOT / "output" / "gold_joined_real"
WINDOW_DIR = ROOT / "output" / "gold_windowed_real"
NEWS_DIR = ROOT / "input" / "silver_news"
TICKS_DIR = ROOT / "input" / "silver_ticks"
 
st.set_page_config(
    page_title="FinTel | Gold Streaming",
    page_icon="📈",
    layout="wide"
)
 
st.title("FinTel — Gold Streaming Monitor")
st.caption(
    "Real Silver inputs · Phase 2 Stock Silver · Phase 3 News Silver "
    "· Spark Structured Streaming Gold"
)
 
st.info(
    "This dashboard displays real Silver stock and news events. "
    "Spark performs the event-time stream-stream join and sliding-window "
    "aggregation using actual event timestamps."
)
 
def read_json_dir(folder: Path) -> pd.DataFrame:
    paths = list(folder.rglob("part-*.json")) if folder.exists() else []
    frames = []
 
    for path in paths:
        try:
            if path.stat().st_size:
                frames.append(pd.read_json(path, lines=True))
        except (ValueError, OSError, json.JSONDecodeError):
            continue
 
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
 
 
def count_json_events(folder: Path) -> int:
    paths = list(folder.glob("*.json")) if folder.exists() else []
    count = 0
 
    for path in paths:
        try:
            if path.stat().st_size:
                count += len(path.read_text(encoding="utf-8").splitlines())
        except OSError:
            continue
 
    return count
 
 
joined = read_json_dir(JOINED_DIR)
windowed = read_json_dir(WINDOW_DIR)
 
news_count = count_json_events(NEWS_DIR)
tick_count = count_json_events(TICKS_DIR)
 
c1, c2, c3, c4 = st.columns(4)
 
c1.metric("Real Silver news events", f"{news_count:,}")
c2.metric("Real Silver stock ticks", f"{tick_count:,}")
c3.metric("Gold joined rows", f"{len(joined):,}")
c4.metric("Gold window rows", f"{len(windowed):,}")
 
st.subheader("Gold processing configuration")
 
cfg1, cfg2, cfg3 = st.columns(3)
cfg1.metric("Event-time join range", "±10 minutes")
cfg2.metric("Window size", "5 minutes")
cfg3.metric("Slide interval", "1 minute")
 
tab1, tab2, tab3 = st.tabs([
    "Gold window aggregates",
    "Joined news + ticks",
    "How it works"
])
 
with tab1:
    if not windowed.empty:
        if "window_end" in windowed:
            windowed["window_end"] = pd.to_datetime(
                windowed["window_end"],
                errors="coerce",
                utc=True
            )
            windowed = windowed.sort_values(
                "window_end",
                ascending=False
            )
 
        st.dataframe(
            windowed,
            use_container_width=True,
            hide_index=True
        )
 
        chart_data = windowed.copy()
 
        if {"ticker", "avg_sentiment_score"}.issubset(chart_data.columns):
            latest = chart_data.groupby(
                "ticker",
                as_index=False
            ).tail(1)
 
            st.markdown("**Latest average sentiment score by ticker**")
            st.bar_chart(
                latest.set_index("ticker")["avg_sentiment_score"]
            )
    else:
        st.warning(
            "Waiting for completed Gold event-time windows. "
            "Append-mode aggregation emits results after the watermark "
            "passes the window end."
        )
 
with tab2:
    if not joined.empty:
        st.dataframe(
            joined.tail(200),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning(
            "No matching news and stock events yet. "
            "Gold requires matching tickers and event timestamps "
            "within ±10 minutes."
        )
 
with tab3:
    st.markdown("""
    **1. Phase 3 – Silver News**
    - Reads real enriched news records.
    - Uses ticker, headline, sentiment, sentiment score and published time.
 
    **2. Phase 2 – Silver Stock**
    - Reads real stock records from the Delta Silver table.
    - Uses symbol, current price, change percentage and market timestamp.
 
    **3. Gold stream-stream join**
    - Reads two independent Spark file streams.
    - Joins `news.ticker = ticks.symbol`.
    - Applies an event-time range of ±10 minutes.
    - Applies a 10-minute watermark to both streams.
 
    **4. Gold sliding-window aggregation**
    - Groups joined events into 5-minute windows.
    - Slides the window every 1 minute.
    - Uses a 10-minute watermark and append output mode.
    - Calculates joined count, average sentiment, average price,
      average change percentage and sentiment counts.
 
    **Important:** Historical news and stock records will only join
    when their actual event timestamps satisfy the ±10-minute condition.
    The dashboard does not manufacture matching records.
    """)
 
st.caption(
    "Dashboard refreshes every 3 seconds. "
    "Keep the adapter and Gold Spark streaming processes running."
)
 
st_autorefresh(interval=3000, key="gold_refresh")
