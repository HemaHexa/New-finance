from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
JOINED_DIR = ROOT / "output" / "gold_joined"
WINDOW_DIR = ROOT / "output" / "gold_windowed"
NEWS_DIR = ROOT / "input" / "silver_news"
TICKS_DIR = ROOT / "input" / "silver_ticks"

st.set_page_config(page_title="FinTel | Gold Streaming", page_icon="📈",
                   layout="wide")
st.title("FinTel — Gold Streaming Monitor")
st.caption("Demonstration mode · Mock Silver inputs · Spark Structured Streaming Gold")

st.info(
    "This dashboard displays synthetic upstream events. The Gold stream-stream "
    "join, event-time watermarks and sliding-window aggregation are executed by Spark."
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

joined = read_json_dir(JOINED_DIR)
windowed = read_json_dir(WINDOW_DIR)

news_files = list(NEWS_DIR.glob("*.json")) if NEWS_DIR.exists() else []
tick_files = list(TICKS_DIR.glob("*.json")) if TICKS_DIR.exists() else []
news_count = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in news_files)
tick_count = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in tick_files)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Mock Silver news events", f"{news_count:,}")
c2.metric("Mock Silver tick events", f"{tick_count:,}")
c3.metric("Gold joined rows", f"{len(joined):,}")
c4.metric("Gold window rows", f"{len(windowed):,}")

st.subheader("Gold processing configuration")
cfg1, cfg2, cfg3 = st.columns(3)
cfg1.metric("Watermark", "10 minutes")
cfg2.metric("Window size", "5 minutes")
cfg3.metric("Slide interval", "1 minute")

tab1, tab2, tab3 = st.tabs(["Gold window aggregates", "Joined news + ticks", "How it works"])

with tab1:
    if not windowed.empty:
        if "window_end" in windowed:
            windowed["window_end"] = pd.to_datetime(windowed["window_end"], errors="coerce", utc=True)
            windowed = windowed.sort_values("window_end", ascending=False)
        st.dataframe(windowed, use_container_width=True, hide_index=True)
        chart_data = windowed.copy()
        if {"ticker", "avg_sentiment_score"}.issubset(chart_data.columns):
            latest = chart_data.groupby("ticker", as_index=False).tail(1)
            st.markdown("**Latest average sentiment score by ticker**")
            st.bar_chart(latest.set_index("ticker")["avg_sentiment_score"])
    else:
        st.warning(
            "Waiting for Spark to close event-time windows. With a 10-minute "
            "watermark, append-mode window results appear after event time advances."
        )

with tab2:
    if not joined.empty:
        st.dataframe(joined.tail(200), use_container_width=True, hide_index=True)
    else:
        st.warning("Waiting for the Gold stream-stream join output.")

with tab3:
    st.markdown("""
    **1. Mock upstream**
    - Produces Silver-shaped news rows with `ticker`, `sentiment`,
      `sentiment_score` and `event_time`.
    - Produces Silver-shaped market rows with `symbol`, `current_price`
      and `market_timestamp` / `event_time`.

    **2. Gold stream-stream join**
    - Reads two independent Spark file streams.
    - Joins `news.ticker = ticks.symbol`.
    - Applies an event-time range of ±10 minutes.
    - Applies a 10-minute watermark to both streams.

    **3. Gold sliding-window aggregation**
    - Groups joined events into 5-minute windows.
    - Slides the window every 1 minute.
    - Uses a 10-minute watermark and append output mode.
    - Calculates joined count, average sentiment, average price and
      positive / negative / neutral news counts.

    **Why the window table may lag:** append-mode event-time aggregations
    emit a window after the watermark passes its end. The mock producer
    advances event time by one minute per cycle so the demo can show
    completed windows without waiting ten real minutes.
    """)
st.caption("Auto-refreshes every 3 seconds. Stop the producer and Spark terminals with Ctrl+C.")
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=3000, key="gold_refresh")
