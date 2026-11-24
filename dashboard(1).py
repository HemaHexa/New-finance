"""
FinTel - Real-Time Financial News & Market Dashboard
Single-page Streamlit dashboard for Gold V4 PostgreSQL tables.

Expected project-root .env:
FINTEL_PG_HOST=localhost
FINTEL_PG_PORT=5432
FINTEL_PG_DB=Fintel_new
FINTEL_PG_USER=postgres
FINTEL_PG_PASSWORD=your_password
FINTEL_PG_SCHEMA=fintel_gold

Run from the project root:
    streamlit run dashboard.py
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

try:
    import psycopg
except ImportError:
    psycopg = None


# ---------------------------------------------------------------------
# Page setup and styling
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="FinTel | Market Intelligence",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

PG_HOST = os.getenv("FINTEL_PG_HOST", "localhost")
PG_PORT = int(os.getenv("FINTEL_PG_PORT", "5432"))
PG_DB = os.getenv("FINTEL_PG_DB", "Fintel_new")
PG_USER = os.getenv("FINTEL_PG_USER", "postgres")
PG_PASSWORD = os.getenv("FINTEL_PG_PASSWORD", "")
PG_SCHEMA = os.getenv("FINTEL_PG_SCHEMA", "fintel_gold")

# Schema is an identifier, not a query value. Restrict it to safe characters.
if not PG_SCHEMA.replace("_", "").isalnum():
    raise ValueError("FINTEL_PG_SCHEMA contains unsupported characters.")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@500;600;700;800&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .stApp { background: #f4f7fb; color: #17243a; }
    [data-testid="stHeader"] { background: rgba(244,247,251,0.92); }
    .block-container { padding-top: 1.25rem; padding-bottom: 2rem; max-width: 1600px; }
    .ft-header {
        background: linear-gradient(115deg, #102b50 0%, #174d78 65%, #1d6b8c 100%);
        padding: 22px 26px; border-radius: 18px; color: white; margin-bottom: 18px;
        box-shadow: 0 8px 24px rgba(16,43,80,.13);
    }
    .ft-brand { font-family: Manrope, sans-serif; font-size: 30px; font-weight: 800; letter-spacing: -.8px; }
    .ft-sub { color: #d8e8f5; font-size: 13px; margin-top: 3px; }
    .ft-live { color: #d8ffe9; font-size: 12px; font-weight: 700; text-align: right; }
    .ft-updated { color: #d8e8f5; font-size: 11px; text-align: right; margin-top: 5px; }
    .kpi {
        background: white; border: 1px solid #e5ebf3; border-radius: 15px;
        padding: 15px 16px; min-height: 112px; box-shadow: 0 3px 12px rgba(23,36,58,.035);
    }
    .kpi-label { color: #718096; font-size: 12px; font-weight: 600; }
    .kpi-value { color: #17243a; font-family: Manrope,sans-serif; font-size: 27px; font-weight: 800; margin-top: 8px; }
    .kpi-note { color: #8794a7; font-size: 11px; margin-top: 2px; }
    .panel {
        background: white; border: 1px solid #e5ebf3; border-radius: 16px;
        padding: 17px 18px; box-shadow: 0 3px 12px rgba(23,36,58,.035);
        margin-bottom: 14px;
    }
    .panel-title { color: #17243a; font-family: Manrope,sans-serif; font-size: 16px; font-weight: 800; margin-bottom: 3px; }
    .panel-sub { color: #8491a4; font-size: 11px; margin-bottom: 12px; }
    .news-item { border-bottom: 1px solid #edf1f6; padding: 10px 0; }
    .news-item:last-child { border-bottom: none; }
    .news-head { color: #1b2b43; font-weight: 700; font-size: 13px; line-height: 1.45; }
    .news-meta { color: #8a97a8; font-size: 10px; margin-top: 4px; }
    .pill-pos { background: #e8f7ef; color: #16834c; border-radius: 20px; padding: 3px 8px; font-size: 10px; font-weight: 700; }
    .pill-neg { background: #fff0ef; color: #c44949; border-radius: 20px; padding: 3px 8px; font-size: 10px; font-weight: 700; }
    .pill-neu { background: #eef2f7; color: #66758a; border-radius: 20px; padding: 3px 8px; font-size: 10px; font-weight: 700; }
    div[data-testid="stMetric"] { background: transparent; }
    div[data-testid="stDataFrame"] { border: 1px solid #edf1f6; border-radius: 10px; }
    .small-muted { color: #8a97a8; font-size: 11px; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------
def get_connection():
    if psycopg is None:
        raise RuntimeError(
            "psycopg is not installed. Install it with: pip install 'psycopg[binary]>=3.2,<4'"
        )
    if not PG_PASSWORD:
        raise RuntimeError(
            "FINTEL_PG_PASSWORD is missing from the project-root .env file."
        )
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=8,
        options="-c timezone=Asia/Kolkata",
    )


@st.cache_data(ttl=15, show_spinner=False)
def read_sql(query: str, params: tuple = ()) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def table(name: str) -> str:
    return f'"{PG_SCHEMA}"."{name}"'


def load_dashboard_data():
    news = read_sql(
        f"""
        SELECT article_id, symbol, headline, source, published_at, sentiment,
               sentiment_score, positive_score, negative_score, neutral_score, processed_at
        FROM {table("gold_news_sentiment")}
        ORDER BY published_at DESC NULLS LAST
        LIMIT 1500
        """
    )
    stock = read_sql(
        f"""
        SELECT symbol, window_start, window_end, price_first, price_last,
               price_high, price_low, price_change_pct, price_range_pct,
               day_change_pct, tick_count, computed_at
        FROM {table("gold_stock_metrics")}
        ORDER BY window_start DESC
        LIMIT 5000
        """
    )
    joined = read_sql(
        f"""
        SELECT symbol, window_start, window_end, article_count, avg_sentiment,
               min_sentiment, latest_headline, latest_sentiment_label,
               latest_news_time, price_first, price_last, price_high, price_low,
               price_change_pct, price_range_pct, day_change_pct, tick_count,
               alert_flag, alert_reason, computed_at
        FROM {table("gold_sentiment_price")}
        ORDER BY window_start DESC
        LIMIT 5000
        """
    )
    alerts = read_sql(
        f"""
        SELECT symbol, window_start, alert_type, alert_message,
               sentiment_score, price_change_pct, created_at
        FROM {table("gold_alerts")}
        ORDER BY created_at DESC
        LIMIT 100
        """
    )
    return news, stock, joined, alerts


# ---------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------
def safe_float(value, digits=2, suffix=""):
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value):,.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return "—"


def sentiment_label(value):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return "Neutral"
    if x > 0.15:
        return "Positive"
    if x < -0.15:
        return "Negative"
    return "Neutral"


def sentiment_pill(label):
    label = str(label or "Neutral").strip().title()
    css = "pill-pos" if label == "Positive" else "pill-neg" if label == "Negative" else "pill-neu"
    return f'<span class="{css}">{label}</span>'


def show_empty(message: str):
    st.markdown(f'<div class="small-muted">{message}</div>', unsafe_allow_html=True)


def make_price_sentiment_chart(df: pd.DataFrame, symbol: str):
    """Price line with sentiment-coloured news markers from joined Gold windows."""
    if df.empty:
        show_empty("No joined stock/news windows are available for this selection yet.")
        return

    plot = df.sort_values("window_start").copy()
    plot["window_start"] = pd.to_datetime(plot["window_start"], errors="coerce")
    plot = plot.dropna(subset=["window_start", "price_last"])
    if plot.empty:
        show_empty("No valid price windows are available.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=plot["window_start"], y=plot["price_last"],
        name="Stock price (₹)", mode="lines+markers",
        line=dict(color="#2678bd", width=2.8), marker=dict(size=5, color="#2678bd"),
        hovertemplate="Time: %{x}<br>Price: ₹%{y:,.2f}<extra></extra>",
    ))

    # A marker is shown only where Gold has a joined news window.
    if "article_count" in plot.columns:
        news_rows = plot[pd.to_numeric(plot["article_count"], errors="coerce").fillna(0) > 0].copy()
    else:
        news_rows = plot.iloc[0:0].copy()

    if not news_rows.empty:
        labels = news_rows.get("latest_sentiment_label", pd.Series(index=news_rows.index, dtype=object)).fillna("Neutral").astype(str).str.title()
        marker_colors = labels.map({"Positive": "#20a36b", "Negative": "#df6868", "Neutral": "#8794a7"}).fillna("#8794a7")
        headlines = news_rows.get("latest_headline", pd.Series(index=news_rows.index, dtype=object)).fillna("News headline unavailable")
        counts = pd.to_numeric(news_rows["article_count"], errors="coerce").fillna(0).astype(int)
        fig.add_trace(go.Scatter(
            x=news_rows["window_start"], y=news_rows["price_last"],
            name="News event", mode="markers",
            marker=dict(color=marker_colors, size=11, line=dict(color="white", width=1.5)),
            customdata=list(zip(headlines, labels, counts)),
            hovertemplate=("<b>%{customdata[0]}</b><br>" +
                           "Sentiment: %{customdata[1]}<br>" +
                           "Articles in window: %{customdata[2]}<br>" +
                           "Price: ₹%{y:,.2f}<extra></extra>"),
        ))

    fig.update_layout(
        height=330, margin=dict(l=8, r=8, t=12, b=8),
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="DM Sans", color="#617087", size=11),
        legend=dict(orientation="h", y=1.12, x=0), hovermode="x unified",
        xaxis=dict(title="Time", showgrid=True, gridcolor="#edf1f6"),
        yaxis=dict(title="Stock price (₹)", showgrid=True, gridcolor="#edf1f6", tickformat=",.2f"),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # Sentiment is displayed as its own trend, matching the supplied reference.
    sentiment_plot = plot.dropna(subset=["avg_sentiment"]).copy()
    if not sentiment_plot.empty:
        sentiment_fig = go.Figure()
        sentiment_fig.add_trace(go.Bar(
            x=sentiment_plot["window_start"], y=sentiment_plot["avg_sentiment"],
            name="Average news sentiment",
            marker_color=["#20a36b" if value >= 0 else "#df6868" for value in sentiment_plot["avg_sentiment"]],
            hovertemplate="Time: %{x}<br>Average sentiment: %{y:.3f}<extra></extra>",
        ))
        sentiment_fig.add_hline(y=0, line_color="#aab5c4", line_dash="dot")
        sentiment_fig.update_layout(
            height=190, margin=dict(l=8, r=8, t=4, b=8),
            paper_bgcolor="white", plot_bgcolor="white",
            font=dict(family="DM Sans", color="#617087", size=10),
            showlegend=False,
            xaxis=dict(title="Time", showgrid=False),
            yaxis=dict(title="Sentiment", range=[-1, 1], showgrid=True, gridcolor="#edf1f6"),
        )
        st.plotly_chart(sentiment_fig, use_container_width=True, config={"displayModeBar": False})


# ---------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------
left, right = st.columns([3.2, 1.2])
with left:
    st.markdown(
        """
        <div class="ft-header">
          <div class="ft-brand">↗ FinTel</div>
          <div class="ft-sub">REAL-TIME FINANCIAL NEWS &amp; MARKET INTELLIGENCE</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with right:
    st.markdown(
        """
        <div class="ft-header" style="height:88px;display:flex;flex-direction:column;justify-content:center;">
          <div class="ft-live">● POSTGRESQL CONNECTED</div>
          <div class="ft-updated">Gold V4 · 5-min windows</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

refresh_col, info_col = st.columns([1, 5])
with refresh_col:
    if st.button("↻ Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
with info_col:
    st.markdown(
        '<div class="small-muted" style="padding-top:9px;">Dashboard reads Gold V4 serving tables in PostgreSQL. Values are based on stored pipeline output.</div>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------
try:
    with st.spinner("Loading Gold V4 analytics from PostgreSQL..."):
        news_df, stock_df, joined_df, alerts_df = load_dashboard_data()
except Exception as exc:
    st.error(f"Unable to load dashboard data: {exc}")
    st.info(
        "Check that PostgreSQL is running, the project-root .env contains the "
        "FINTEL_PG_* settings, and the five Gold V4 tables exist in the configured schema."
    )
    st.stop()

# Normalize timestamps and numeric fields
for frame, cols in [
    (news_df, ["published_at", "processed_at"]),
    (stock_df, ["window_start", "window_end", "computed_at"]),
    (joined_df, ["window_start", "window_end", "latest_news_time", "computed_at"]),
    (alerts_df, ["window_start", "created_at"]),
]:
    for col in cols:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce")

for frame, cols in [
    (news_df, ["sentiment_score", "positive_score", "negative_score", "neutral_score"]),
    (stock_df, ["price_first", "price_last", "price_high", "price_low", "price_change_pct", "day_change_pct"]),
    (joined_df, ["avg_sentiment", "price_last", "price_change_pct"]),
]:
    for col in cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

# Company selector
symbols = sorted(
    set(news_df.get("symbol", pd.Series(dtype=str)).dropna().astype(str).str.upper())
    | set(stock_df.get("symbol", pd.Series(dtype=str)).dropna().astype(str).str.upper())
    | set(joined_df.get("symbol", pd.Series(dtype=str)).dropna().astype(str).str.upper())
)
company_names = {
    "RELIANCE": "Reliance Industries", "TCS": "TCS", "INFY": "Infosys",
    "HDFCBANK": "HDFC Bank", "ICICIBANK": "ICICI Bank", "TATASTEEL": "Tata Steel",
    "SBIN": "State Bank of India", "ITC": "ITC", "SUNPHARMA": "Sun Pharma",
    "BHARTIARTL": "Bharti Airtel", "ADANIENT": "Adani Enterprises",
    "COALINDIA": "Coal India", "BAJFINANCE": "Bajaj Finance",
    "HINDALCO": "Hindalco Industries", "ONGC": "ONGC",
}
selector_options = ["All companies"] + symbols
selected = st.selectbox(
    "Select company",
    selector_options,
    format_func=lambda x: "All companies" if x == "All companies" else f"{company_names.get(x, x)} ({x})",
)
selected_news = news_df.copy() if selected == "All companies" else news_df[news_df["symbol"].str.upper() == selected]
selected_stock = stock_df.copy() if selected == "All companies" else stock_df[stock_df["symbol"].str.upper() == selected]
selected_joined = joined_df.copy() if selected == "All companies" else joined_df[joined_df["symbol"].str.upper() == selected]
selected_alerts = alerts_df.copy() if selected == "All companies" else alerts_df[alerts_df["symbol"].str.upper() == selected]

# ---------------------------------------------------------------------
# KPI strip - counts are derived from Gold V4 PostgreSQL data
# ---------------------------------------------------------------------
total_news = int(len(selected_news))
stock_tickers = int(selected_stock["symbol"].nunique()) if not selected_stock.empty else 0
if not selected_news.empty and "sentiment" in selected_news:
    labels = selected_news["sentiment"].fillna("").astype(str).str.lower()
    positive_n = int(labels.str.contains("positive").sum())
    negative_n = int(labels.str.contains("negative").sum())
    neutral_n = int(labels.str.contains("neutral").sum())
else:
    positive_n = negative_n = neutral_n = 0
avg_sent = selected_joined["avg_sentiment"].mean() if not selected_joined.empty else float("nan")
latest_news_count = int(selected_news["article_id"].nunique()) if not selected_news.empty else 0

kpis = [
    ("TOTAL NEWS", f"{total_news:,}", "Gold news articles"),
    ("STOCK TICKERS", f"{stock_tickers:,}", "Companies with stock windows"),
    ("POSITIVE NEWS", f"{positive_n:,}", "Article sentiment labels"),
    ("NEGATIVE NEWS", f"{negative_n:,}", "Article sentiment labels"),
    ("AVG SENTIMENT", safe_float(avg_sent, 3), "Joined 5-minute windows"),
    ("LATEST NEWS", f"{latest_news_count:,}", "Available article records"),
]
cols = st.columns(6)
for col, (label, value, note) in zip(cols, kpis):
    with col:
        st.markdown(
            f'<div class="kpi"><div class="kpi-label">{label}</div>'
            f'<div class="kpi-value">{value}</div><div class="kpi-note">{note}</div></div>',
            unsafe_allow_html=True,
        )

st.write("")


# ---------------------------------------------------------------------
# Main row: Latest news + stock price table
# ---------------------------------------------------------------------
news_col, stock_col = st.columns([1.05, 1.35], gap="large")
with news_col:
    st.markdown('<div class="panel"><div class="panel-title">📰 Latest News</div>'
                '<div class="panel-sub">Latest article records from Gold V4</div>', unsafe_allow_html=True)
    latest = selected_news.sort_values("published_at", ascending=False, na_position="last").head(8)
    if latest.empty:
        show_empty("No news articles are available for this selection.")
    else:
        for _, row in latest.iterrows():
            headline = str(row.get("headline") or "Headline unavailable")
            source = str(row.get("source") or "Unknown source")
            symbol = str(row.get("symbol") or "")
            label = str(row.get("sentiment") or sentiment_label(row.get("sentiment_score")))
            published = row.get("published_at")
            published_text = published.strftime("%d %b %Y · %I:%M %p") if pd.notna(published) else "Time unavailable"
            st.markdown(
                f'<div class="news-item"><div class="news-head">{headline}</div>'
                f'<div class="news-meta">{symbol} &nbsp;·&nbsp; {source} &nbsp;·&nbsp; {published_text}'
                f' &nbsp;&nbsp;{sentiment_pill(label)}</div></div>',
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)

with stock_col:
    st.markdown('<div class="panel"><div class="panel-title">📊 Stock Price Tracker</div>'
                '<div class="panel-sub">Latest completed stock windows by company</div>', unsafe_allow_html=True)
    if selected_stock.empty:
        show_empty("No stock metrics are available.")
    else:
        latest_stock = (
            selected_stock.sort_values("window_start")
            .groupby("symbol", as_index=False).tail(1)
            .sort_values("symbol")
        )
        display = latest_stock[[
            c for c in ["symbol", "price_last", "price_change_pct", "day_change_pct", "tick_count", "window_start"]
            if c in latest_stock.columns
        ]].copy()
        rename = {
            "symbol": "Ticker", "price_last": "Last price (₹)",
            "price_change_pct": "Window change %", "day_change_pct": "Day change %",
            "tick_count": "Ticks", "window_start": "Window",
        }
        display = display.rename(columns=rename)
        st.dataframe(
            display, use_container_width=True, hide_index=True,
            column_config={
                "Last price (₹)": st.column_config.NumberColumn(format="₹ %.2f"),
                "Window change %": st.column_config.NumberColumn(format="%.3f%%"),
                "Day change %": st.column_config.NumberColumn(format="%.3f%%"),
                "Ticks": st.column_config.NumberColumn(format="%d"),
                "Window": st.column_config.DatetimeColumn(format="DD MMM, HH:mm"),
            },
        )
    st.markdown("</div>", unsafe_allow_html=True)


# ---------------------------------------------------------------------
# Main comparison chart
# ---------------------------------------------------------------------
st.markdown('<div class="panel"><div class="panel-title">📈 News vs Stock Price</div>'
            '<div class="panel-sub">5-minute Gold windows · 1-minute slide · 10-minute watermark</div>',
            unsafe_allow_html=True)
if selected == "All companies":
    chart_symbol = st.selectbox(
        "Choose a company for the comparison chart",
        symbols if symbols else ["No company data"],
        format_func=lambda x: f"{company_names.get(x, x)} ({x})",
        key="chart_company",
    )
else:
    chart_symbol = selected
chart_data = joined_df[joined_df["symbol"].str.upper() == chart_symbol] if chart_symbol in symbols else pd.DataFrame()
make_price_sentiment_chart(chart_data, chart_symbol)
st.markdown("</div>", unsafe_allow_html=True)

# Sentiment summary and trending stories
sent_col, trend_col = st.columns([1, 1], gap="large")
with sent_col:
    st.markdown('<div class="panel"><div class="panel-title">🧭 Sentiment Overview</div>'
                '<div class="panel-sub">Article-level FinBERT classifications</div>', unsafe_allow_html=True)
    sentiment_counts = pd.DataFrame({
        "Sentiment": ["Positive", "Negative", "Neutral"],
        "Articles": [positive_n, negative_n, neutral_n],
    })
    if int(sentiment_counts["Articles"].sum()) > 0:
        fig = go.Figure(data=[go.Pie(
            labels=sentiment_counts["Sentiment"], values=sentiment_counts["Articles"],
            hole=0.68, sort=False,
            marker=dict(colors=["#20a36b", "#df6868", "#7d91ad"]),
            textinfo="percent", hovertemplate="%{label}: %{value}<extra></extra>",
        )])
        fig.update_layout(
            height=245, margin=dict(l=0, r=0, t=5, b=5),
            paper_bgcolor="white", showlegend=True,
            legend=dict(orientation="h", y=-0.08, x=0.05),
            annotations=[dict(
                text=f"{int(sentiment_counts['Articles'].sum()):,}<br><span style='font-size:10px'>Articles</span>",
                x=0.5, y=0.5, showarrow=False, font=dict(size=20, color="#17243a")
            )],
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        show_empty("No article-level sentiment data is available.")
    st.markdown("</div>", unsafe_allow_html=True)

with trend_col:
    st.markdown('<div class="panel"><div class="panel-title">🔥 Trending News · Top 5</div>'
                '<div class="panel-sub">Ranked by absolute sentiment score, not by market impact</div>',
                unsafe_allow_html=True)
    if selected_news.empty:
        show_empty("No trending articles are available.")
    else:
        trending = selected_news.copy()
        trending["sentiment_score"] = pd.to_numeric(trending["sentiment_score"], errors="coerce")
        trending["strength"] = trending["sentiment_score"].abs()
        trending = trending.sort_values("strength", ascending=False).head(5)
        for rank, (_, row) in enumerate(trending.iterrows(), start=1):
            label = str(row.get("sentiment") or sentiment_label(row.get("sentiment_score")))
            st.markdown(
                f'<div class="news-item"><div class="news-head">{rank}. {row.get("headline") or "Headline unavailable"}</div>'
                f'<div class="news-meta">{row.get("symbol", "")} · {row.get("source") or "Unknown source"}'
                f' · Score: {safe_float(row.get("sentiment_score"), 3)}'
                f' &nbsp;&nbsp;{sentiment_pill(label)}</div></div>',
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)


# ---------------------------------------------------------------------
# Alerts and footer
# ---------------------------------------------------------------------
with st.expander(f"⚠️ Gold alerts ({len(selected_alerts)})", expanded=False):
    if selected_alerts.empty:
        show_empty("No alerts have been recorded.")
    else:
        alert_view = selected_alerts.rename(columns={
            "symbol": "Ticker", "window_start": "Window", "alert_type": "Alert type",
            "alert_message": "Reason", "sentiment_score": "Sentiment",
            "price_change_pct": "Price change %", "created_at": "Created at",
        })
        st.dataframe(alert_view, use_container_width=True, hide_index=True)

st.markdown(
    f'<div class="small-muted" style="text-align:center;padding:18px 0 4px;">'
    f'FinTel · Gold V4 PostgreSQL Dashboard · {PG_DB}.{PG_SCHEMA} · '
    f'Refreshed {datetime.now().astimezone().strftime("%d %b %Y, %I:%M:%S %p")}</div>',
    unsafe_allow_html=True,
)
