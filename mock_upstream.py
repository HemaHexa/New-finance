"""Generate mock Silver news and ticker events as JSONL files for Gold demo.

The payloads follow the uploaded projects' Silver conventions:
- News: article_id, ticker, sentiment, sentiment_score, event_time
- Ticks: symbol, current_price, change, change_percent, currency, source,
         market_timestamp, event_time

No Kafka, RSS, FinBERT, or external market API is required.
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NEWS_DIR = ROOT / "input" / "silver_news"
TICKS_DIR = ROOT / "input" / "silver_ticks"

STOCKS = {
    "RELIANCE": ("Reliance Industries", 1450.0),
    "TCS": ("Tata Consultancy Services", 3820.0),
    "INFY": ("Infosys", 1580.0),
    "HDFCBANK": ("HDFC Bank", 1740.0),
    "ICICIBANK": ("ICICI Bank", 1260.0),
    "TATASTEEL": ("Tata Steel", 165.0),
    "SBIN": ("State Bank of India", 820.0),
    "ITC": ("ITC", 410.0),
    "SUNPHARMA": ("Sun Pharmaceutical", 1720.0),
    "BHARTIARTL": ("Bharti Airtel", 1920.0),
    "ADANIENT": ("Adani Enterprises", 2450.0),
    "COALINDIA": ("Coal India", 390.0),
    "BAJFINANCE": ("Bajaj Finance", 920.0),
}

HEADLINES = {
    "positive": [
        "{name} reports stronger quarterly performance",
        "Analysts raise outlook for {name} after robust demand",
        "{name} expands investment plans amid positive growth signals",
    ],
    "negative": [
        "{name} faces margin pressure amid higher costs",
        "Investors reassess outlook for {name} after weaker guidance",
        "{name} shares slip as market sentiment turns cautious",
    ],
    "neutral": [
        "{name} schedules board meeting to review business updates",
        "{name} announces routine operational update",
        "Market participants monitor developments at {name}",
    ],
}

def write_atomic(directory: Path, prefix: str, rows: list[dict]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    temp = directory / f".{prefix}_{token}.tmp"
    final = directory / f"{prefix}_{token}.json"
    with temp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    temp.replace(final)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Wall-clock seconds between event-time batches")
    parser.add_argument("--cycles", type=int, default=0,
                        help="0 means run continuously")
    args = parser.parse_args()

    random.seed(17)
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    TICKS_DIR.mkdir(parents=True, exist_ok=True)

    # Start sufficiently behind current event time so the 10-minute
    # event-time watermark can close windows during a short demo.
    event_clock = datetime.now(timezone.utc) - timedelta(minutes=30)
    cycle = 0
    print("Mock Silver upstream is running. CTRL+C to stop.", flush=True)
    print(f"Tickers: {len(STOCKS)} | Event-time step: 1 minute | "
          f"Wall-clock interval: {args.interval}s", flush=True)

    try:
        while args.cycles == 0 or cycle < args.cycles:
            news_rows, tick_rows = [], []
            for symbol, (name, base_price) in STOCKS.items():
                sentiment = random.choices(
                    ["positive", "negative", "neutral"],
                    weights=[0.48, 0.27, 0.25], k=1
                )[0]
                score = {
                    "positive": random.uniform(0.25, 0.92),
                    "negative": random.uniform(-0.92, -0.25),
                    "neutral": random.uniform(-0.12, 0.12),
                }[sentiment]
                headline = random.choice(HEADLINES[sentiment]).format(name=name)
                ts = event_clock.isoformat(timespec="seconds").replace("+00:00", "Z")

                news_rows.append({
                    "article_id": f"DEMO-{cycle:05d}-{symbol}",
                    "ticker": symbol,
                    "headline": headline,
                    "sentiment": sentiment,
                    "sentiment_score": round(score, 4),
                    "event_time": ts,
                    "source": "DEMO-SILVER-NEWS",
                    "is_mock": True,
                })

                drift = random.uniform(-0.006, 0.006)
                price = round(base_price * (1 + drift + (cycle % 7 - 3) * 0.0004), 2)
                previous = base_price
                tick_rows.append({
                    "tick_id": f"TICK-{cycle:05d}-{symbol}",
                    "symbol": symbol,
                    "current_price": price,
                    "change": round(price - previous, 2),
                    "change_percent": round((price / previous - 1) * 100, 4),
                    "currency": "INR",
                    "source": "DEMO-MARKET-TICKER",
                    "market_timestamp": ts,
                    "event_time": ts,
                    "is_mock": True,
                })

            write_atomic(NEWS_DIR, f"silver_news_{cycle:06d}", news_rows)
            write_atomic(TICKS_DIR, f"silver_ticks_{cycle:06d}", tick_rows)
            print(f"[MOCK SILVER] cycle={cycle} | news={len(news_rows)} | "
                  f"ticks={len(tick_rows)} | event_time={event_clock.isoformat()}",
                  flush=True)
            cycle += 1
            event_clock += timedelta(minutes=1)
            time.sleep(max(0.1, args.interval))
    except KeyboardInterrupt:
        print("\nMock upstream stopped.", flush=True)

if __name__ == "__main__":
    main()
