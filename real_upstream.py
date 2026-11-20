import json
import time
import hashlib
from pathlib import Path
from datetime import datetime
import pyarrow.parquet as pq
 
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
 
ROOT = Path(__file__).resolve().parent
 
STOCK_PATH = Path(r"C:\FinTel\Market_ticker\silver\data\stock_silver")
NEWS_PATH = Path(r"C:\FinTel\silver_news\data\silver\news_by_ticker")
 
NEWS_OUT = ROOT / "input" / "silver_news"
TICKS_OUT = ROOT / "input" / "silver_ticks"
STATE_DIR = ROOT / "checkpoints" / "real_upstream"
STATE_FILE = STATE_DIR / "processed.json"
 
POLL_SECONDS = 30
 
for folder in [NEWS_OUT, TICKS_OUT, STATE_DIR]:
    folder.mkdir(parents=True, exist_ok=True)
 
def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"tick_ids": [], "news_ids": [], "news_files": []}
 
def save_state(state):
    temp = STATE_FILE.with_suffix(".tmp")
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    temp.replace(STATE_FILE)
 
def make_id(value):
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()
 
def write_event(folder, key, record):
    filename = make_id(key) + ".json"
    target = folder / filename
 
    if target.exists():
        return
 
    temp = folder / (filename + ".tmp")
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(record, f, default=str)
    temp.replace(target)
 
def parse_event_time(value):
    if value is None:
        return None
 
    value = str(value).strip()
    if not value:
        return None
 
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.isoformat()
    except ValueError:
        try:
            from dateutil.parser import parse
            return parse(value).isoformat()
        except Exception:
            return None
 
builder = (
    SparkSession.builder
    .appName("FinTel_Real_Silver_Adapter")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
 
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
 
print("\nFinTel REAL Silver adapter started")
print("Stock source:", STOCK_PATH)
print("News source:", NEWS_PATH)
print("Polling interval:", POLL_SECONDS, "seconds")
 
state = load_state()
tick_ids = set(state.get("tick_ids", []))
news_ids = set(state.get("news_ids", []))
news_files = set(state.get("news_files", []))
 
try:
    while True:
        new_ticks = 0
        new_news = 0
 
        # Read new Stock Silver records from Delta
        try:
            stock_df = spark.read.format("delta").load(str(STOCK_PATH))
 
            for row in stock_df.collect():
                record = row.asDict(recursive=True)
 
                tick_id = record.get("tick_id")
                symbol = record.get("symbol")
                market_time = parse_event_time(record.get("market_timestamp"))
 
                unique_id = str(tick_id) if tick_id else (
                    f"{symbol}|{market_time}|{record.get('current_price')}"
                )
 
                if unique_id in tick_ids:
                    continue
 
                if not symbol or not market_time:
                    continue
 
                output = {
                    "tick_id": unique_id,
                    "symbol": str(symbol).upper(),
                    "current_price": record.get("current_price"),
                    "change": record.get("change"),
                    "change_percent": record.get("change_percent"),
                    "currency": record.get("currency"),
                    "source": record.get("source"),
                    "market_timestamp": market_time,
                    "event_time": market_time,
                    "is_mock": False
                }
 
                write_event(TICKS_OUT, unique_id, output)
                tick_ids.add(unique_id)
                new_ticks += 1
 
        except Exception as e:
            print("Stock read error:", str(e))
 
        # Read newly created Phase 3 Parquet files
        try:
            parquet_files = sorted(NEWS_PATH.rglob("*.parquet"))
 
            for file_path in parquet_files:
                file_key = str(file_path.resolve())
 
                if file_key in news_files:
                    continue
 
                try:
                    table = pq.read_table(file_path)
                    rows = table.to_pylist()
 
                    for row in rows:
                        article_id = row.get("article_id")
                        ticker = row.get("ticker")
                        headline = row.get("cleaned_headline") or row.get("headline")
                        sentiment = row.get("sentiment")
                        sentiment_score = row.get("sentiment_score")
                        published_at = row.get("published_at")
 
                        event_time = parse_event_time(published_at)
 
                        if not article_id or not ticker or not event_time:
                            continue
 
                        unique_id = f"{article_id}|{str(ticker).upper()}"
 
                        if unique_id in news_ids:
                            continue
 
                        output = {
                            "article_id": str(article_id),
                            "ticker": str(ticker).upper(),
                            "headline": headline or "",
                            "sentiment": sentiment or "neutral",
                            "sentiment_score": sentiment_score,
                            "event_time": event_time,
                            "source": row.get("source") or "",
                            "is_mock": False
                        }
 
                        write_event(NEWS_OUT, unique_id, output)
                        news_ids.add(unique_id)
                        new_news += 1
 
                    news_files.add(file_key)
 
                except Exception as e:
                    print("News file read error:", file_path.name, str(e))
 
        except Exception as e:
            print("News directory error:", str(e))
 
        state = {
            "tick_ids": sorted(tick_ids),
            "news_ids": sorted(news_ids),
            "news_files": sorted(news_files)
        }
        save_state(state)
 
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"New stock ticks: {new_ticks} | New news records: {new_news} | "
            f"Total tracked ticks: {len(tick_ids)} | "
            f"Total tracked news: {len(news_ids)}"
        )
 
        time.sleep(POLL_SECONDS)
 
except KeyboardInterrupt:
    print("\nAdapter stopped by user.")
 
finally:
    spark.stop()
