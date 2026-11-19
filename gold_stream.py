"""Gold: real Spark Structured Streaming stream-stream join + sliding window."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

INPUT_NEWS = str(ROOT / "input" / "silver_news")
INPUT_TICKS = str(ROOT / "input" / "silver_ticks")
JOINED_OUT = str(ROOT / "output" / "gold_joined")
WINDOW_OUT = str(ROOT / "output" / "gold_windowed")
CHECKPOINT_JOIN = str(ROOT / "checkpoints" / "gold_join")
CHECKPOINT_WINDOW = str(ROOT / "checkpoints" / "gold_window")

news_schema = StructType([
    StructField("article_id", StringType(), False),
    StructField("ticker", StringType(), False),
    StructField("headline", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("event_time", StringType(), False),
    StructField("source", StringType(), True),
    StructField("is_mock", BooleanType(), True),
])

tick_schema = StructType([
    StructField("tick_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("current_price", DoubleType(), False),
    StructField("change", DoubleType(), True),
    StructField("change_percent", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("source", StringType(), True),
    StructField("market_timestamp", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("is_mock", BooleanType(), True),
])

def main() -> None:
    for path in [INPUT_NEWS, INPUT_TICKS, JOINED_OUT, WINDOW_OUT,
                 CHECKPOINT_JOIN, CHECKPOINT_WINDOW]:
        Path(path).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("FinTel-Gold-Streaming-Demo")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.python.use.daemon", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.streaming.multipleWatermarkPolicy", "min")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    news = (
        spark.readStream.schema(news_schema)
        .option("maxFilesPerTrigger", 20)
        .json(INPUT_NEWS)
        .withColumn("news_event_time", F.to_timestamp("event_time"))
        .withWatermark("news_event_time", "10 minutes")
        .alias("n")
    )

    ticks = (
        spark.readStream.schema(tick_schema)
        .option("maxFilesPerTrigger", 20)
        .json(INPUT_TICKS)
        .withColumn("market_event_time", F.to_timestamp("event_time"))
        .withWatermark("market_event_time", "10 minutes")
        .alias("t")
    )

    # Genuine stream-stream join with event-time range constraints.
    join_condition = (
        (F.col("n.ticker") == F.col("t.symbol"))
        & (
            F.col("t.market_event_time")
            >= F.col("n.news_event_time") - F.expr("INTERVAL 10 MINUTES")
        )
        & (
            F.col("t.market_event_time")
            <= F.col("n.news_event_time") + F.expr("INTERVAL 10 MINUTES")
        )
    )

    joined = (
        news.join(ticks, join_condition, "inner")
        .select(
            F.col("n.article_id").alias("article_id"),
            F.col("n.ticker").alias("ticker"),
            F.col("n.headline").alias("headline"),
            F.col("n.sentiment").alias("sentiment"),
            F.col("n.sentiment_score").alias("sentiment_score"),
            F.col("n.news_event_time").alias("news_event_time"),
            F.col("t.tick_id").alias("tick_id"),
            F.col("t.current_price").alias("current_price"),
            F.col("t.change_percent").alias("change_percent"),
            F.col("t.currency").alias("currency"),
            F.col("t.market_event_time").alias("market_event_time"),
            F.greatest(
                F.col("n.news_event_time"), F.col("t.market_event_time")
            ).alias("joined_event_time"),
            F.lit("STREAM_STREAM_JOIN").alias("gold_operation"),
            F.lit(True).alias("is_mock"),
        )
    )

    # First Gold output: joined news + market observations.
    join_query = (
        joined.writeStream
        .queryName("gold_news_market_stream_join")
        .format("json")
        .outputMode("append")
        .option("path", JOINED_OUT)
        .option("checkpointLocation", CHECKPOINT_JOIN)
        .trigger(processingTime="2 seconds")
        .start()
    )

    # Second Gold stage: 5-minute event-time window, sliding every minute.
    # A 10-minute watermark allows late data up to that event-time delay.
    joined_schema = StructType([
        StructField("article_id", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("news_event_time", StringType(), True),
        StructField("tick_id", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("change_percent", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("market_event_time", StringType(), True),
        StructField("joined_event_time", StringType(), True),
        StructField("gold_operation", StringType(), True),
        StructField("is_mock", BooleanType(), True),
    ])

    joined_stream = (
        spark.readStream.schema(joined_schema)
        .option("maxFilesPerTrigger", 50)
        .json(JOINED_OUT)
        .withColumn("window_event_time", F.to_timestamp("joined_event_time"))
        .withWatermark("window_event_time", "10 minutes")
    )

    windowed = (
        joined_stream
        .groupBy(
            F.window("window_event_time", "5 minutes", "1 minute"),
            F.col("ticker")
        )
        .agg(
            F.count("*").alias("joined_event_count"),
            F.avg("sentiment_score").alias("avg_sentiment_score"),
            F.avg("current_price").alias("avg_price"),
            F.avg("change_percent").alias("avg_change_percent"),
            F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0))
             .alias("positive_news_count"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0))
             .alias("negative_news_count"),
            F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0))
             .alias("neutral_news_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "ticker",
            "joined_event_count",
            "avg_sentiment_score",
            "avg_price",
            "avg_change_percent",
            "positive_news_count",
            "negative_news_count",
            "neutral_news_count",
            F.lit("5 minutes / 1 minute slide / 10 minutes watermark")
             .alias("window_config"),
            F.lit(True).alias("is_mock"),
        )
    )

    window_query = (
        windowed.writeStream
        .queryName("gold_sliding_window_aggregation")
        .format("json")
        .outputMode("append")
        .option("path", WINDOW_OUT)
        .option("checkpointLocation", CHECKPOINT_WINDOW)
        .trigger(processingTime="2 seconds")
        .start()
    )

    print("=" * 68, flush=True)
    print("FINTEL GOLD STREAMING DEMO IS RUNNING", flush=True)
    print(f"Spark version: {spark.version}", flush=True)
    print("Input: mock Silver News + mock Silver Market Ticks", flush=True)
    print("Join: ticker equality + +/- 10-minute event-time range", flush=True)
    print("Watermark: 10 minutes on both input streams", flush=True)
    print("Aggregation: 5-minute window, 1-minute slide", flush=True)
    print("Output: output/gold_joined and output/gold_windowed", flush=True)
    print("Keep this terminal open. CTRL+C to stop.", flush=True)
    print("=" * 68, flush=True)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping Gold queries...", flush=True)
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()
        print("Gold Spark session stopped.", flush=True)

if __name__ == "__main__":
    main()
