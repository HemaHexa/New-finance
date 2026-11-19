# FinTel Gold Streaming Demo

A minimal, standalone demonstration built from the uploaded FinTel news, Market Ticker,
and Silver-news projects. It isolates Gold so it can be demonstrated without Kafka,
RSS polling, FinBERT model downloads, PostgreSQL, or the original failing Silver runtime.

## What is real and what is simulated?

- **Simulated upstream:** a local mock producer writes Silver-shaped news and market-tick
  JSON events. News sentiment values are dummy values, not FinBERT predictions.
- **Real Gold processing:** Apache Spark Structured Streaming reads two independent
  streaming sources, performs a genuine stream-stream join with event-time constraints,
  applies watermarks, and computes a sliding-window aggregation.
- **Dashboard:** Streamlit reads the Spark Gold output files and displays the results.

## Gold logic

1. News Silver stream: `ticker`, `sentiment`, `sentiment_score`, `event_time`.
2. Market Silver stream: `symbol`, `current_price`, `change_percent`, `event_time`.
3. Stream-stream join:
   - `news.ticker = ticks.symbol`
   - tick event time is within +/- 10 minutes of news event time
   - 10-minute watermark on both input streams
4. Gold sliding-window aggregation:
   - window duration: 5 minutes
   - slide duration: 1 minute
   - watermark: 10 minutes
   - metrics: joined event count, average sentiment score, average price,
     average percentage change, positive/negative/neutral news counts.
5. Outputs:
   - `output/gold_joined/`
   - `output/gold_windowed/`

The mock producer advances event time by one minute per cycle while sleeping one
wall-clock second by default. This lets append-mode windows close quickly for a demo.
The event timestamps are simulated; they are not real market timestamps.

## Prerequisites

- Windows 10/11
- Python 3.12
- Java 17 or newer (`java -version`)
- Internet access once, to install Python packages
- No Kafka, ZooKeeper, Delta Lake, Hadoop `winutils`, API key, or FinBERT model required.

This demo pins PySpark 4.1.4 to match the PySpark version used by the uploaded
Market Ticker project. It does not use Delta Lake because Gold's demonstration
outputs are JSON files and no Delta transaction log is required.

## Run on Windows

1. Extract the ZIP to a short local path, for example `C:\work\fintel-gold-demo`.
2. Double-click `setup.bat` once. It creates an isolated `.goldvenv` and installs
   `requirements.txt` (PySpark 4.1.4, Streamlit, pandas).
3. Double-click `run_demo.bat`.
4. Three terminal windows open: mock Silver upstream, Spark Gold, and Streamlit.
5. Open `http://localhost:8501`.

Keep all three windows open. Stop each with `Ctrl+C`.

## If `py -3.12` is unavailable

Edit `setup.bat` to use the Python version installed on the machine, but keep
PySpark 4.1.4. Do not reuse the old virtual environment containing PySpark 3.5.4
with Python 3.12.2.

## Important: restart / clean demo

Spark Structured Streaming checkpoints preserve offsets and state. To replay from
the beginning, stop all three windows, then delete only these demo folders:

- `checkpoints\gold_join`
- `checkpoints\gold_window`
- `output\gold_joined`
- `output\gold_windowed`
- `input\silver_news`
- `input\silver_ticks`

Then run `run_demo.bat` again. Do not delete folders while Spark is running.

## Troubleshooting

- **Java not found:** install/use Java 17+ and ensure `java` is on PATH.
- **Port 8501 occupied:** change `--server.port 8501` in `run_demo.bat`.
- **No window rows yet:** keep the mock producer running. Append-mode window results
  are emitted only after the 10-minute event-time watermark passes the window end.
- **Spark startup issue:** see the full stack trace in the `FinTel Gold Spark` terminal.
- **Python mismatch:** this package uses its own `.goldvenv`; do not run it with the
  project's previous `.venv`.

## Source alignment

The field names are adapted from the uploaded code:
- News project: `article_id`, `published_at`, `headline`, `source`, `ingested_at`.
- Silver News project: article-to-ticker mapping and sentiment fields
  (`sentiment`, `positive_score`, `negative_score`, `neutral_score`,
  `sentiment_score`).
- Market Ticker project: `symbol`, `current_price`, `change`, `change_percent`,
  `currency`, `source`, `timestamp`, `fetched_at`, `ingested_at`.

The demo adds a normalized `event_time` field to each Silver-shaped event so both
streams can use Spark event-time processing consistently.
