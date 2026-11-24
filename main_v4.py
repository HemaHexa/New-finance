"""FinTel Gold v4 launcher. Does not replace the existing main.py."""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

if os.name == "nt" and Path(r"C:\hadoop").exists():
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from common import audit
from config import settings
from config.database import test_connection
from gold.gold_job_v4 import start_gold
from gold.news_normalizer import NewsSilverNormalizer


def main():
    print("\n========== FINTEL GOLD V4 STARTING ==========\n")
    if not settings.PG_ENABLED:
        raise RuntimeError("Set FINTEL_PG_ENABLED=1 in the project-root .env")
    if not test_connection():
        raise RuntimeError("PostgreSQL Gold schema validation failed")

    builder = (
        SparkSession.builder
        .appName("FinTel-Gold-v4-PostgreSQL")
        .master(settings.SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.network.timeout", "120s")
        .config("spark.executor.heartbeatInterval", "30s")
        .config("spark.python.worker.reuse", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    normalizer = NewsSilverNormalizer()
    queries = []
    run_id = audit.new_run_id()
    status = "STOPPED"
    error_text = None

    try:
        # Convert the existing Silver backlog once, then watch for new batches.
        normalizer.scan_once()
        normalizer.start()
        audit.job_start(run_id, "gold_job_v4")

        queries = start_gold(spark, run_id)
        print("[GOLD V4] All three streaming queries are active.")
        print("[GOLD V4] PostgreSQL is the only Gold serving store.")
        print("[GOLD V4] Press Ctrl+C to stop safely.")
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[GOLD V4] Shutdown requested by user.")
    except Exception as exc:
        status = "FAILED"
        error_text = str(exc)
        audit.job_end(run_id, "gold_job_v4", status="FAILED", last_error=error_text)
        raise
    finally:
        for query in queries:
            try:
                if query.isActive:
                    query.stop()
            except Exception as stop_error:
                print(f"[GOLD V4] Query stop warning: {stop_error}")
        normalizer.stop()
        if status != "FAILED":
            try:
                audit.job_end(run_id, "gold_job_v4", status=status, last_error=error_text)
            except Exception as audit_error:
                print(f"[GOLD V4] Audit shutdown warning: {audit_error}")
        spark.stop()
        print("[GOLD V4] Pipeline stopped.")


if __name__ == "__main__":
    main()
