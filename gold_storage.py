"""PostgreSQL persistence for Gold v4 (psycopg 3, schema-qualified)."""

from __future__ import annotations

from functools import partial

from pyspark.sql import DataFrame


TABLE_CONFIG = {
    "gold_stock_metrics": {
        "columns": [
            "symbol", "window_start", "window_end", "price_first", "price_last",
            "price_high", "price_low", "price_change_pct", "price_range_pct",
            "day_change_pct", "tick_count", "computed_at",
        ],
        "keys": ["symbol", "window_start"],
    },
    "gold_news_sentiment": {
        "columns": [
            "article_id", "symbol", "headline", "source", "published_at", "sentiment",
            "sentiment_score", "positive_score", "negative_score", "neutral_score", "processed_at",
        ],
        "keys": ["article_id", "symbol"],
    },
    "gold_sentiment_price": {
        "columns": [
            "symbol", "window_start", "window_end", "article_count", "avg_sentiment",
            "min_sentiment", "latest_headline", "latest_sentiment_label", "latest_news_time",
            "price_first", "price_last", "price_high", "price_low", "price_change_pct",
            "price_range_pct", "day_change_pct", "tick_count", "alert_flag", "alert_reason",
            "computed_at",
        ],
        "keys": ["symbol", "window_start"],
    },
    "gold_alerts": {
        "columns": [
            "symbol", "window_start", "alert_type", "alert_message", "sentiment_score",
            "price_change_pct", "created_at",
        ],
        "keys": ["symbol", "window_start", "alert_type"],
        "id_column": "alert_id",
    },
    "gold_audit": {
        "columns": [
            "run_id", "batch_id", "job_name", "status", "input_tick_count",
            "input_news_count", "output_row_count", "rows_with_news", "late_records",
            "started_at", "completed_at", "duration_ms", "error_message",
        ],
        "keys": ["run_id", "batch_id"],
        "id_column": "audit_id",
    },
}


def _write_partition(schema_name, table_name, columns, key_columns, id_column, rows):
    """Executor-side writer. Each Spark partition uses its own PG transaction."""
    from psycopg import sql
    from config.database import get_connection

    records = list(rows)
    if not records:
        return

    with get_connection() as connection:
        with connection.cursor() as cursor:
            add_manual_id = False
            next_id = None

            if id_column:
                cursor.execute(
                    """
                    SELECT column_default, is_identity
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s AND column_name = %s
                    """,
                    (schema_name, table_name, id_column),
                )
                metadata = cursor.fetchone()
                if not metadata:
                    raise RuntimeError(f"Missing ID column {schema_name}.{table_name}.{id_column}")
                column_default, is_identity = metadata
                add_manual_id = column_default is None and is_identity == "NO"

                if add_manual_id:
                    table_ident = sql.SQL("{}.{}").format(
                        sql.Identifier(schema_name), sql.Identifier(table_name)
                    )
                    cursor.execute(
                        sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(table_ident)
                    )
                    cursor.execute(
                        sql.SQL("SELECT COALESCE(MAX({}), 0) + 1 FROM {}").format(
                            sql.Identifier(id_column), table_ident
                        )
                    )
                    next_id = int(cursor.fetchone()[0])

            insert_columns = ([id_column] if add_manual_id else []) + list(columns)
            table_ident = sql.SQL("{}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            column_sql = sql.SQL(", ").join(sql.Identifier(c) for c in insert_columns)
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in insert_columns)
            conflict_sql = sql.SQL(", ").join(sql.Identifier(c) for c in key_columns)
            update_columns = [c for c in columns if c not in key_columns]

            if update_columns:
                update_sql = sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                    for c in update_columns
                )
                conflict_action = sql.SQL("DO UPDATE SET {}").format(update_sql)
            else:
                conflict_action = sql.SQL("DO NOTHING")

            statement = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}"
            ).format(table_ident, column_sql, placeholders, conflict_sql, conflict_action)

            payload = []
            for row in records:
                values = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
                ordered = [values.get(column) for column in columns]
                if add_manual_id:
                    ordered.insert(0, next_id)
                    next_id += 1
                payload.append(tuple(ordered))

            cursor.executemany(statement, payload)


def upsert_dataframe(df: DataFrame, table_name: str):
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported Gold table: {table_name}")

    from config import settings

    if not settings.PG_ENABLED:
        raise RuntimeError("PostgreSQL is disabled. Set FINTEL_PG_ENABLED=1 in the root .env")

    config = TABLE_CONFIG[table_name]
    missing = set(config["columns"]) - set(df.columns)
    if missing:
        raise ValueError(f"{table_name} missing DataFrame columns: {sorted(missing)}")

    selected = df.select(*config["columns"])
    # Gold batches are compact aggregates. Collect only this result set in the
    # driver and open a single PostgreSQL transaction instead of spawning a
    # Python worker partition task for each table write.
    records = selected.collect()
    _write_partition(
        settings.PG_SCHEMA,
        table_name,
        config["columns"],
        config["keys"],
        config.get("id_column"),
        records,
    )


def write_audit_record(record: dict):
    """Write one Gold batch audit row; retry-safe on (run_id, batch_id)."""
    from config import settings
    from psycopg import sql
    from config.database import get_connection

    if not settings.PG_ENABLED:
        raise RuntimeError("PostgreSQL is disabled")

    table_name = "gold_audit"
    config = TABLE_CONFIG[table_name]
    columns = config["columns"]
    values = [record.get(column) for column in columns]

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_default, is_identity
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = 'audit_id'
                """,
                (settings.PG_SCHEMA, table_name),
            )
            metadata = cursor.fetchone()
            if not metadata:
                raise RuntimeError("gold_audit.audit_id column not found")

            column_default, is_identity = metadata
            add_manual_id = column_default is None and is_identity == "NO"
            prefix_columns = []
            prefix_values = []

            if add_manual_id:
                table_ident = sql.SQL("{}.{}").format(
                    sql.Identifier(settings.PG_SCHEMA), sql.Identifier(table_name)
                )
                cursor.execute(
                    sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(table_ident)
                )
                cursor.execute(
                    sql.SQL("SELECT COALESCE(MAX(audit_id), 0) + 1 FROM {}").format(table_ident)
                )
                prefix_columns = ["audit_id"]
                prefix_values = [int(cursor.fetchone()[0])]

            insert_columns = prefix_columns + columns
            insert_values = prefix_values + values
            table_ident = sql.SQL("{}.{}").format(
                sql.Identifier(settings.PG_SCHEMA), sql.Identifier(table_name)
            )
            column_sql = sql.SQL(", ").join(sql.Identifier(c) for c in insert_columns)
            placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in insert_columns)
            update_columns = [c for c in columns if c not in config["keys"]]
            update_sql = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in update_columns
            )
            statement = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                table_ident,
                column_sql,
                placeholders,
                sql.SQL(", ").join(sql.Identifier(c) for c in config["keys"]),
                update_sql,
            )
            cursor.execute(statement, insert_values)
