"""PostgreSQL storage module - data operations only."""

import json
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from typing import Any

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from util.constant import (
    ALERT_FIELD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_MAX_CONN,
    POSTGRES_MAX_RETRY,
    POSTGRES_MIN_CONN,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_RETRY_DELAY,
    POSTGRES_USER,
    TRADE_FIELD,
)
from util.logging import get_logger
from util.metrics import record_error, record_retry, track_latency
from util.retry import RetryConfig, retry_operation

logger = get_logger(__name__)


class Postgres:
    """PostgreSQL database operations for crypto data pipeline."""

    def __init__(
        self,
        host: str = POSTGRES_HOST,
        port: int = POSTGRES_PORT,
        user: str = POSTGRES_USER,
        password: str = POSTGRES_PASSWORD,
        database: str = POSTGRES_DB,
        min_connections: int = POSTGRES_MIN_CONN,
        max_connections: int = POSTGRES_MAX_CONN,
        max_retries: int = POSTGRES_MAX_RETRY,
        retry_delay: float = POSTGRES_RETRY_DELAY,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.min_connections = min_connections
        self.max_connections = max_connections

        self.retry = RetryConfig(
            max_retries=max_retries,
            initial_delay_ms=int(retry_delay * 1000),
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.1,
            retryable_exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError),
        )

        self.pool: pool.ThreadedConnectionPool | None = None
        self.connect()
        logger.info(f"Postgres initialized at {host}:{port}/{database}")

    def connect(self) -> None:
        """Establish connection pool with retry."""

        def create_pool():
            self.pool = pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            return self.pool

        try:
            retry_operation(
                create_pool,
                config=self.retry,
                operation_name="PostgreSQL connection",
                on_retry=lambda a, d, e: record_retry("postgres", "connect", "failed"),
            )
            record_retry("postgres", "connect", "success")
        except Exception:
            record_error("postgres", "connection_error", "critical")
            raise

    @contextmanager
    def conn(self):
        """Context manager for database connection with auto commit/rollback."""
        c = None
        try:
            c = self.pool.getconn()  # type: ignore
            yield c
            c.commit()
        except Exception:
            if c and not c.closed:
                c.rollback()
            raise
        finally:
            if c and not c.closed:
                self.pool.putconn(c)  # type: ignore

    def run(self, query: str, params: tuple | None = None, fetch: bool = False) -> list[dict[str, Any]] | None:
        """Execute SQL query with retry logic."""

        def do_run():
            with self.conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()] if fetch else None

        try:
            with track_latency("postgres", "query"):
                result = retry_operation(
                    do_run,
                    config=self.retry,
                    operation_name="PostgreSQL query",
                    on_retry=lambda a, d, e: record_retry("postgres", "query", "failed"),
                )
            record_retry("postgres", "query", "success")
            return result
        except Exception:
            record_error("postgres", "query_error", "error")
            raise

    def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    # ========== Merge Operations ==========

    def build_update_clause(self, field_list: list[str]) -> str:
        """Build UPDATE clause for MERGE operations."""
        return ", ".join(f"{f}=EXCLUDED.{f}" for f in field_list if f not in ("timestamp", "symbol"))

    def merge_staging_to_trade(self) -> int:
        """Merge staging trades to main trades table."""
        fields_str = ", ".join(TRADE_FIELD)
        update_clause = self.build_update_clause(TRADE_FIELD)

        merge_sql = f"""
            INSERT INTO trades_1m ({fields_str})
            SELECT {fields_str} FROM staging_trades_1m
            ON CONFLICT (symbol, timestamp) DO UPDATE SET {update_clause}
        """

        with self.conn() as c, c.cursor() as cur:
            cur.execute(merge_sql)
            count = cur.rowcount
            cur.execute("TRUNCATE staging_trades_1m")

        logger.info(f"Merged {count} records from staging to trades_1m")
        return count

    def merge_staging_to_alert(self) -> int:
        """Merge staging alerts to main alerts table."""
        fields_str = ", ".join(ALERT_FIELD)

        merge_sql = f"""
            INSERT INTO alerts ({fields_str})
            SELECT {fields_str} FROM staging_alerts
            ON CONFLICT (timestamp, symbol, alert_type) DO NOTHING
        """

        with self.conn() as c, c.cursor() as cur:
            cur.execute(merge_sql)
            count = cur.rowcount
            cur.execute("TRUNCATE staging_alerts")

        logger.info(f"Merged {count} alerts from staging to alerts")
        return count

    # ========== Read Operations ==========

    def query_by_range(
        self, table: str, symbol: str, start: datetime, end: datetime, column: str = "*", order: str = "timestamp ASC"
    ) -> list[dict[str, Any]]:
        """Generic query by symbol and time range."""
        query = f"""
            SELECT {column} FROM {table}
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY {order}
        """
        return self.run(query, (symbol, start, end), fetch=True) or []

    def get_candle(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """Get OHLCV candles from trades_1m table."""
        return self.query_by_range("trades_1m", symbol, start, end, ", ".join(TRADE_FIELD))

    def get_candle_agg(self, symbol: str, start: datetime, end: datetime, interval: str = "5m") -> list[dict[str, Any]]:
        """Get candles from materialized views for different timeframes."""
        if interval == "1m":
            return self.get_candle(symbol, start, end)

        table_map = {"5m": "trades_5m", "15m": "trades_15m", "1h": "trades_1h"}
        if interval not in table_map:
            raise ValueError(f"Invalid interval: {interval}")

        column = "timestamp, symbol, open, high, low, close, volume, quote_volume, trade_count, buy_count, sell_count"
        return self.query_by_range(table_map[interval], symbol, start, end, column)

    def get_alert(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        """Get alerts for a symbol in time range."""
        result = self.query_by_range("alerts", symbol, start, end, ", ".join(ALERT_FIELD), "timestamp DESC")

        for row in result:
            if row.get("metadata") and isinstance(row["metadata"], str):
                with suppress(json.JSONDecodeError):
                    row["metadata"] = json.loads(row["metadata"])
        return result

    def get_trades_count(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1h"
    ) -> list[dict[str, Any]]:
        """Get aggregated trade count by time interval."""
        trunc = {"1m": "minute", "1h": "hour", "1d": "day"}.get(interval, "hour")

        query = """
            SELECT date_trunc(%s, timestamp) AS bucket_timestamp, SUM(trade_count) AS total_trade_count
            FROM trades_1m
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY bucket_timestamp ORDER BY bucket_timestamp ASC
        """

        result = self.run(query, (trunc, symbol, start, end), fetch=True)
        return [
            {"timestamp": r["bucket_timestamp"], "trade_count": int(r["total_trade_count"] or 0), "interval": interval}
            for r in (result or [])
        ]

    # ========== ML Features ==========

    def get_ml_features_for_training(
        self, start: datetime, end: datetime, symbols: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Get ML features for model training."""
        base = "SELECT * FROM ml_features WHERE volatility_next_5m IS NOT NULL AND timestamp >= %s AND timestamp <= %s"

        if symbols:
            query = f"{base} AND symbol = ANY(%s) ORDER BY timestamp"
            return self.run(query, (start, end, symbols), fetch=True) or []

        return self.run(f"{base} ORDER BY timestamp", (start, end), fetch=True) or []

    def get_ml_features_latest(self, symbol: str) -> dict[str, Any] | None:
        """Get latest ML features for a symbol."""
        query = "SELECT * FROM ml_features WHERE symbol = %s ORDER BY timestamp DESC LIMIT 1"
        result = self.run(query, (symbol,), fetch=True)
        return result[0] if result else None

    # ========== Volatility Predictions ==========

    def get_latest_volatility_prediction(self, symbol: str) -> dict[str, Any] | None:
        """Get latest volatility prediction for a symbol."""
        query = """
            SELECT timestamp, symbol, current_volatility, predicted_volatility_5m, computed_at
            FROM volatility_predictions WHERE symbol = %s ORDER BY timestamp DESC LIMIT 1
        """
        result = self.run(query, (symbol,), fetch=True)
        return result[0] if result else None

    def get_volatility_prediction(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        """Get volatility predictions in time range."""
        query = """
            SELECT timestamp, symbol, current_volatility, predicted_volatility_5m, computed_at
            FROM volatility_predictions
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s ORDER BY timestamp ASC
        """
        return self.run(query, (symbol, start, end), fetch=True) or []

    # ========== Job Checkpoints ==========

    def get_checkpoint(self, job_name: str) -> dict[str, Any] | None:
        """Get checkpoint for a job."""
        query = "SELECT job_name, last_processed_timestamp, records_processed, updated_at FROM job_checkpoints WHERE job_name = %s"
        result = self.run(query, (job_name,), fetch=True)
        return result[0] if result else None

    def update_checkpoint(self, job_name: str, last_processed_timestamp: datetime, records_processed: int = 0) -> None:
        """Update checkpoint for a job."""
        query = """
            INSERT INTO job_checkpoints (job_name, last_processed_timestamp, records_processed, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (job_name) DO UPDATE SET
                last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                records_processed = job_checkpoints.records_processed + EXCLUDED.records_processed,
                updated_at = NOW()
        """
        self.run(query, (job_name, last_processed_timestamp, records_processed))
        logger.debug(f"Checkpoint updated: {job_name} -> {last_processed_timestamp}")

    def delete_checkpoint(self, job_name: str) -> bool:
        """Delete checkpoint for a job."""
        self.run("DELETE FROM job_checkpoints WHERE job_name = %s", (job_name,))
        logger.debug(f"Checkpoint deleted: {job_name}")
        return True

    # ========== Cleanup ==========

    def cleanup(self, table: str, retention_days: int, batch_size: int = 1000) -> int:
        """Delete old records from specified table in batches."""
        if table not in {"trades_1m", "alerts"}:
            raise ValueError(f"Invalid table: {table}")
        if retention_days <= 0 or batch_size <= 0:
            raise ValueError("retention_days and batch_size must be positive")

        id_col = "id" if table == "alerts" else "ctid"
        delete_sql = f"""
            DELETE FROM {table} WHERE {id_col} IN (
                SELECT {id_col} FROM {table}
                WHERE timestamp < NOW() - INTERVAL '%s days' LIMIT %s
            )
        """

        total = 0
        while True:
            with self.conn() as c, c.cursor() as cur:
                cur.execute(delete_sql, (retention_days, batch_size))
                deleted = cur.rowcount

            total += deleted
            if deleted < batch_size:
                break
            logger.debug(f"Deleted {deleted} from {table}, total: {total}")

        logger.info(f"Cleanup {table}: {total} records deleted")
        return total

    def cleanup_all(self, retention_days: int, batch_size: int = 1000) -> dict[str, int]:
        """Cleanup all tables."""
        results = {}
        for t in ["trades_1m", "alerts"]:
            try:
                results[t] = self.cleanup(t, retention_days, batch_size)
            except Exception as e:
                logger.error(f"Failed to cleanup {t}: {e}")
                results[t] = 0

        logger.info(f"Cleanup all: {sum(results.values())} total records deleted")
        return results

    # ========== Validation Errors ==========

    def write_validation_errors(
        self, source: str, records: list[dict[str, Any]], failed: list[list[dict[str, Any]]]
    ) -> int:
        """Write validation errors to database."""
        if not records:
            return 0

        prepared = [
            {
                "source_type": source,
                "record_data": json.dumps(rec, default=str),
                "failed_expectations": json.dumps(failed[i] if i < len(failed) else [], default=str),
            }
            for i, rec in enumerate(records)
        ]

        query = """
            INSERT INTO validation_errors (source_type, record_data, failed_expectations)
            VALUES (%(source_type)s, %(record_data)s, %(failed_expectations)s)
        """

        def do_run():
            with self.conn() as c, c.cursor() as cur:
                cur.executemany(query, prepared)
                return len(prepared)

        try:
            with track_latency("postgres", "write_validation_errors"):
                result = retry_operation(
                    do_run,
                    config=self.retry,
                    operation_name="PostgreSQL batch insert validation errors",
                    on_retry=lambda a, d, e: record_retry("postgres", "write_validation_errors", "failed"),
                )
            record_retry("postgres", "write_validation_errors", "success")
            logger.info(f"Wrote {result} validation errors for {source}")
            return result
        except Exception as e:
            record_error("postgres", "write_validation_errors_error", "error")
            logger.error(f"Failed to write validation errors: {e}")
            raise

    # ========== Aggregates Refresh ==========

    def refresh_aggregates(self) -> None:
        """Manually refresh continuous aggregates if needed."""
        for view in ["trades_5m", "trades_15m", "trades_1h"]:
            try:
                with self.conn() as c, c.cursor() as cur:
                    cur.execute(f"CALL refresh_continuous_aggregate('{view}', NULL, NULL)")
                logger.info(f"Refreshed {view}")
            except Exception as e:
                logger.debug(f"Refresh {view} skipped: {e}")


def check_health(
    host: str = POSTGRES_HOST,
    port: int = POSTGRES_PORT,
    user: str = POSTGRES_USER,
    password: str = POSTGRES_PASSWORD,
    database: str = POSTGRES_DB,
    retries: int = POSTGRES_MAX_RETRY,
    delay: float = POSTGRES_RETRY_DELAY,
    max_retries: int | None = None,
    retry_delay: float | None = None,
    **_context,
) -> dict[str, Any]:
    """Check PostgreSQL connection health."""
    actual_retries = max_retries if max_retries is not None else retries
    actual_delay = retry_delay if retry_delay is not None else delay

    retry_config = RetryConfig(
        max_retries=actual_retries,
        initial_delay_ms=int(actual_delay * 1000),
        max_delay_ms=60000,
        multiplier=2.0,
        jitter_factor=0.1,
    )

    attempt_count = [0]

    def do_check():
        attempt_count[0] += 1
        c = psycopg2.connect(host=host, port=port, user=user, password=password, database=database, connect_timeout=10)
        with c.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        c.close()

        return {
            "service": "postgresql",
            "tier": "warm",
            "status": "healthy",
            "host": host,
            "port": port,
            "database": database,
            "attempt": attempt_count[0],
            "timestamp": datetime.now(UTC).isoformat(),
        }

    try:
        with track_latency("postgres_health", "check"):
            result = retry_operation(
                do_check,
                config=retry_config,
                operation_name="PostgreSQL health check",
                on_retry=lambda a, d, e: record_retry("postgres_health", "check", "failed"),
            )
        logger.info(f"PostgreSQL health check passed: {host}:{port}/{database}")
        record_retry("postgres_health", "check", "success")
        return result
    except Exception as e:
        record_error("postgres_health", "health_check_error", "critical")
        raise Exception(f"PostgreSQL health check failed after {actual_retries} attempts: {e}")
