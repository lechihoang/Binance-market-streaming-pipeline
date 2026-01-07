"""PostgreSQL storage module."""

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from util.logging import get_logger
from util.retry import RetryConfig, retry_operation
from util.metrics import track_latency, record_error, record_retry

logger = get_logger(__name__)


class Postgres:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "crypto",
        password: str = "crypto",
        database: str = "crypto_data",
        min_connections: int = 1,
        max_connections: int = 10,
        max_retries: int = 3,
        retry_delay: float = 1.0
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
        
        self.pool: Optional[pool.ThreadedConnectionPool] = None
        self.connect()
        self.init_tables()
        logger.info(f"Postgres initialized at {host}:{port}/{database}")

    def connect(self) -> None:
        def create_pool():
            self.pool = pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            return self.pool
        
        def on_retry_cb(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres", "connect", "failed")
        
        try:
            retry_operation(
                create_pool,
                config=self.retry,
                operation_name="PostgreSQL connection",
                on_retry=on_retry_cb,
            )
            record_retry("postgres", "connect", "success")
        except Exception:
            record_error("postgres", "connection_error", "critical")
            raise

    @contextmanager
    def conn(self):
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

    def run(
        self, 
        query: str, 
        params: Optional[tuple] = None,
        fetch: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        def do_run():
            with self.conn() as c:
                with c.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    if fetch:
                        return [dict(row) for row in cur.fetchall()]
                    return None
        
        def on_retry_cb(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres", "query", "failed")
        
        try:
            with track_latency("postgres", "query"):
                result = retry_operation(
                    do_run,
                    config=self.retry,
                    operation_name="PostgreSQL query",
                    on_retry=on_retry_cb,
                )
            record_retry("postgres", "query", "success")
            return result
        except Exception:
            record_error("postgres", "query_error", "error")
            raise

    def init_tables(self) -> None:
        with self.conn() as c:
            with c.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trades_1m (
                        timestamp TIMESTAMPTZ NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        quote_volume DOUBLE PRECISION,
                        trade_count INTEGER,
                        buy_count INTEGER,
                        sell_count INTEGER,
                        volume_weighted_avg_price DOUBLE PRECISION,
                        price_change_percent DOUBLE PRECISION,
                        buy_sell_ratio DOUBLE PRECISION,
                        average_price DOUBLE PRECISION,
                        price_volatility DOUBLE PRECISION
                    )
                """)
                
                cur.execute("""
                    SELECT create_hypertable(
                        'trades_1m', 
                        'timestamp',
                        if_not_exists => TRUE,
                        chunk_time_interval => INTERVAL '1 day'
                    )
                """)
                
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_1m_symbol_ts 
                    ON trades_1m(symbol, timestamp)
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trades_1m_ts 
                    ON trades_1m(timestamp DESC)
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS staging_trades_1m (
                        timestamp TIMESTAMPTZ NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        quote_volume DOUBLE PRECISION,
                        trade_count INTEGER,
                        buy_count INTEGER,
                        sell_count INTEGER,
                        volume_weighted_avg_price DOUBLE PRECISION,
                        price_change_percent DOUBLE PRECISION,
                        buy_sell_ratio DOUBLE PRECISION,
                        average_price DOUBLE PRECISION,
                        price_volatility DOUBLE PRECISION
                    )
                """)
                
                self.setup_policies(cur)
                self.setup_aggs(cur)
                self.setup_ml_features_table(cur)
                self.setup_predictions_table(cur)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        alert_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        message TEXT,
                        metadata TEXT,
                        UNIQUE (timestamp, symbol, alert_type)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_alerts_ts 
                    ON alerts(timestamp DESC, symbol)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS validation_errors (
                        id SERIAL PRIMARY KEY,
                        source_type VARCHAR(50) NOT NULL,
                        record_data JSONB NOT NULL,
                        failed_expectations JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_validation_errors_created 
                    ON validation_errors(created_at DESC)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_validation_errors_source 
                    ON validation_errors(source_type, created_at DESC)
                """)
                
                # Job checkpoints table for incremental processing
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS job_checkpoints (
                        job_name VARCHAR(100) PRIMARY KEY,
                        last_processed_timestamp TIMESTAMPTZ NOT NULL,
                        records_processed BIGINT DEFAULT 0,
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_job_checkpoints_updated
                    ON job_checkpoints(updated_at DESC)
                """)
        logger.debug("PostgreSQL tables initialized")

    def setup_policies(self, cur) -> None:
        try:
            cur.execute("""
                ALTER TABLE trades_1m SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'symbol'
                )
            """)
        except Exception as e:
            if "already has compression" not in str(e).lower():
                logger.warning(f"Compression setup warning: {e}")
        
        try:
            cur.execute("""
                SELECT add_compression_policy('trades_1m', INTERVAL '7 days', if_not_exists => TRUE)
            """)
        except Exception as e:
            logger.warning(f"Compression policy warning: {e}")
        
        try:
            cur.execute("""
                SELECT add_retention_policy('trades_1m', INTERVAL '90 days', if_not_exists => TRUE)
            """)
        except Exception as e:
            logger.warning(f"Retention policy warning: {e}")
        
        logger.debug("TimescaleDB policies configured")

    def setup_aggs(self, cur) -> None:
        aggs = [
            ("trades_5m", "5 minutes"),
            ("trades_15m", "15 minutes"),
            ("trades_1h", "1 hour"),
        ]
        
        for view, interval in aggs:
            try:
                cur.execute(f"""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS {view}
                    WITH (timescaledb.continuous) AS
                    SELECT
                        time_bucket(INTERVAL '{interval}', timestamp) AS timestamp,
                        symbol,
                        first(open, timestamp) AS open,
                        max(high) AS high,
                        min(low) AS low,
                        last(close, timestamp) AS close,
                        sum(volume) AS volume,
                        sum(quote_volume) AS quote_volume,
                        sum(trade_count) AS trade_count,
                        sum(buy_count) AS buy_count,
                        sum(sell_count) AS sell_count
                    FROM trades_1m
                    GROUP BY time_bucket(INTERVAL '{interval}', timestamp), symbol
                    WITH NO DATA
                """)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.warning(f"Continuous aggregate {view} warning: {e}")
        
        refresh = [
            ("trades_5m", "1 hour", "1 minute", "1 minute"),
            ("trades_15m", "2 hours", "1 minute", "5 minutes"),
            ("trades_1h", "4 hours", "5 minutes", "15 minutes"),
        ]
        
        for view, start, end, schedule in refresh:
            try:
                cur.execute(f"""
                    SELECT add_continuous_aggregate_policy('{view}',
                        start_offset => INTERVAL '{start}',
                        end_offset => INTERVAL '{end}',
                        schedule_interval => INTERVAL '{schedule}',
                        if_not_exists => TRUE
                    )
                """)
            except Exception as e:
                logger.warning(f"Refresh policy for {view} warning: {e}")
        
        logger.debug("Continuous aggregates configured")
    
    def refresh_aggregates(self) -> None:
        aggs = ["trades_5m", "trades_15m", "trades_1h"]
        for view in aggs:
            try:
                with self.conn() as c:
                    with c.cursor() as cur:
                        cur.execute(f"CALL refresh_continuous_aggregate('{view}', NULL, NULL)")
                logger.info(f"Refreshed {view}")
            except Exception as e:
                logger.debug(f"Refresh {view} skipped: {e}")

    def setup_ml_features_table(self, cur) -> None:
        """Create ml_features table matching notebook training features."""
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ml_features (
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    
                    -- Base data
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    quote_volume DOUBLE PRECISION,
                    trade_count INTEGER,
                    
                    -- Selected features matching notebook model (15 features)
                    return_5m DOUBLE PRECISION,
                    return_15m DOUBLE PRECISION,
                    volatility_5m DOUBLE PRECISION,
                    volatility_15m DOUBLE PRECISION,
                    volatility_30m DOUBLE PRECISION,
                    volatility_60m DOUBLE PRECISION,
                    volatility_ratio DOUBLE PRECISION,
                    candle_range DOUBLE PRECISION,
                    volume_ratio_60m DOUBLE PRECISION,
                    buy_ratio DOUBLE PRECISION,
                    buy_sell_imbalance DOUBLE PRECISION,
                    price_vs_ma_15m DOUBLE PRECISION,
                    price_vs_ma_60m DOUBLE PRECISION,
                    hour INTEGER,
                    symbol_encoded INTEGER,
                    
                    -- Target for training (future volatility)
                    volatility_next_5m DOUBLE PRECISION,
                    
                    computed_at TIMESTAMPTZ DEFAULT NOW(),
                    
                    PRIMARY KEY (symbol, timestamp)
                )
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ml_features_ts
                ON ml_features(timestamp DESC)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ml_features_symbol_ts
                ON ml_features(symbol, timestamp DESC)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ml_features_training
                ON ml_features(timestamp DESC)
                WHERE volatility_next_5m IS NOT NULL
            """)
            
            logger.debug("ML features table created")
        except Exception as e:
            logger.warning(f"ML features table setup warning: {e}")

    def setup_predictions_table(self, cur) -> None:
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS volatility_predictions (
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    current_volatility DOUBLE PRECISION,
                    predicted_volatility_5m DOUBLE PRECISION,
                    computed_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (symbol, timestamp)
                )
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_volatility_predictions_ts
                ON volatility_predictions(timestamp DESC)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_volatility_predictions_symbol_ts
                ON volatility_predictions(symbol, timestamp DESC)
            """)
            
            logger.debug("Volatility predictions table created")
        except Exception as e:
            logger.warning(f"Volatility predictions table setup warning: {e}")

    # ========== Job Checkpoints ==========

    def get_checkpoint(self, job_name: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT job_name, last_processed_timestamp, records_processed, updated_at
            FROM job_checkpoints
            WHERE job_name = %s
        """
        result = self.run(query, (job_name,), fetch=True)
        return result[0] if result else None

    def update_checkpoint(
        self,
        job_name: str,
        last_processed_timestamp: datetime,
        records_processed: int = 0
    ) -> None:
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
        query = "DELETE FROM job_checkpoints WHERE job_name = %s"
        self.run(query, (job_name,))
        logger.debug(f"Checkpoint deleted: {job_name}")
        return True

    def close(self) -> None:
        if self.pool:
            self.pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    def merge_staging_to_trades(self) -> int:
        merge_sql = """
            INSERT INTO trades_1m (
                timestamp, symbol, open, high, low, close, volume, quote_volume,
                trade_count, buy_count, sell_count, volume_weighted_avg_price,
                price_change_percent, buy_sell_ratio, average_price, price_volatility
            )
            SELECT 
                timestamp, symbol, open, high, low, close, volume, quote_volume,
                trade_count, buy_count, sell_count, volume_weighted_avg_price,
                price_change_percent, buy_sell_ratio, average_price, price_volatility
            FROM staging_trades_1m
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count,
                buy_count = EXCLUDED.buy_count,
                sell_count = EXCLUDED.sell_count,
                volume_weighted_avg_price = EXCLUDED.volume_weighted_avg_price,
                price_change_percent = EXCLUDED.price_change_percent,
                buy_sell_ratio = EXCLUDED.buy_sell_ratio,
                average_price = EXCLUDED.average_price,
                price_volatility = EXCLUDED.price_volatility
        """
        
        with self.conn() as c:
            with c.cursor() as cur:
                cur.execute(merge_sql)
                merged_count = cur.rowcount
                cur.execute("TRUNCATE staging_trades_1m")
        
        logger.info(f"Merged {merged_count} records from staging to trades_1m")
        return merged_count

    # ========== Candles (Read) ==========

    def get_candles(
        self, symbol: str, start: datetime, end: datetime
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT timestamp, symbol, open, high, low, close, 
                   volume, quote_volume, trade_count, buy_count, sell_count,
                   volume_weighted_avg_price, price_change_percent, 
                   buy_sell_ratio, average_price, price_volatility
            FROM trades_1m
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        result = self.run(query, (symbol, start, end), fetch=True)
        return result or []

    def get_candles_agg(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime, 
        interval: str = "5m"
    ) -> List[Dict[str, Any]]:
        if interval == "1m":
            return self.get_candles(symbol, start, end)
        
        tables = {"5m": "trades_5m", "15m": "trades_15m", "1h": "trades_1h"}
        
        if interval not in tables:
            raise ValueError(f"Invalid interval: {interval}")
        
        query = f"""
            SELECT 
                timestamp, symbol, open, high, low, close,
                volume, quote_volume, trade_count, buy_count, sell_count
            FROM {tables[interval]}
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        
        result = self.run(query, (symbol, start, end), fetch=True)
        return result or []

    # ========== Alerts (Read) ==========

    def get_alerts(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        query = """
            SELECT timestamp, symbol, alert_type, severity, message, metadata
            FROM alerts
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp DESC
        """
        result = self.run(query, (symbol, start, end), fetch=True)
        if not result:
            return []
        
        alerts = []
        for row in result:
            a = dict(row)
            if a.get('metadata') and isinstance(a['metadata'], str):
                try:
                    a['metadata'] = json.loads(a['metadata'])
                except (json.JSONDecodeError, TypeError):
                    pass
            alerts.append(a)
        return alerts

    # ========== ML Features (Read) ==========

    def get_ml_features_for_training(
        self, 
        start: datetime,
        end: datetime,
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        if symbols:
            query = """
                SELECT * FROM ml_features
                WHERE volatility_next_5m IS NOT NULL
                  AND timestamp >= %s AND timestamp <= %s
                  AND symbol = ANY(%s)
                ORDER BY timestamp
            """
            result = self.run(query, (start, end, symbols), fetch=True)
        else:
            query = """
                SELECT * FROM ml_features
                WHERE volatility_next_5m IS NOT NULL
                  AND timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp
            """
            result = self.run(query, (start, end), fetch=True)
        return result or []

    def get_ml_features_latest(self, symbol: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT * FROM ml_features
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """
        result = self.run(query, (symbol,), fetch=True)
        return result[0] if result else None

    # ========== Volatility Predictions (Read) ==========

    def get_latest_volatility_prediction(self, symbol: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT timestamp, symbol, current_volatility, predicted_volatility_5m, computed_at
            FROM volatility_predictions
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """
        result = self.run(query, (symbol,), fetch=True)
        return result[0] if result else None

    def get_volatility_predictions(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT timestamp, symbol, current_volatility, predicted_volatility_5m, computed_at
            FROM volatility_predictions
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        result = self.run(query, (symbol, start, end), fetch=True)
        return result or []

    # ========== Trades Count ==========

    def get_trades_count(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1h"
    ) -> List[Dict[str, Any]]:
        trunc = {"1m": "minute", "1h": "hour", "1d": "day"}.get(interval, "hour")
        
        query = """
            SELECT 
                date_trunc(%s, timestamp) AS bucket_timestamp,
                SUM(trade_count) AS total_trade_count
            FROM trades_1m
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY bucket_timestamp
            ORDER BY bucket_timestamp ASC
        """
        
        result = self.run(query, (trunc, symbol, start, end), fetch=True)
        if not result:
            return []
        
        return [
            {
                "timestamp": row["bucket_timestamp"],
                "trade_count": int(row["total_trade_count"] or 0),
                "interval": interval,
            }
            for row in result
        ]

    # ========== Cleanup ==========

    def cleanup(self, table: str, retention_days: int, batch_size: int = 1000) -> int:
        valid = {"trades_1m", "alerts"}
        if table not in valid:
            raise ValueError(f"Invalid table: {table}")
        
        if retention_days <= 0 or batch_size <= 0:
            raise ValueError("retention_days and batch_size must be positive")
        
        total = 0
        
        if table == "alerts":
            delete_q = f"""
                DELETE FROM {table}
                WHERE id IN (
                    SELECT id FROM {table}
                    WHERE timestamp < NOW() - INTERVAL '%s days'
                    LIMIT %s
                )
            """
        else:
            delete_q = f"""
                DELETE FROM {table}
                WHERE ctid IN (
                    SELECT ctid FROM {table}
                    WHERE timestamp < NOW() - INTERVAL '%s days'
                    LIMIT %s
                )
            """
        
        def delete_batch():
            with self.conn() as c:
                with c.cursor() as cur:
                    cur.execute(delete_q, (retention_days, batch_size))
                    deleted = cur.rowcount
                    c.commit()
                    return deleted
        
        def on_retry_cb(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres", f"cleanup_{table}", "failed")
        
        try:
            with track_latency("postgres", f"cleanup_{table}"):
                while True:
                    deleted = retry_operation(
                        delete_batch,
                        config=self.retry,
                        operation_name=f"PostgreSQL cleanup {table}",
                        on_retry=on_retry_cb,
                    )
                    total += deleted
                    if deleted < batch_size:
                        break
                    logger.debug(f"Deleted {deleted} from {table}, total: {total}")
            
            record_retry("postgres", f"cleanup_{table}", "success")
            logger.info(f"Cleanup {table}: {total} records deleted")
            return total
        except Exception as e:
            record_error("postgres", f"cleanup_{table}_error", "error")
            logger.error(f"Failed to cleanup {table}: {e}")
            raise

    def cleanup_all(self, retention_days: int, batch_size: int = 1000) -> Dict[str, int]:
        tables = ["trades_1m", "alerts"]
        results: Dict[str, int] = {}
        
        for t in tables:
            try:
                results[t] = self.cleanup(t, retention_days, batch_size)
            except Exception as e:
                logger.error(f"Failed to cleanup {t}: {e}")
                results[t] = 0
        
        logger.info(f"Cleanup all: {sum(results.values())} total records deleted")
        return results

    # ========== Validation Errors ==========

    def write_validation_errors(
        self, 
        source: str,
        records: List[Dict[str, Any]], 
        failed: List[List[Dict[str, Any]]]
    ) -> int:
        if not records:
            return 0
        
        prepared = []
        for i, rec in enumerate(records):
            prepared.append({
                'source_type': source,
                'record_data': json.dumps(rec, default=str),
                'failed_expectations': json.dumps(
                    failed[i] if i < len(failed) else [],
                    default=str
                ),
            })
        
        query = """
            INSERT INTO validation_errors
            (source_type, record_data, failed_expectations)
            VALUES (%(source_type)s, %(record_data)s, %(failed_expectations)s)
        """
        
        def do_run():
            with self.conn() as c:
                with c.cursor() as cur:
                    cur.executemany(query, prepared)
                    c.commit()
                    return len(prepared)
        
        def on_retry_cb(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres", "write_validation_errors", "failed")
        
        try:
            with track_latency("postgres", "write_validation_errors"):
                result = retry_operation(
                    do_run,
                    config=self.retry,
                    operation_name="PostgreSQL batch insert validation errors",
                    on_retry=on_retry_cb,
                )
            record_retry("postgres", "write_validation_errors", "success")
            logger.info(f"Wrote {result} validation errors for {source}")
            return result
        except Exception as e:
            record_error("postgres", "write_validation_errors_error", "error")
            logger.error(f"Failed to write validation errors: {e}")
            raise


def check_health(
    host: str = "localhost",
    port: int = 5432,
    user: str = "crypto",
    password: str = "crypto",
    database: str = "crypto_data",
    retries: int = 3,
    delay: float = 1.0,
    max_retries: Optional[int] = None,
    retry_delay: Optional[float] = None,
    **context
) -> Dict[str, Any]:
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
        c = psycopg2.connect(
            host=host, port=port, user=user,
            password=password, database=database, connect_timeout=10
        )
        with c.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        c.close()
        
        return {
            'service': 'postgresql', 'tier': 'warm', 'status': 'healthy',
            'host': host, 'port': port, 'database': database,
            'attempt': attempt_count[0], 'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def on_retry_cb(attempt: int, delay_ms: int, error: Exception):
        record_retry("postgres_health", "check", "failed")
    
    try:
        with track_latency("postgres_health", "check"):
            result = retry_operation(
                do_check, config=retry_config,
                operation_name="PostgreSQL health check", on_retry=on_retry_cb,
            )
        logger.info(f"PostgreSQL health check passed: {host}:{port}/{database}")
        record_retry("postgres_health", "check", "success")
        return result
    except Exception as e:
        record_error("postgres_health", "health_check_error", "critical")
        raise Exception(f"PostgreSQL health check failed after {actual_retries} attempts: {e}")
