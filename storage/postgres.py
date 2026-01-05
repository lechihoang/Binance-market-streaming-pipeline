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
                
                self.setup_policies(cur)
                self.setup_aggs(cur)
                self.setup_ml_view(cur)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        alert_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        message TEXT,
                        metadata JSONB,
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

    def setup_ml_view(self, cur) -> None:
        """Create ML features view for LSTM anomaly detection."""
        try:
            cur.execute("""
                CREATE OR REPLACE VIEW trades_ml_features AS
                SELECT 
                    timestamp,
                    symbol,
                    
                    -- Price features (4)
                    close,
                    price_change_percent AS price_change_pct,
                    price_volatility,
                    close - LAG(close, 5) OVER w AS price_momentum_5,
                    
                    -- Volume features (3)
                    volume,
                    quote_volume,
                    volume / NULLIF(LAG(volume, 1) OVER w, 0) AS volume_ratio,
                    
                    -- Trade features (2)
                    trade_count,
                    buy_sell_ratio
                    
                FROM trades_1m
                WINDOW w AS (PARTITION BY symbol ORDER BY timestamp)
            """)
            logger.debug("ML features view created")
        except Exception as e:
            logger.warning(f"ML view setup warning: {e}")

    def close(self) -> None:
        if self.pool:
            self.pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    # ========== Candles ==========

    def write_candle(self, candle: Dict[str, Any]) -> None:
        query = """
            INSERT INTO trades_1m 
            (timestamp, symbol, open, high, low, close, volume, quote_volume, 
             trade_count, buy_count, sell_count, volume_weighted_avg_price,
             price_change_percent, buy_sell_ratio, average_price, price_volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume, trade_count = EXCLUDED.trade_count,
                buy_count = EXCLUDED.buy_count, sell_count = EXCLUDED.sell_count,
                volume_weighted_avg_price = EXCLUDED.volume_weighted_avg_price,
                price_change_percent = EXCLUDED.price_change_percent,
                buy_sell_ratio = EXCLUDED.buy_sell_ratio,
                average_price = EXCLUDED.average_price,
                price_volatility = EXCLUDED.price_volatility
        """
        params = (
            candle.get('timestamp'), candle.get('symbol'),
            candle.get('open'), candle.get('high'), candle.get('low'),
            candle.get('close'), candle.get('volume'),
            candle.get('quote_volume'), candle.get('trade_count'),
            candle.get('buy_count'), candle.get('sell_count'),
            candle.get('volume_weighted_avg_price'),
            candle.get('price_change_percent'),
            candle.get('buy_sell_ratio'),
            candle.get('average_price'),
            candle.get('price_volatility')
        )
        self.run(query, params)

    def write_candles(self, candles: List[Dict[str, Any]]) -> int:
        if not candles:
            return 0
        
        query = """
            INSERT INTO trades_1m 
            (timestamp, symbol, open, high, low, close, volume, quote_volume, 
             trade_count, buy_count, sell_count, volume_weighted_avg_price,
             price_change_percent, buy_sell_ratio, average_price, price_volatility)
            VALUES (%(timestamp)s, %(symbol)s, %(open)s, %(high)s, %(low)s, 
                    %(close)s, %(volume)s, %(quote_volume)s, %(trade_count)s, 
                    %(buy_count)s, %(sell_count)s, %(volume_weighted_avg_price)s,
                    %(price_change_percent)s, %(buy_sell_ratio)s, 
                    %(average_price)s, %(price_volatility)s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume, trade_count = EXCLUDED.trade_count,
                buy_count = EXCLUDED.buy_count, sell_count = EXCLUDED.sell_count,
                volume_weighted_avg_price = EXCLUDED.volume_weighted_avg_price,
                price_change_percent = EXCLUDED.price_change_percent,
                buy_sell_ratio = EXCLUDED.buy_sell_ratio,
                average_price = EXCLUDED.average_price,
                price_volatility = EXCLUDED.price_volatility
        """
        
        def do_run():
            with self.conn() as c:
                with c.cursor() as cur:
                    cur.executemany(query, candles)
                    c.commit()
                    return len(candles)
        
        def on_retry_cb(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres", "write_candles", "failed")
        
        try:
            with track_latency("postgres", "write_candles"):
                result = retry_operation(
                    do_run,
                    config=self.retry,
                    operation_name="PostgreSQL batch upsert candles",
                    on_retry=on_retry_cb,
                )
            record_retry("postgres", "write_candles", "success")
            logger.debug(f"Wrote {result} candles")
            return result
        except Exception as e:
            record_error("postgres", "write_candles_error", "error")
            logger.error(f"Failed to write candles: {e}")
            raise

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

    # ========== Alerts ==========

    def write_alert(self, alert: Dict[str, Any]) -> None:
        metadata = alert.get('metadata')
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)
        
        query = """
            INSERT INTO alerts
            (timestamp, symbol, alert_type, severity, message, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            alert.get('timestamp'), alert.get('symbol'),
            alert.get('alert_type'), alert.get('severity'),
            alert.get('message'), metadata
        )
        self.run(query, params)

    def write_alerts(self, alerts: List[Dict[str, Any]]) -> int:
        if not alerts:
            return 0
        
        prepared = []
        for a in alerts:
            p = dict(a)
            meta = p.get('metadata')
            if isinstance(meta, dict):
                p['metadata'] = json.dumps(meta)
            prepared.append(p)
        
        query = """
            INSERT INTO alerts
            (timestamp, symbol, alert_type, severity, message, metadata)
            VALUES (%(timestamp)s, %(symbol)s, %(alert_type)s, %(severity)s, 
                    %(message)s, %(metadata)s)
            ON CONFLICT (timestamp, symbol, alert_type) DO UPDATE SET
                severity = EXCLUDED.severity,
                message = EXCLUDED.message,
                metadata = EXCLUDED.metadata
        """
        
        def do_run():
            with self.conn() as c:
                with c.cursor() as cur:
                    cur.executemany(query, prepared)
                    c.commit()
                    return len(prepared)
        
        def on_retry_cb(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres", "write_alerts", "failed")
        
        try:
            with track_latency("postgres", "write_alerts"):
                result = retry_operation(
                    do_run,
                    config=self.retry,
                    operation_name="PostgreSQL batch insert alerts",
                    on_retry=on_retry_cb,
                )
            record_retry("postgres", "write_alerts", "success")
            logger.debug(f"Wrote {result} alerts")
            return result
        except Exception as e:
            record_error("postgres", "write_alerts_error", "error")
            logger.error(f"Failed to write alerts: {e}")
            raise

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

    # ========== ML Features ==========

    def get_ml_features(
        self, 
        symbol: str, 
        limit: int = 60
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT * FROM trades_ml_features
            WHERE symbol = %s
              AND timestamp > NOW() - INTERVAL '2 hours'
            ORDER BY timestamp DESC
            LIMIT %s
        """
        result = self.run(query, (symbol, limit), fetch=True)
        return result[::-1] if result else []

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
