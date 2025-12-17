"""
PostgreSQL storage module for warm path data access.

Provides sub-second latency access to 90 days of historical data.
Tables: trades_1m, indicators, alerts
"""

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from src.utils.logging import get_logger
from src.utils.retry import RetryConfig, retry_operation
from src.utils.metrics import track_latency, record_error, record_retry

logger = get_logger(__name__)


class PostgresStorage:
    """PostgreSQL storage for interactive analytics (Warm Path).
    
    Stores 90 days of historical data with sub-second query latency.
    Supports concurrent writes from multiple Spark jobs.
    Tables: trades_1m, indicators, alerts
    """

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
        
        self._retry_config = RetryConfig(
            max_retries=max_retries,
            initial_delay_ms=int(retry_delay * 1000),
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.1,
            retryable_exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError),
        )
        
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._connect_with_retry()
        self._init_tables()
        logger.info(f"PostgresStorage initialized at {host}:{port}/{database}")


    def _connect_with_retry(self) -> None:
        """Create connection pool with retry logic."""
        def create_pool():
            self._pool = pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            return self._pool
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres_storage", "connect", "failed")
        
        try:
            retry_operation(
                create_pool,
                config=self._retry_config,
                operation_name="PostgreSQL connection",
                on_retry=on_retry,
            )
            record_retry("postgres_storage", "connect", "success")
        except Exception:
            record_error("postgres_storage", "connection_error", "critical")
            raise

    @contextmanager
    def _get_connection(self):
        """Get a connection from the pool with automatic return."""
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    def _execute_with_retry(
        self, 
        query: str, 
        params: tuple = None,
        fetch: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        """Execute a query with retry logic."""
        def execute_query():
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    if fetch:
                        return [dict(row) for row in cur.fetchall()]
                    return None
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres_storage", "query", "failed")
        
        try:
            with track_latency("postgres_storage", "query"):
                result = retry_operation(
                    execute_query,
                    config=self._retry_config,
                    operation_name="PostgreSQL query",
                    on_retry=on_retry,
                )
            record_retry("postgres_storage", "query", "success")
            return result
        except Exception:
            record_error("postgres_storage", "query_error", "error")
            raise

    def _init_tables(self) -> None:
        """Create tables and indexes if they don't exist."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trades_1m (
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        quote_volume DOUBLE PRECISION,
                        trades_count INTEGER,
                        PRIMARY KEY (symbol, timestamp)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trades_1m_ts 
                    ON trades_1m(timestamp)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS indicators (
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        rsi DOUBLE PRECISION,
                        macd DOUBLE PRECISION,
                        macd_signal DOUBLE PRECISION,
                        sma_20 DOUBLE PRECISION,
                        ema_12 DOUBLE PRECISION,
                        ema_26 DOUBLE PRECISION,
                        bb_upper DOUBLE PRECISION,
                        bb_lower DOUBLE PRECISION,
                        atr DOUBLE PRECISION,
                        PRIMARY KEY (symbol, timestamp)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_indicators_ts 
                    ON indicators(timestamp)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        alert_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        message TEXT,
                        metadata JSONB
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_alerts_ts 
                    ON alerts(timestamp DESC, symbol)
                """)
        logger.debug("PostgreSQL tables initialized")

    def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("PostgreSQL connection pool closed")


    # ==================== Upsert Operations ====================

    def upsert_candle(self, candle: Dict[str, Any]) -> None:
        """Upsert a 1-minute candle record."""
        query = """
            INSERT INTO trades_1m 
            (timestamp, symbol, open, high, low, close, volume, quote_volume, trades_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume, trades_count = EXCLUDED.trades_count
        """
        params = (
            candle.get('timestamp'), candle.get('symbol'),
            candle.get('open'), candle.get('high'), candle.get('low'),
            candle.get('close'), candle.get('volume'),
            candle.get('quote_volume'), candle.get('trades_count')
        )
        self._execute_with_retry(query, params)

    def upsert_indicators(self, indicators: Dict[str, Any]) -> None:
        """Upsert technical indicators record."""
        query = """
            INSERT INTO indicators
            (timestamp, symbol, rsi, macd, macd_signal, sma_20, ema_12, ema_26, 
             bb_upper, bb_lower, atr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                rsi = EXCLUDED.rsi, macd = EXCLUDED.macd, macd_signal = EXCLUDED.macd_signal,
                sma_20 = EXCLUDED.sma_20, ema_12 = EXCLUDED.ema_12, ema_26 = EXCLUDED.ema_26,
                bb_upper = EXCLUDED.bb_upper, bb_lower = EXCLUDED.bb_lower, atr = EXCLUDED.atr
        """
        params = (
            indicators.get('timestamp'), indicators.get('symbol'),
            indicators.get('rsi'), indicators.get('macd'), indicators.get('macd_signal'),
            indicators.get('sma_20'), indicators.get('ema_12'), indicators.get('ema_26'),
            indicators.get('bb_upper'), indicators.get('bb_lower'), indicators.get('atr')
        )
        self._execute_with_retry(query, params)

    def insert_alert(self, alert: Dict[str, Any]) -> None:
        """Insert an alert record."""
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
        self._execute_with_retry(query, params)

    # ==================== Batch Operations ====================

    def upsert_candles_batch(self, candles: List[Dict[str, Any]]) -> int:
        """Batch upsert candles using executemany.
        
        Uses INSERT with ON CONFLICT for efficient batch upsert operations.
        
        Args:
            candles: List of candle dictionaries with keys:
                timestamp, symbol, open, high, low, close, volume, 
                quote_volume, trades_count
                
        Returns:
            Number of affected rows (inserts + updates)
            
        Requirements: 1.3, 4.1, 4.3
        """
        if not candles:
            return 0
        
        query = """
            INSERT INTO trades_1m 
            (timestamp, symbol, open, high, low, close, volume, quote_volume, trades_count)
            VALUES (%(timestamp)s, %(symbol)s, %(open)s, %(high)s, %(low)s, 
                    %(close)s, %(volume)s, %(quote_volume)s, %(trades_count)s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume, trades_count = EXCLUDED.trades_count
        """
        
        def execute_batch():
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(query, candles)
                    conn.commit()
                    return len(candles)
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres_storage", "upsert_candles_batch", "failed")
        
        try:
            with track_latency("postgres_storage", "upsert_candles_batch"):
                result = retry_operation(
                    execute_batch,
                    config=self._retry_config,
                    operation_name="PostgreSQL batch upsert candles",
                    on_retry=on_retry,
                )
            record_retry("postgres_storage", "upsert_candles_batch", "success")
            logger.debug(f"Batch upserted {result} candles")
            return result
        except Exception as e:
            record_error("postgres_storage", "upsert_candles_batch_error", "error")
            logger.error(f"Failed to batch upsert candles: {e}")
            raise

    def insert_alerts_batch(self, alerts: List[Dict[str, Any]]) -> int:
        """Batch insert alerts using executemany.
        
        Args:
            alerts: List of alert dictionaries with keys:
                timestamp, symbol, alert_type, severity, message, metadata
                
        Returns:
            Number of inserted rows
            
        Requirements: 4.2, 4.3
        """
        if not alerts:
            return 0
        
        # Prepare alerts with JSON-serialized metadata
        prepared_alerts = []
        for alert in alerts:
            prepared = dict(alert)
            metadata = prepared.get('metadata')
            if isinstance(metadata, dict):
                prepared['metadata'] = json.dumps(metadata)
            prepared_alerts.append(prepared)
        
        query = """
            INSERT INTO alerts
            (timestamp, symbol, alert_type, severity, message, metadata)
            VALUES (%(timestamp)s, %(symbol)s, %(alert_type)s, %(severity)s, 
                    %(message)s, %(metadata)s)
        """
        
        def execute_batch():
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(query, prepared_alerts)
                    conn.commit()
                    return len(prepared_alerts)
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres_storage", "insert_alerts_batch", "failed")
        
        try:
            with track_latency("postgres_storage", "insert_alerts_batch"):
                result = retry_operation(
                    execute_batch,
                    config=self._retry_config,
                    operation_name="PostgreSQL batch insert alerts",
                    on_retry=on_retry,
                )
            record_retry("postgres_storage", "insert_alerts_batch", "success")
            logger.debug(f"Batch inserted {result} alerts")
            return result
        except Exception as e:
            record_error("postgres_storage", "insert_alerts_batch_error", "error")
            logger.error(f"Failed to batch insert alerts: {e}")
            raise

    # ==================== Query Methods ====================

    def query_candles(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query candles for a symbol within time range."""
        interval_minutes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
        minutes = interval_minutes.get(interval, 1)
        
        if minutes == 1:
            query = """
                SELECT timestamp, symbol, open, high, low, close, 
                       volume, quote_volume, trades_count
                FROM trades_1m
                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC
            """
            result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        else:
            query = """
                WITH bucketed AS (
                    SELECT 
                        date_trunc('hour', timestamp) + 
                        (EXTRACT(minute FROM timestamp)::int / %s) * INTERVAL '%s minutes' AS bucket,
                        symbol, open, high, low, close, volume, quote_volume, trades_count,
                        timestamp,
                        ROW_NUMBER() OVER (PARTITION BY 
                            date_trunc('hour', timestamp) + 
                            (EXTRACT(minute FROM timestamp)::int / %s) * INTERVAL '%s minutes'
                            ORDER BY timestamp ASC) as rn_first,
                        ROW_NUMBER() OVER (PARTITION BY 
                            date_trunc('hour', timestamp) + 
                            (EXTRACT(minute FROM timestamp)::int / %s) * INTERVAL '%s minutes'
                            ORDER BY timestamp DESC) as rn_last
                    FROM trades_1m
                    WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                )
                SELECT 
                    bucket as timestamp, %s as symbol,
                    (SELECT open FROM bucketed b2 WHERE b2.bucket = b.bucket AND b2.rn_first = 1 LIMIT 1) as open,
                    MAX(high) as high, MIN(low) as low,
                    (SELECT close FROM bucketed b3 WHERE b3.bucket = b.bucket AND b3.rn_last = 1 LIMIT 1) as close,
                    SUM(volume) as volume, SUM(quote_volume) as quote_volume, SUM(trades_count) as trades_count
                FROM bucketed b
                GROUP BY bucket
                ORDER BY bucket ASC
            """
            result = self._execute_with_retry(
                query, 
                (minutes, minutes, minutes, minutes, minutes, minutes, symbol, start, end, symbol), 
                fetch=True
            )
        return result or []

    def query_indicators(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        """Query technical indicators for a symbol within time range."""
        query = """
            SELECT timestamp, symbol, rsi, macd, macd_signal, sma_20,
                   ema_12, ema_26, bb_upper, bb_lower, atr
            FROM indicators
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        return result or []

    def query_alerts(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        """Query alerts for a symbol within time range."""
        query = """
            SELECT timestamp, symbol, alert_type, severity, message, metadata
            FROM alerts
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp DESC
        """
        result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        if not result:
            return []
        
        alerts = []
        for row in result:
            alert = dict(row)
            if alert.get('metadata') and isinstance(alert['metadata'], str):
                try:
                    alert['metadata'] = json.loads(alert['metadata'])
                except (json.JSONDecodeError, TypeError):
                    pass
            alerts.append(alert)
        return alerts

    def query_trades_count(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1h"
    ) -> List[Dict[str, Any]]:
        """Query trades count aggregated by time interval.
        
        Args:
            symbol: Trading pair symbol (e.g., BTCUSDT)
            start: Start datetime
            end: End datetime
            interval: Time interval (1m, 1h, 1d)
            
        Returns:
            List of dicts with timestamp, trades_count, interval
        """
        # Map interval to PostgreSQL date_trunc format
        interval_map = {
            "1m": "minute",
            "1h": "hour",
            "1d": "day",
        }
        
        trunc_interval = interval_map.get(interval, "hour")
        
        query = """
            SELECT 
                date_trunc(%s, timestamp) AS bucket_timestamp,
                SUM(trades_count) AS total_trades_count
            FROM trades_1m
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY bucket_timestamp
            ORDER BY bucket_timestamp ASC
        """
        
        result = self._execute_with_retry(
            query, (trunc_interval, symbol, start, end), fetch=True
        )
        
        if not result:
            return []
        
        return [
            {
                "timestamp": row["bucket_timestamp"],
                "trades_count": int(row["total_trades_count"] or 0),
                "interval": interval,
            }
            for row in result
        ]


# ============================================================================
# HEALTH CHECK
# ============================================================================

def check_postgres_health(
    host: str = "localhost",
    port: int = 5432,
    user: str = "crypto",
    password: str = "crypto",
    database: str = "crypto_data",
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """Check PostgreSQL connection health."""
    retry_config = RetryConfig(
        max_retries=max_retries,
        initial_delay_ms=int(retry_delay * 1000),
        max_delay_ms=60000,
        multiplier=2.0,
        jitter_factor=0.1,
    )
    
    attempt_count = [0]
    
    def do_health_check():
        attempt_count[0] += 1
        conn = psycopg2.connect(
            host=host, port=port, user=user,
            password=password, database=database, connect_timeout=10
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        
        return {
            'service': 'postgresql', 'tier': 'warm', 'status': 'healthy',
            'host': host, 'port': port, 'database': database,
            'attempt': attempt_count[0], 'timestamp': datetime.now().isoformat()
        }
    
    def on_retry(attempt: int, delay_ms: int, error: Exception):
        record_retry("postgres_health", "check", "failed")
    
    try:
        with track_latency("postgres_health", "check"):
            result = retry_operation(
                do_health_check, config=retry_config,
                operation_name="PostgreSQL health check", on_retry=on_retry,
            )
        logger.info(f"PostgreSQL health check passed: {host}:{port}/{database}")
        record_retry("postgres_health", "check", "success")
        return result
    except Exception as e:
        record_error("postgres_health", "health_check_error", "critical")
        raise Exception(f"PostgreSQL health check failed after {max_retries} attempts: {e}")
