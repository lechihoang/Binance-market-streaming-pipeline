"""
PostgresStorage - Warm Path storage for interactive analytics.

Provides sub-second latency access to 90-day historical data
using PostgreSQL database with connection pooling for concurrent writes.
"""

import logging
import time
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class PostgresStorage:
    """PostgreSQL storage for interactive analytics (Warm Path).
    
    Stores 90 days of historical data with sub-second query latency.
    Supports concurrent writes from multiple Spark jobs.
    Tables: trades_1m, indicators, alerts, daily_stats
    """
    
    # Retention periods in days
    RETENTION_TRADES_1M = 90
    RETENTION_INDICATORS = 90
    RETENTION_ALERTS = 180
    RETENTION_DAILY_STATS = 730  # 2 years
    
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
        """Initialize PostgreSQL connection pool.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            user: Database user
            password: Database password
            database: Database name
            min_connections: Minimum connections in pool
            max_connections: Maximum connections in pool
            max_retries: Maximum number of connection retry attempts
            retry_delay: Base delay in seconds between retries (exponential backoff)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._connect_with_retry()
        self._init_tables()
        logger.info(
            f"PostgresStorage initialized at {host}:{port}/{database}"
        )

    def _connect_with_retry(self) -> None:
        """Create connection pool with retry logic and exponential backoff.
        
        Raises:
            psycopg2.OperationalError: If connection fails after all retries
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                self._pool = pool.ThreadedConnectionPool(
                    self.min_connections,
                    self.max_connections,
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                if attempt > 0:
                    logger.info(
                        f"PostgreSQL connection successful on attempt {attempt + 1}"
                    )
                return
            except psycopg2.OperationalError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"PostgreSQL connection failed (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"PostgreSQL connection failed after {self.max_retries} attempts"
                    )
                    raise
        
        raise last_error

    @contextmanager
    def _get_connection(self):
        """Get a connection from the pool with automatic return.
        
        Yields:
            psycopg2 connection object
        """
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
        """Execute a query with retry logic.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            fetch: If True, fetch and return results
            
        Returns:
            List of result dicts if fetch=True, else None
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                with self._get_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(query, params)
                        if fetch:
                            return [dict(row) for row in cur.fetchall()]
                        return None
            except psycopg2.OperationalError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Query failed (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    raise
        
        raise last_error


    def _init_tables(self) -> None:
        """Create tables and indexes if they don't exist.
        
        Creates:
        - trades_1m: 1-minute candle data
        - indicators: Technical indicators
        - alerts: Alert records
        - daily_stats: Daily statistics
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # trades_1m table for 1-minute candles
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
                
                # Create index on timestamp for trades_1m
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trades_1m_ts 
                    ON trades_1m(timestamp)
                """)
                
                # indicators table
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
                
                # Create index on timestamp for indicators
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_indicators_ts 
                    ON indicators(timestamp)
                """)
                
                # alerts table with SERIAL id
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
                
                # Create index on timestamp and symbol for alerts
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_alerts_ts 
                    ON alerts(timestamp DESC, symbol)
                """)
                
                # daily_stats table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        date DATE NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        volatility DOUBLE PRECISION,
                        PRIMARY KEY (symbol, date)
                    )
                """)
                
        logger.debug("PostgreSQL tables initialized")


    def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    # ==================== Upsert Operations ====================

    def upsert_candle(self, candle: Dict[str, Any]) -> None:
        """Upsert a 1-minute candle record using ON CONFLICT.
        
        Args:
            candle: Dict with keys: timestamp, symbol, open, high, low, close,
                   volume, quote_volume, trades_count
        """
        query = """
            INSERT INTO trades_1m 
            (timestamp, symbol, open, high, low, close, volume, quote_volume, trades_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trades_count = EXCLUDED.trades_count
        """
        params = (
            candle.get('timestamp'),
            candle.get('symbol'),
            candle.get('open'),
            candle.get('high'),
            candle.get('low'),
            candle.get('close'),
            candle.get('volume'),
            candle.get('quote_volume'),
            candle.get('trades_count')
        )
        self._execute_with_retry(query, params)

    def upsert_indicators(self, indicators: Dict[str, Any]) -> None:
        """Upsert technical indicators record using ON CONFLICT.
        
        Args:
            indicators: Dict with keys: timestamp, symbol, rsi, macd, macd_signal,
                       sma_20, ema_12, ema_26, bb_upper, bb_lower, atr
        """
        query = """
            INSERT INTO indicators
            (timestamp, symbol, rsi, macd, macd_signal, sma_20, ema_12, ema_26, 
             bb_upper, bb_lower, atr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                rsi = EXCLUDED.rsi,
                macd = EXCLUDED.macd,
                macd_signal = EXCLUDED.macd_signal,
                sma_20 = EXCLUDED.sma_20,
                ema_12 = EXCLUDED.ema_12,
                ema_26 = EXCLUDED.ema_26,
                bb_upper = EXCLUDED.bb_upper,
                bb_lower = EXCLUDED.bb_lower,
                atr = EXCLUDED.atr
        """
        params = (
            indicators.get('timestamp'),
            indicators.get('symbol'),
            indicators.get('rsi'),
            indicators.get('macd'),
            indicators.get('macd_signal'),
            indicators.get('sma_20'),
            indicators.get('ema_12'),
            indicators.get('ema_26'),
            indicators.get('bb_upper'),
            indicators.get('bb_lower'),
            indicators.get('atr')
        )
        self._execute_with_retry(query, params)

    def insert_alert(self, alert: Dict[str, Any]) -> None:
        """Insert an alert record with SERIAL id.
        
        Args:
            alert: Dict with keys: timestamp, symbol, alert_type, severity,
                  message, metadata
        """
        metadata = alert.get('metadata')
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)
        
        query = """
            INSERT INTO alerts
            (timestamp, symbol, alert_type, severity, message, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            alert.get('timestamp'),
            alert.get('symbol'),
            alert.get('alert_type'),
            alert.get('severity'),
            alert.get('message'),
            metadata
        )
        self._execute_with_retry(query, params)

    def upsert_daily_stats(self, stats: Dict[str, Any]) -> None:
        """Upsert daily statistics record using ON CONFLICT.
        
        Args:
            stats: Dict with keys: date, symbol, open, high, low, close,
                  volume, volatility
        """
        query = """
            INSERT INTO daily_stats
            (date, symbol, open, high, low, close, volume, volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                volatility = EXCLUDED.volatility
        """
        params = (
            stats.get('date'),
            stats.get('symbol'),
            stats.get('open'),
            stats.get('high'),
            stats.get('low'),
            stats.get('close'),
            stats.get('volume'),
            stats.get('volatility')
        )
        self._execute_with_retry(query, params)


    # ==================== Query Methods ====================

    def query_candles(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query candles for a symbol within time range, aggregated by interval.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start: Start datetime
            end: End datetime
            interval: Candle interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
            
        Returns:
            List of candle dictionaries
        """
        # Map interval to PostgreSQL date_trunc/time bucket
        interval_minutes = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440
        }
        
        minutes = interval_minutes.get(interval, 1)
        
        if minutes == 1:
            # No aggregation needed for 1m
            query = """
                SELECT timestamp, symbol, open, high, low, close, 
                       volume, quote_volume, trades_count
                FROM trades_1m
                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC
            """
            result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        else:
            # Aggregate 1m candles into larger intervals
            # Use date_trunc for standard intervals, or custom bucketing
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
                    bucket as timestamp,
                    %s as symbol,
                    (SELECT open FROM bucketed b2 WHERE b2.bucket = b.bucket AND b2.rn_first = 1 LIMIT 1) as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    (SELECT close FROM bucketed b3 WHERE b3.bucket = b.bucket AND b3.rn_last = 1 LIMIT 1) as close,
                    SUM(volume) as volume,
                    SUM(quote_volume) as quote_volume,
                    SUM(trades_count) as trades_count
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

    def query_indicators(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Query technical indicators for a symbol within time range.
        
        Args:
            symbol: Trading pair symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of indicator dictionaries
        """
        query = """
            SELECT timestamp, symbol, rsi, macd, macd_signal, sma_20,
                   ema_12, ema_26, bb_upper, bb_lower, atr
            FROM indicators
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        return result or []

    def query_alerts(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Query alerts for a symbol within time range.
        
        Args:
            symbol: Trading pair symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of alert dictionaries
        """
        query = """
            SELECT timestamp, symbol, alert_type, severity, message, metadata
            FROM alerts
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp DESC
        """
        result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        if not result:
            return []
        
        # Parse metadata JSON if present (PostgreSQL returns dict for JSONB)
        alerts = []
        for row in result:
            alert = dict(row)
            # JSONB is already parsed by psycopg2, but handle string case
            if alert.get('metadata') and isinstance(alert['metadata'], str):
                try:
                    alert['metadata'] = json.loads(alert['metadata'])
                except (json.JSONDecodeError, TypeError):
                    pass
            alerts.append(alert)
        return alerts

    def query_daily_stats(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Query daily statistics for a symbol within date range.
        
        Args:
            symbol: Trading pair symbol
            start: Start datetime (date portion used)
            end: End datetime (date portion used)
            
        Returns:
            List of daily stats dictionaries
        """
        start_date = start.date() if isinstance(start, datetime) else start
        end_date = end.date() if isinstance(end, datetime) else end
        
        query = """
            SELECT date, symbol, open, high, low, close, volume, volatility
            FROM daily_stats
            WHERE symbol = %s AND date >= %s AND date <= %s
            ORDER BY date ASC
        """
        result = self._execute_with_retry(query, (symbol, start_date, end_date), fetch=True)
        return result or []

    # ==================== Retention Cleanup ====================

    def cleanup_old_data(self) -> Dict[str, int]:
        """Delete records older than retention periods.
        
        Retention periods:
        - trades_1m: 90 days
        - indicators: 90 days
        - alerts: 180 days
        - daily_stats: 2 years (730 days)
        
        Returns:
            Dict with count of deleted records per table
        """
        now = datetime.now()
        deleted_counts = {}
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Cleanup trades_1m (90 days)
                cutoff_trades = now - timedelta(days=self.RETENTION_TRADES_1M)
                cur.execute(
                    "DELETE FROM trades_1m WHERE timestamp < %s",
                    (cutoff_trades,)
                )
                deleted_counts['trades_1m'] = cur.rowcount
                
                # Cleanup indicators (90 days)
                cutoff_indicators = now - timedelta(days=self.RETENTION_INDICATORS)
                cur.execute(
                    "DELETE FROM indicators WHERE timestamp < %s",
                    (cutoff_indicators,)
                )
                deleted_counts['indicators'] = cur.rowcount
                
                # Cleanup alerts (180 days)
                cutoff_alerts = now - timedelta(days=self.RETENTION_ALERTS)
                cur.execute(
                    "DELETE FROM alerts WHERE timestamp < %s",
                    (cutoff_alerts,)
                )
                deleted_counts['alerts'] = cur.rowcount
                
                # Cleanup daily_stats (2 years)
                cutoff_daily = now - timedelta(days=self.RETENTION_DAILY_STATS)
                cur.execute(
                    "DELETE FROM daily_stats WHERE date < %s",
                    (cutoff_daily.date(),)
                )
                deleted_counts['daily_stats'] = cur.rowcount
        
        logger.info(f"Cleanup completed: {deleted_counts}")
        return deleted_counts
