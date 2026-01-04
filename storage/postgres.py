"""PostgreSQL storage module for warm path data access."""

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from utils.logging import get_logger
from utils.retry import RetryConfig, retry_operation
from utils.metrics import track_latency, record_error, record_retry

logger = get_logger(__name__)


class PostgresStorage:
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
                        trade_count INTEGER,
                        buy_count INTEGER,
                        sell_count INTEGER,
                        volume_weighted_avg_price DOUBLE PRECISION,
                        price_change_percent DOUBLE PRECISION,
                        buy_sell_ratio DOUBLE PRECISION,
                        average_price DOUBLE PRECISION,
                        price_volatility DOUBLE PRECISION,
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
                        metadata JSONB,
                        UNIQUE (timestamp, symbol, alert_type)
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_alerts_ts 
                    ON alerts(timestamp DESC, symbol)
                """)
        logger.debug("PostgreSQL tables initialized")

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            logger.info("PostgreSQL connection pool closed")


    def upsert_candle(self, candle: Dict[str, Any]) -> None:
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
        self._execute_with_retry(query, params)

    def upsert_indicators(self, indicators: Dict[str, Any]) -> None:
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

    def upsert_candles_batch(self, candles: List[Dict[str, Any]]) -> int:
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
        """Batch insert alerts using executemany."""
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
            ON CONFLICT (timestamp, symbol, alert_type) DO UPDATE SET
                severity = EXCLUDED.severity,
                message = EXCLUDED.message,
                metadata = EXCLUDED.metadata
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

    def query_candles(
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
        result = self._execute_with_retry(query, (symbol, start, end), fetch=True)
        return result or []

    def query_candles_aggregated(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime, 
        interval: str = "5m"
    ) -> List[Dict[str, Any]]:
        if interval == "1m":
            return self.query_candles(symbol, start, end)
        
        valid_intervals = {"5m", "15m"}
        if interval not in valid_intervals:
            raise ValueError(f"Invalid interval: {interval}. Must be one of {valid_intervals | {'1m'}}")
        
        interval_minutes = int(interval.replace("m", ""))
        
        query = """
            SELECT 
                date_trunc('hour', timestamp) + 
                    INTERVAL '1 minute' * (EXTRACT(MINUTE FROM timestamp)::int / %s * %s) as timestamp,
                %s as symbol,
                (array_agg(open ORDER BY timestamp ASC))[1] as open,
                MAX(high) as high,
                MIN(low) as low,
                (array_agg(close ORDER BY timestamp DESC))[1] as close,
                SUM(volume) as volume,
                SUM(quote_volume) as quote_volume,
                SUM(trade_count) as trade_count,
                SUM(buy_count) as buy_count,
                SUM(sell_count) as sell_count,
                SUM(quote_volume) / NULLIF(SUM(volume), 0) as volume_weighted_avg_price,
                ((array_agg(close ORDER BY timestamp DESC))[1] - (array_agg(open ORDER BY timestamp ASC))[1]) 
                    / NULLIF((array_agg(open ORDER BY timestamp ASC))[1], 0) * 100 as price_change_percent,
                SUM(buy_count)::float / NULLIF(SUM(sell_count), 0) as buy_sell_ratio,
                AVG(average_price) as average_price,
                AVG(price_volatility) as price_volatility
            FROM trades_1m
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY 1
            ORDER BY 1 ASC
        """
        
        params = (
            interval_minutes, interval_minutes,
            symbol,
            symbol, start, end
        )
        
        result = self._execute_with_retry(query, params, fetch=True)
        return result or []

    def query_indicators(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
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
        interval_map = {
            "1m": "minute",
            "1h": "hour",
            "1d": "day",
        }
        
        trunc_interval = interval_map.get(interval, "hour")
        
        query = """
            SELECT 
                date_trunc(%s, timestamp) AS bucket_timestamp,
                SUM(trade_count) AS total_trade_count
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
                "trade_count": int(row["total_trade_count"] or 0),
                "interval": interval,
            }
            for row in result
        ]

    def cleanup_table(
        self,
        table_name: str,
        retention_days: int,
        batch_size: int = 1000
    ) -> int:
        """Delete records older than retention period from a table."""
        # Validate table name to prevent SQL injection
        valid_tables = {"trades_1m", "indicators", "alerts"}
        if table_name not in valid_tables:
            raise ValueError(f"Invalid table name: {table_name}. Must be one of {valid_tables}")
        
        if retention_days <= 0:
            raise ValueError(f"retention_days must be positive, got {retention_days}")
        
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        
        total_deleted = 0
        
        # Use ctid for efficient batch deletion
        # ctid is PostgreSQL's internal row identifier
        if table_name == "alerts":
            # alerts table uses 'id' as primary key
            delete_query = f"""
                DELETE FROM {table_name}
                WHERE id IN (
                    SELECT id FROM {table_name}
                    WHERE timestamp < NOW() - INTERVAL '%s days'
                    LIMIT %s
                )
            """
        else:
            # trades_1m and indicators use (symbol, timestamp) as primary key
            delete_query = f"""
                DELETE FROM {table_name}
                WHERE ctid IN (
                    SELECT ctid FROM {table_name}
                    WHERE timestamp < NOW() - INTERVAL '%s days'
                    LIMIT %s
                )
            """
        
        def delete_batch():
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(delete_query, (retention_days, batch_size))
                    deleted = cur.rowcount
                    conn.commit()
                    return deleted
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("postgres_storage", f"cleanup_{table_name}", "failed")
        
        try:
            with track_latency("postgres_storage", f"cleanup_{table_name}"):
                # Keep deleting batches until no more records to delete
                while True:
                    deleted = retry_operation(
                        delete_batch,
                        config=self._retry_config,
                        operation_name=f"PostgreSQL cleanup {table_name}",
                        on_retry=on_retry,
                    )
                    total_deleted += deleted
                    
                    if deleted < batch_size:
                        # No more records to delete
                        break
                    
                    logger.debug(f"Deleted batch of {deleted} records from {table_name}, total: {total_deleted}")
            
            record_retry("postgres_storage", f"cleanup_{table_name}", "success")
            logger.info(f"Cleanup completed for {table_name}: {total_deleted} records deleted (retention: {retention_days} days)")
            return total_deleted
            
        except Exception as e:
            record_error("postgres_storage", f"cleanup_{table_name}_error", "error")
            logger.error(f"Failed to cleanup {table_name}: {e}")
            raise

    def cleanup_all_tables(self, retention_days: int, batch_size: int = 1000) -> Dict[str, int]:
        """Cleanup all tables by deleting records older than retention period."""
        tables = ["trades_1m", "indicators", "alerts"]
        results: Dict[str, int] = {}
        
        for table in tables:
            try:
                deleted = self.cleanup_table(table, retention_days, batch_size)
                results[table] = deleted
            except Exception as e:
                logger.error(f"Failed to cleanup table {table}: {e}")
                results[table] = 0
                # Continue with other tables even if one fails
        
        total = sum(results.values())
        logger.info(f"Cleanup all tables completed: {total} total records deleted across {len(tables)} tables")
        return results


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
