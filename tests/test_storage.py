"""
Consolidated test module for Storage Layer.
Contains all tests for Redis, PostgreSQL, MinIO storage, health checks, query router, and storage writer.

Table of Contents:
- Imports and Setup (line ~20)
- Health Check Tests (line ~80)
- Query Router Property Tests (line ~250)
- Redis Storage Property Tests (line ~400)
- Storage Writer Property Tests (line ~600)

Requirements: 6.3
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from hypothesis import given, settings, strategies as st, assume, HealthCheck

from src.storage.redis import RedisStorage, check_redis_health
from src.storage.postgres import PostgresStorage, check_postgres_health
from src.storage.minio import MinioStorage, check_minio_health
from src.storage.query_router import QueryRouter
from src.storage.storage_writer import StorageWriter


# ============================================================================
# REDIS AVAILABILITY CHECK
# ============================================================================

def is_redis_available():
    """Check if Redis is available for testing."""
    try:
        import redis
        client = redis.Redis(host="localhost", port=6379, db=15, socket_connect_timeout=1)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


REDIS_AVAILABLE = is_redis_available()
skip_if_no_redis = pytest.mark.skipif(
    not REDIS_AVAILABLE,
    reason="Redis server not available at localhost:6379"
)


# ============================================================================
# REDIS HEALTH CHECK TESTS
# ============================================================================

class TestRedisHealthCheck:
    """Tests for Redis health check (Hot Path)."""
    
    @patch('redis.Redis')
    def test_redis_health_check_success(self, mock_redis_class):
        """Test successful Redis health check."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis_class.return_value = mock_client
        
        result = check_redis_health(host='localhost', port=6379)
        
        assert result['service'] == 'redis'
        assert result['tier'] == 'hot'
        assert result['status'] == 'healthy'
        assert result['host'] == 'localhost'
        assert result['port'] == 6379
        assert result['attempt'] == 1
        assert 'timestamp' in result
        
        mock_client.ping.assert_called_once()
        mock_client.close.assert_called_once()
    
    @patch('redis.Redis')
    def test_redis_health_check_retry_on_failure(self, mock_redis_class):
        """Test Redis health check retries on connection failure."""
        from redis.exceptions import ConnectionError
        
        mock_client = Mock()
        mock_client.ping.side_effect = [
            ConnectionError("Connection refused"),
            ConnectionError("Connection refused"),
            True
        ]
        mock_redis_class.return_value = mock_client
        
        result = check_redis_health(
            host='localhost', 
            port=6379, 
            max_retries=3,
            retry_delay=0.01
        )
        
        assert result['status'] == 'healthy'
        assert result['attempt'] == 3
    
    @patch('redis.Redis')
    def test_redis_health_check_fails_after_max_retries(self, mock_redis_class):
        """Test Redis health check fails after max retries."""
        from redis.exceptions import ConnectionError
        
        mock_client = Mock()
        mock_client.ping.side_effect = ConnectionError("Connection refused")
        mock_redis_class.return_value = mock_client
        
        with pytest.raises(Exception) as exc_info:
            check_redis_health(
                host='localhost', 
                port=6379, 
                max_retries=2,
                retry_delay=0.01
            )
        
        assert "Redis health check failed after 2 attempts" in str(exc_info.value)


# ============================================================================
# POSTGRESQL HEALTH CHECK TESTS
# ============================================================================

class TestPostgresHealthCheck:
    """Tests for PostgreSQL health check (Warm Path)."""
    
    @patch('psycopg2.connect')
    def test_postgres_health_check_success(self, mock_connect):
        """Test successful PostgreSQL health check."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn
        
        result = check_postgres_health(
            host='localhost',
            port=5432,
            user='crypto',
            password='crypto',
            database='crypto_data'
        )
        
        assert result['service'] == 'postgresql'
        assert result['tier'] == 'warm'
        assert result['status'] == 'healthy'
        assert result['host'] == 'localhost'
        assert result['port'] == 5432
        assert result['database'] == 'crypto_data'
        assert result['attempt'] == 1
        
        mock_conn.close.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_postgres_health_check_fails_after_max_retries(self, mock_connect):
        """Test PostgreSQL health check fails after max retries."""
        mock_connect.side_effect = Exception("Connection refused")
        
        with pytest.raises(Exception) as exc_info:
            check_postgres_health(
                host='localhost',
                port=5432,
                max_retries=2,
                retry_delay=0.01
            )
        
        assert "PostgreSQL health check failed after 2 attempts" in str(exc_info.value)


# ============================================================================
# MINIO HEALTH CHECK TESTS
# ============================================================================

class TestMinioHealthCheck:
    """Tests for MinIO health check (Cold Path)."""
    
    @patch('src.storage.minio.Minio')
    def test_minio_health_check_success_bucket_exists(self, mock_minio_class):
        """Test successful MinIO health check when bucket exists."""
        mock_client = Mock()
        mock_client.list_buckets.return_value = []
        mock_client.bucket_exists.return_value = True
        mock_minio_class.return_value = mock_client
        
        result = check_minio_health(
            endpoint='localhost:9000',
            access_key='minioadmin',
            secret_key='minioadmin',
            bucket='crypto-data'
        )
        
        assert result['service'] == 'minio'
        assert result['tier'] == 'cold'
        assert result['status'] == 'healthy'
        assert result['endpoint'] == 'localhost:9000'
        assert result['bucket'] == 'crypto-data'
        assert result['attempt'] == 1
        
        mock_client.list_buckets.assert_called_once()
        mock_client.bucket_exists.assert_called_once_with('crypto-data')
        mock_client.make_bucket.assert_not_called()
    
    @patch('src.storage.minio.Minio')
    def test_minio_health_check_creates_bucket_if_not_exists(self, mock_minio_class):
        """Test MinIO health check creates bucket if it doesn't exist."""
        mock_client = Mock()
        mock_client.list_buckets.return_value = []
        mock_client.bucket_exists.return_value = False
        mock_minio_class.return_value = mock_client
        
        result = check_minio_health(
            endpoint='localhost:9000',
            bucket='new-bucket'
        )
        
        assert result['status'] == 'healthy'
        mock_client.make_bucket.assert_called_once_with('new-bucket')
    
    @patch('src.storage.minio.Minio')
    def test_minio_health_check_fails_after_max_retries(self, mock_minio_class):
        """Test MinIO health check fails after max retries."""
        mock_client = Mock()
        mock_client.list_buckets.side_effect = Exception("Connection refused")
        mock_minio_class.return_value = mock_client
        
        with pytest.raises(Exception) as exc_info:
            check_minio_health(
                endpoint='localhost:9000',
                max_retries=2,
                retry_delay=0.01
            )
        
        assert "MinIO health check failed after 2 attempts" in str(exc_info.value)


# ============================================================================
# QUERY ROUTER FIXTURES AND STRATEGIES
# ============================================================================

# Strategy for generating time offsets in minutes (for Redis tier: < 60 minutes)
redis_offset_minutes = st.integers(min_value=0, max_value=59)

# Strategy for generating time offsets in hours (for PostgreSQL tier: 1 hour to < 90 days)
postgres_offset_hours = st.integers(min_value=1, max_value=2159)

# Strategy for generating time offsets in days (for MinIO tier: >= 90 days)
minio_offset_days = st.integers(min_value=90, max_value=365 * 5)


@pytest.fixture
def mock_redis():
    """Create mock RedisStorage."""
    mock = MagicMock()
    mock.get_indicators.return_value = None
    mock.get_aggregation.return_value = None
    mock.get_recent_trades.return_value = []
    mock.get_recent_alerts.return_value = []
    return mock


@pytest.fixture
def mock_postgres():
    """Create mock PostgresStorage."""
    mock = MagicMock()
    mock.query_indicators.return_value = []
    mock.query_candles.return_value = []
    mock.query_alerts.return_value = []
    return mock


@pytest.fixture
def mock_minio():
    """Create mock MinioStorage."""
    mock = MagicMock()
    mock.read_indicators.return_value = []
    mock.read_klines.return_value = []
    mock.read_alerts.return_value = []
    return mock


@pytest.fixture
def query_router(mock_redis, mock_postgres, mock_minio):
    """Create QueryRouter with mock storage instances."""
    return QueryRouter(
        redis=mock_redis,
        postgres=mock_postgres,
        minio=mock_minio,
    )


# ============================================================================
# QUERY ROUTER PROPERTY TESTS
# ============================================================================

class TestQueryRoutingCorrectness:
    """
    Property tests for query routing correctness.
    
    Feature: storage-tier-migration, Property 5: Query Interface Compatibility
    Validates: Requirements 4.1, 4.2
    """
    
    # Class constants for tier names
    TIER_REDIS = "redis"
    TIER_POSTGRES = "postgres"
    TIER_MINIO = "minio"
    
    @given(offset_minutes=redis_offset_minutes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_tier_selection_for_recent_queries(self, query_router, offset_minutes):
        """For any query within last 1 hour, the router should select Redis tier."""
        now = datetime.now()
        start = now - timedelta(minutes=offset_minutes)
        
        selected_tier = query_router._select_tier(start)
        
        assert selected_tier == self.TIER_REDIS, \
            f"Expected Redis for {offset_minutes} minutes ago, got {selected_tier}"
    
    @given(offset_hours=postgres_offset_hours)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_postgres_tier_selection_for_medium_range_queries(self, query_router, offset_hours):
        """For any query between 1 hour and 90 days, the router should select PostgreSQL tier."""
        now = datetime.now()
        start = now - timedelta(hours=offset_hours)
        
        selected_tier = query_router._select_tier(start)
        
        assert selected_tier == self.TIER_POSTGRES, \
            f"Expected PostgreSQL for {offset_hours} hours ago, got {selected_tier}"
    
    @given(offset_days=minio_offset_days)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_minio_tier_selection_for_historical_queries(self, query_router, offset_days):
        """For any query >= 90 days, the router should select MinIO tier."""
        now = datetime.now()
        start = now - timedelta(days=offset_days)
        
        selected_tier = query_router._select_tier(start)
        
        assert selected_tier == self.TIER_MINIO, \
            f"Expected MinIO for {offset_days} days ago, got {selected_tier}"
    
    def test_boundary_exactly_1_hour(self, query_router):
        """Test boundary condition: exactly 1 hour ago should use PostgreSQL."""
        now = datetime.now()
        start = now - timedelta(hours=1)
        
        selected_tier = query_router._select_tier(start)
        
        assert selected_tier == self.TIER_POSTGRES
    
    def test_boundary_exactly_90_days(self, query_router):
        """Test boundary condition: exactly 90 days ago should use MinIO."""
        now = datetime.now()
        start = now - timedelta(days=90)
        
        selected_tier = query_router._select_tier(start)
        
        assert selected_tier == self.TIER_MINIO


# ============================================================================
# REDIS STORAGE STRATEGIES
# ============================================================================

symbol_strategy = st.sampled_from([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"
])

price_strategy = st.floats(min_value=0.00001, max_value=1000000.0, allow_nan=False, allow_infinity=False)
volume_strategy = st.floats(min_value=0.0, max_value=1000000000.0, allow_nan=False, allow_infinity=False)
timestamp_strategy = st.integers(min_value=1600000000000, max_value=2000000000000)
interval_strategy = st.sampled_from(["1m", "5m", "15m", "1h", "4h", "1d"])

ticker_stats_strategy = st.fixed_dictionaries({
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "quote_volume": volume_strategy,
})

indicators_strategy = st.fixed_dictionaries({
    "rsi": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    "macd": st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
    "macd_signal": st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
    "sma_20": price_strategy,
    "ema_12": price_strategy,
    "ema_26": price_strategy,
    "bb_upper": price_strategy,
    "bb_lower": price_strategy,
    "atr": st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
})

ohlcv_strategy = st.fixed_dictionaries({
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "timestamp": timestamp_strategy,
})

trade_strategy = st.fixed_dictionaries({
    "price": price_strategy,
    "quantity": volume_strategy,
    "timestamp": timestamp_strategy,
    "is_buyer_maker": st.booleans(),
})

alert_strategy = st.fixed_dictionaries({
    "timestamp": timestamp_strategy,
    "symbol": symbol_strategy,
    "alert_type": st.sampled_from(["price_spike", "volume_spike", "whale_alert", "volatility"]),
    "severity": st.sampled_from(["low", "medium", "high", "critical"]),
    "message": st.text(min_size=1, max_size=200),
})


@pytest.fixture
def redis_storage():
    """Create RedisStorage instance and clean up after test."""
    import redis as redis_lib
    storage = RedisStorage(host="localhost", port=6379, db=15)
    # Use client.flushdb() directly since flush_db method was removed
    storage.client.flushdb()
    yield storage
    storage.client.flushdb()


# ============================================================================
# REDIS STORAGE PROPERTY TESTS
# ============================================================================

@skip_if_no_redis
class TestRedisHashRoundTrip:
    """Property tests for Redis hash round-trip consistency."""
    
    @given(symbol=symbol_strategy, indicators=indicators_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_indicators_round_trip(self, redis_storage, symbol, indicators):
        """
        Feature: three-tier-storage, Property 1: Redis hash round-trip consistency
        Validates: Requirements 1.4
        """
        redis_storage.write_indicators(symbol, indicators)
        
        result = redis_storage.get_indicators(symbol)
        
        assert result is not None
        for key in indicators:
            assert abs(result[key] - indicators[key]) < 1e-9





# ============================================================================
# STORAGE WRITER STRATEGIES
# ============================================================================

trades_count_strategy = st.integers(min_value=0, max_value=1000000)
rsi_strategy = st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)
macd_strategy = st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False)
atr_strategy = st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False)


def recent_timestamp_strategy():
    """Generate timestamps within the last 30 days as Unix milliseconds."""
    now = datetime.now()
    return st.integers(
        min_value=int((now - timedelta(days=30)).timestamp() * 1000),
        max_value=int(now.timestamp() * 1000)
    )


aggregation_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "interval": interval_strategy,
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "quote_volume": volume_strategy,
    "trades_count": trades_count_strategy,
})

indicators_data_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "rsi": rsi_strategy,
    "macd": macd_strategy,
    "macd_signal": macd_strategy,
    "sma_20": price_strategy,
    "ema_12": price_strategy,
    "ema_26": price_strategy,
    "bb_upper": price_strategy,
    "bb_lower": price_strategy,
    "atr": atr_strategy,
})

alert_data_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "alert_type": st.sampled_from(["price_spike", "volume_spike", "whale_alert", "volatility"]),
    "alert_level": st.sampled_from(["low", "medium", "high", "critical"]),
    "details": st.none() | st.fixed_dictionaries({
        "threshold": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        "actual": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    }),
})


# ============================================================================
# STORAGE WRITER PROPERTY TESTS
# ============================================================================

@skip_if_no_redis
class TestPartialFailureResilience:
    """Property tests for partial failure resilience using mocks."""
    
    @given(aggregation=aggregation_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=50, deadline=None)
    def test_aggregation_redis_failure_continues_to_others(self, aggregation, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        
        For any write operation where Redis fails, PostgreSQL and MinIO
        should still receive the data successfully.
        """
        mock_postgres = MagicMock(spec=PostgresStorage)
        mock_postgres.upsert_candle.return_value = None
        
        mock_minio = MagicMock(spec=MinioStorage)
        mock_minio.write_klines.return_value = True
        
        mock_redis = MagicMock(spec=RedisStorage)
        mock_redis.write_aggregation.side_effect = Exception("Redis connection failed")
        
        writer = StorageWriter(
            redis=mock_redis,
            postgres=mock_postgres,
            minio=mock_minio
        )
        
        data = {**aggregation, "timestamp": timestamp}
        
        results = writer.write_aggregation(data)
        
        assert results["redis"] is False, "Redis should fail"
        assert results["warm"] is True, "Warm path (PostgreSQL) should succeed despite Redis failure"
        assert results["cold"] is True, "Cold path (MinIO) should succeed despite Redis failure"
        
        mock_postgres.upsert_candle.assert_called_once()
        mock_minio.write_klines.assert_called_once()
    
    @given(aggregation=aggregation_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=50, deadline=None)
    def test_aggregation_postgres_failure_continues_to_others(self, aggregation, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        """
        redis_storage = RedisStorage(host="localhost", port=6379, db=15)
        redis_storage.client.flushdb()
        
        try:
            mock_postgres = MagicMock(spec=PostgresStorage)
            mock_postgres.upsert_candle.side_effect = Exception("PostgreSQL write failed")
            
            mock_minio = MagicMock(spec=MinioStorage)
            mock_minio.write_klines.return_value = True
            
            writer = StorageWriter(
                redis=redis_storage,
                postgres=mock_postgres,
                minio=mock_minio
            )
            
            data = {**aggregation, "timestamp": timestamp}
            symbol = data["symbol"]
            interval = data["interval"]
            
            results = writer.write_aggregation(data)
            
            assert results["redis"] is True, "Redis should succeed despite PostgreSQL failure"
            assert results["warm"] is False, "Warm path (PostgreSQL) should fail"
            assert results["cold"] is True, "Cold path (MinIO) should succeed despite PostgreSQL failure"
            
            redis_data = redis_storage.get_aggregation(symbol, interval)
            assert redis_data is not None, "Data should be in Redis"
        finally:
            redis_storage.client.flushdb()
    
    @given(aggregation=aggregation_strategy, timestamp=recent_timestamp_strategy())
    @settings(max_examples=50, deadline=None)
    def test_aggregation_minio_failure_continues_to_others(self, aggregation, timestamp):
        """
        Feature: three-tier-storage, Property 8: Partial failure resilience
        Validates: Requirements 5.4
        """
        redis_storage = RedisStorage(host="localhost", port=6379, db=15)
        redis_storage.client.flushdb()
        
        try:
            mock_postgres = MagicMock(spec=PostgresStorage)
            mock_postgres.upsert_candle.return_value = None
            
            mock_minio = MagicMock(spec=MinioStorage)
            mock_minio.write_klines.side_effect = Exception("MinIO write failed")
            
            writer = StorageWriter(
                redis=redis_storage,
                postgres=mock_postgres,
                minio=mock_minio
            )
            
            data = {**aggregation, "timestamp": timestamp}
            symbol = data["symbol"]
            interval = data["interval"]
            
            results = writer.write_aggregation(data)
            
            assert results["redis"] is True, "Redis should succeed despite MinIO failure"
            assert results["warm"] is True, "Warm path (PostgreSQL) should succeed despite MinIO failure"
            assert results["cold"] is False, "Cold path (MinIO) should fail"
            
            redis_data = redis_storage.get_aggregation(symbol, interval)
            assert redis_data is not None, "Data should be in Redis"
            
            mock_postgres.upsert_candle.assert_called_once()
        finally:
            redis_storage.client.flushdb()
