"""Tests for storage - Redis/PostgreSQL health checks, query router tier selection, data quality."""

import os
import tempfile
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import great_expectations as gx
import pytest
from great_expectations import expectations as gxe
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from validator.aggregation_validator import build_aggregation_expectations
from validator.aggregation_validator import run_ge_validation as run_aggregation_ge_validation
from validator.anomaly_validator import build_anomaly_expectations
from validator.anomaly_validator import run_ge_validation as run_anomaly_ge_validation
from storage.postgres import Postgres
from storage.postgres import check_health as check_postgres_health
from storage.query_router import Router
from storage.redis import Redis
from storage.redis import check_health as check_redis_health


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
skip_if_no_redis = pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis server not available at localhost:6379")


class TestRedisHealthCheck:
    """Tests for Redis health check (Hot Path)."""

    @patch("redis.Redis")
    def test_redis_health_check_success(self, mock_redis_class):
        """Test successful Redis health check."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis_class.return_value = mock_client

        result = check_redis_health(host="localhost", port=6379)

        assert result["service"] == "redis"
        assert result["tier"] == "hot"
        assert result["status"] == "healthy"
        assert result["host"] == "localhost"
        assert result["port"] == 6379
        assert result["attempt"] == 1
        assert "timestamp" in result

        mock_client.ping.assert_called_once()
        mock_client.close.assert_called_once()

    @patch("redis.Redis")
    def test_redis_health_check_retry_on_failure(self, mock_redis_class):
        """Test Redis health check retries on connection failure."""
        from redis.exceptions import ConnectionError

        mock_client = Mock()
        mock_client.ping.side_effect = [
            ConnectionError("Connection refused"),
            ConnectionError("Connection refused"),
            True,
        ]
        mock_redis_class.return_value = mock_client

        result = check_redis_health(host="localhost", port=6379, max_retries=3, retry_delay=0.01)

        assert result["status"] == "healthy"
        assert result["attempt"] == 3

    @patch("redis.Redis")
    def test_redis_health_check_fails_after_max_retries(self, mock_redis_class):
        """Test Redis health check fails after max retries."""
        from redis.exceptions import ConnectionError

        mock_client = Mock()
        mock_client.ping.side_effect = ConnectionError("Connection refused")
        mock_redis_class.return_value = mock_client

        with pytest.raises(Exception) as exc_info:
            check_redis_health(host="localhost", port=6379, max_retries=2, retry_delay=0.01)

        assert "Redis health check failed after 2 attempts" in str(exc_info.value)


class TestPostgresHealthCheck:
    """Tests for PostgreSQL health check (Warm Path)."""

    @patch("psycopg2.connect")
    def test_postgres_health_check_success(self, mock_connect):
        """Test successful PostgreSQL health check."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        result = check_postgres_health(
            host="localhost", port=5432, user="crypto", password="crypto", database="crypto_data"
        )

        assert result["service"] == "postgresql"
        assert result["tier"] == "warm"
        assert result["status"] == "healthy"
        assert result["host"] == "localhost"
        assert result["port"] == 5432
        assert result["database"] == "crypto_data"
        assert result["attempt"] == 1

        mock_conn.close.assert_called_once()

    @patch("psycopg2.connect")
    def test_postgres_health_check_fails_after_max_retries(self, mock_connect):
        """Test PostgreSQL health check fails after max retries."""
        mock_connect.side_effect = Exception("Connection refused")

        with pytest.raises(Exception) as exc_info:
            check_postgres_health(host="localhost", port=5432, max_retries=2, retry_delay=0.01)

        assert "PostgreSQL health check failed after 2 attempts" in str(exc_info.value)


# Strategy for generating time offsets in minutes (for Redis tier: < 60 minutes)
redis_offset_minutes = st.integers(min_value=0, max_value=59)

# Strategy for generating time offsets in hours (for PostgreSQL tier: >= 1 hour)
postgres_offset_hours = st.integers(min_value=1, max_value=2159)


@pytest.fixture
def mock_redis():
    """Create mock Redis."""
    mock = MagicMock()
    mock.get_aggregation.return_value = None
    mock.get_recent_trades.return_value = []
    mock.get_recent_alerts.return_value = []
    return mock


@pytest.fixture
def mock_postgres():
    """Create mock Postgres."""
    mock = MagicMock()
    mock.query_klines.return_value = []
    mock.query_alerts.return_value = []
    return mock


@pytest.fixture
def query_router(mock_redis, mock_postgres):
    """Create Router with mock storage instances."""
    return Router(
        redis=mock_redis,
        postgres=mock_postgres,
    )


class TestQueryRoutingCorrectness:
    """
    Property tests for query routing correctness.

    Feature: storage-tier-migration, Property 5: Query Interface Compatibility
    Validates: Requirements 4.1, 4.2
    """

    TIER_REDIS = "redis"
    TIER_POSTGRES = "postgres"

    @given(offset_minutes=redis_offset_minutes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_tier_selection_for_recent_queries(self, query_router, offset_minutes):
        """For any query within last 1 hour, the router should select Redis tier."""
        now = datetime.now(UTC)
        start = now - timedelta(minutes=offset_minutes)

        selected_tier = query_router.select_tier(start)

        assert selected_tier == self.TIER_REDIS, f"Expected Redis for {offset_minutes} minutes ago, got {selected_tier}"

    @given(offset_hours=postgres_offset_hours)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_postgres_tier_selection_for_historical_queries(self, query_router, offset_hours):
        """For any query > 1 hour, the router should select PostgreSQL tier."""
        now = datetime.now(UTC)
        start = now - timedelta(hours=offset_hours)

        selected_tier = query_router.select_tier(start)

        assert selected_tier == self.TIER_POSTGRES, (
            f"Expected PostgreSQL for {offset_hours} hours ago, got {selected_tier}"
        )

    def test_boundary_exactly_1_hour(self, query_router):
        """Test boundary: exactly 1 hour ago uses PostgreSQL (start <= now - 1h)."""
        now = datetime.now(UTC)
        start = now - timedelta(hours=1)

        selected_tier = query_router.select_tier(start)

        assert selected_tier == self.TIER_POSTGRES

    def test_boundary_just_under_1_hour(self, query_router):
        """Test boundary: just under 1 hour ago uses Redis (start > now - 1h)."""
        now = datetime.now(UTC)
        start = now - timedelta(minutes=59)

        selected_tier = query_router.select_tier(start)

        assert selected_tier == self.TIER_REDIS


# =============================================================================
# Data Quality Tests using Great Expectations
# =============================================================================

# Valid symbols for testing
VALID_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "SOLUSDT",
    "DOTUSDT",
    "MATICUSDT",
    "LTCUSDT",
]


def test_ohlcv_data_quality_valid():
    """Test that valid OHLCV data passes all GE expectations."""
    ohlcv_records = [
        {"symbol": "BTCUSDT", "open": 50000.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": 1000.0},
        {"symbol": "ETHUSDT", "open": 3000.0, "high": 3100.0, "low": 2900.0, "close": 3050.0, "volume": 500.0},
    ]

    expectations = build_aggregation_expectations()
    result = run_aggregation_ge_validation(ohlcv_records, expectations)

    assert result.success, (
        f"Valid OHLCV should pass. Failed: {[r.expectation_config.type for r in result.results if not r.success]}"
    )


def test_ohlcv_high_less_than_low_fails():
    """Test that OHLCV with high < low fails validation."""
    invalid_ohlcv = [
        {"symbol": "BTCUSDT", "open": 50000.0, "high": 48000.0, "low": 49000.0, "close": 50500.0, "volume": 1000.0},
        # high (48000) < low (49000) - invalid!
    ]

    expectations = build_aggregation_expectations()
    result = run_aggregation_ge_validation(invalid_ohlcv, expectations)

    assert not result.success, "OHLCV with high < low should fail"


def test_ohlcv_high_less_than_open_fails():
    """Test that OHLCV with high < open fails validation."""
    invalid_ohlcv = [
        {"symbol": "BTCUSDT", "open": 50000.0, "high": 49000.0, "low": 48000.0, "close": 48500.0, "volume": 1000.0},
        # high (49000) < open (50000) - invalid!
    ]

    expectations = build_aggregation_expectations()
    result = run_aggregation_ge_validation(invalid_ohlcv, expectations)

    assert not result.success, "OHLCV with high < open should fail"


def test_ohlcv_high_less_than_close_fails():
    """Test that OHLCV with high < close fails validation."""
    invalid_ohlcv = [
        {"symbol": "BTCUSDT", "open": 48000.0, "high": 49000.0, "low": 47000.0, "close": 50000.0, "volume": 1000.0},
        # high (49000) < close (50000) - invalid!
    ]

    expectations = build_aggregation_expectations()
    result = run_aggregation_ge_validation(invalid_ohlcv, expectations)

    assert not result.success, "OHLCV with high < close should fail"


def test_ohlcv_negative_price_fails():
    """Test that OHLCV with negative prices fails validation."""
    invalid_ohlcv = [
        {"symbol": "BTCUSDT", "open": -100.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": 1000.0},
    ]

    expectations = build_aggregation_expectations()
    result = run_aggregation_ge_validation(invalid_ohlcv, expectations)

    assert not result.success, "OHLCV with negative price should fail"


def test_ohlcv_negative_volume_fails():
    """Test that OHLCV with negative volume fails validation."""
    invalid_ohlcv = [
        {"symbol": "BTCUSDT", "open": 50000.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": -100.0},
    ]

    expectations = build_aggregation_expectations()
    result = run_aggregation_ge_validation(invalid_ohlcv, expectations)

    assert not result.success, "OHLCV with negative volume should fail"


def test_ohlcv_null_symbol_fails():
    """Test that OHLCV with null symbol fails validation."""
    invalid_ohlcv = [
        {"symbol": None, "open": 50000.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": 1000.0},
    ]

    expectations = build_aggregation_expectations()
    result = run_aggregation_ge_validation(invalid_ohlcv, expectations)

    assert not result.success, "OHLCV with null symbol should fail"


def test_alert_data_quality_valid():
    """Test that valid alert data passes all GE expectations."""
    import uuid
    from datetime import timezone

    alerts = [
        {
            "alert_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "symbol": "BTCUSDT",
            "alert_type": "VOLUME_SPIKE",
            "alert_level": "HIGH",
            "created_at": datetime.now(UTC).isoformat(),
        },
        {
            "alert_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "symbol": "ETHUSDT",
            "alert_type": "PRICE_SPIKE",
            "alert_level": "MEDIUM",
            "created_at": datetime.now(UTC).isoformat(),
        },
    ]

    expectations = build_anomaly_expectations()
    result = run_anomaly_ge_validation(alerts, expectations)

    assert result.success, (
        f"Valid alerts should pass. Failed: {[r.expectation_config.type for r in result.results if not r.success]}"
    )


def test_alert_invalid_type_fails():
    """Test that alert with invalid alert_type fails validation."""
    import uuid
    from datetime import timezone

    invalid_alerts = [
        {
            "alert_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "symbol": "BTCUSDT",
            "alert_type": "UNKNOWN_TYPE",  # Not in VALID_ALERT_TYPES
            "alert_level": "HIGH",
            "created_at": datetime.now(UTC).isoformat(),
        },
    ]

    expectations = build_anomaly_expectations()
    result = run_anomaly_ge_validation(invalid_alerts, expectations)

    assert not result.success, "Alert with invalid alert_type should fail"


def test_alert_invalid_level_fails():
    """Test that alert with invalid alert_level fails validation."""
    import uuid
    from datetime import timezone

    invalid_alerts = [
        {
            "alert_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "symbol": "BTCUSDT",
            "alert_type": "VOLUME_SPIKE",
            "alert_level": "CRITICAL",  # Not in VALID_ALERT_LEVELS (should be HIGH/MEDIUM/LOW)
            "created_at": datetime.now(UTC).isoformat(),
        },
    ]

    expectations = build_anomaly_expectations()
    result = run_anomaly_ge_validation(invalid_alerts, expectations)

    assert not result.success, "Alert with invalid alert_level should fail"


def test_alert_null_symbol_fails():
    """Test that alert with null symbol fails validation."""
    import uuid
    from datetime import timezone

    invalid_alerts = [
        {
            "alert_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "symbol": None,  # Null symbol
            "alert_type": "VOLUME_SPIKE",
            "alert_level": "HIGH",
            "created_at": datetime.now(UTC).isoformat(),
        },
    ]

    expectations = build_anomaly_expectations()
    result = run_anomaly_ge_validation(invalid_alerts, expectations)

    assert not result.success, "Alert with null symbol should fail"


# Build custom expectations for trade data
def build_trade_expectations():
    """Build list of expectations for trade data."""
    return [
        gxe.ExpectColumnToExist(column="price"),
        gxe.ExpectColumnToExist(column="quantity"),
        gxe.ExpectColumnToExist(column="timestamp"),
        gxe.ExpectColumnValuesToNotBeNull(column="price"),
        gxe.ExpectColumnValuesToNotBeNull(column="quantity"),
        gxe.ExpectColumnValuesToNotBeNull(column="timestamp"),
        gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=0),
    ]


def run_trade_ge_validation(records, expectations):
    """Run GE validation on trade records."""
    import pandas as pd

    df = pd.DataFrame(records)
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas("trade_source")
    data_asset = data_source.add_dataframe_asset(name="trade_data")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("trade_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name="trade_suite")
    for exp in expectations:
        suite.add_expectation(exp)

    return batch.validate(suite)


def test_trade_data_quality_valid():
    """Test that valid trade data passes all GE expectations."""
    trades = [
        {"price": 50000.0, "quantity": 0.5, "timestamp": 1704067200000, "is_buyer_maker": True},
        {"price": 50100.0, "quantity": 1.0, "timestamp": 1704067260000, "is_buyer_maker": False},
    ]

    expectations = build_trade_expectations()
    result = run_trade_ge_validation(trades, expectations)

    assert result.success, (
        f"Valid trades should pass. Failed: {[r.expectation_config.type for r in result.results if not r.success]}"
    )


def test_trade_negative_price_fails():
    """Test that trade with negative price fails validation."""
    invalid_trades = [
        {"price": -100.0, "quantity": 0.5, "timestamp": 1704067200000, "is_buyer_maker": True},
    ]

    expectations = build_trade_expectations()
    result = run_trade_ge_validation(invalid_trades, expectations)

    assert not result.success, "Trade with negative price should fail"


def test_trade_negative_quantity_fails():
    """Test that trade with negative quantity fails validation."""
    invalid_trades = [
        {"price": 50000.0, "quantity": -1.0, "timestamp": 1704067200000, "is_buyer_maker": True},
    ]

    expectations = build_trade_expectations()
    result = run_trade_ge_validation(invalid_trades, expectations)

    assert not result.success, "Trade with negative quantity should fail"


def test_trade_null_price_fails():
    """Test that trade with null price fails validation."""
    invalid_trades = [
        {"price": None, "quantity": 0.5, "timestamp": 1704067200000, "is_buyer_maker": True},
    ]

    expectations = build_trade_expectations()
    result = run_trade_ge_validation(invalid_trades, expectations)

    assert not result.success, "Trade with null price should fail"
