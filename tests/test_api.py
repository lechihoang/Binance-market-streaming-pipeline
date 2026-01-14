"""
Consolidated test module for FastAPI Backend.
Contains all tests for market, analytics, alerts, rate limiting, fallback, system endpoints, and data quality.

Table of Contents:
- Imports and Setup (line ~20)
- Data Quality Tests with Great Expectations (line ~150)
- Market Router Tests (line ~250)
- Rate Limit Tests (line ~350)
- Fallback Chain Tests (line ~450)
- System Router Tests (line ~550)

Requirements: 6.4
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import great_expectations as gx
import pytest
from fastapi.testclient import TestClient
from great_expectations import expectations as gxe
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from api.app import (
    RATE_LIMIT_PER_MINUTE,
    app,
    determine_overall_status,
    get_postgres,
    get_query_router,
    get_redis,
    get_ticker_storage,
    rate_tracker,
)
from processing.validators.aggregation_validator import build_aggregation_expectations
from processing.validators.aggregation_validator import run_ge_validation as run_aggregation_ge_validation
from processing.validators.anomaly_validator import build_anomaly_expectations
from processing.validators.anomaly_validator import run_ge_validation as run_anomaly_ge_validation
from storage.postgres import Postgres
from storage.query_router import Router
from storage.redis import Redis


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


def create_mock_redis():
    """Create a mock Redis storage that returns None for all queries."""
    mock = MagicMock()
    mock.get_latest_ticker.return_value = None
    mock.get_latest_price.return_value = None
    mock.get_recent_trades.return_value = None
    mock.get_recent_alerts.return_value = []
    mock.health_check.return_value = True
    return mock


def create_mock_postgres():
    """Create a mock PostgreSQL storage that returns empty results."""
    mock = MagicMock()
    mock.query_candles.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


def create_mock_ticker_storage():
    """Create a mock Redis that returns empty results."""
    mock = MagicMock()
    mock.ping.return_value = True
    mock.get_ticker.return_value = None
    mock.get_ticker_all.return_value = []
    return mock


@pytest.fixture
def redis_storage():
    """Create Redis instance and clean up after test."""
    storage = Redis(host="localhost", port=6379, db=15)
    # Use client.flushdb() directly since flush_db method was removed
    storage.client.flushdb()
    yield storage
    storage.client.flushdb()


@pytest.fixture
def postgres_storage():
    """Create mock Postgres instance for testing."""
    mock = MagicMock(spec=Postgres)
    mock.query_candles.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


@pytest.fixture
def test_client(redis_storage, postgres_storage):
    """Create test client with mocked storage dependencies."""
    with rate_tracker._lock:
        rate_tracker._requests.clear()

    def override_get_redis():
        return redis_storage

    def override_get_postgres():
        return postgres_storage

    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_postgres] = override_get_postgres
    app.dependency_overrides[get_ticker_storage] = create_mock_ticker_storage

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()

    with rate_tracker._lock:
        rate_tracker._requests.clear()


@pytest.fixture
def fresh_client():
    """Create test client with fresh rate limit state and mocked dependencies."""
    with rate_tracker._lock:
        rate_tracker._requests.clear()

    app.dependency_overrides[get_redis] = create_mock_redis
    app.dependency_overrides[get_postgres] = create_mock_postgres
    app.dependency_overrides[get_ticker_storage] = create_mock_ticker_storage

    client = TestClient(app)
    yield client

    app.dependency_overrides.clear()
    with rate_tracker._lock:
        rate_tracker._requests.clear()


symbol_strategy = st.sampled_from(
    ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"]
)

limit_strategy = st.integers(min_value=1, max_value=1000)

# Minimal strategies for integration tests that write to Redis
price_strategy = st.floats(min_value=0.01, max_value=100000.0, allow_nan=False, allow_infinity=False)

alert_strategy = st.fixed_dictionaries(
    {
        "symbol": symbol_strategy,
        "alert_type": st.sampled_from(["whale", "price_spike", "volume_anomaly", "volatility"]),
        "severity": st.sampled_from(["info", "warning", "critical"]),
        "timestamp": st.integers(min_value=1704067200000, max_value=1704153600000),
        "message": st.text(min_size=5, max_size=100, alphabet=st.characters(whitelist_categories=("L", "N", "P", "Z"))),
    }
)

ticker_stats_strategy = st.fixed_dictionaries(
    {
        "open": price_strategy,
        "high": price_strategy,
        "low": price_strategy,
        "close": price_strategy,
        "volume": st.floats(min_value=0.0, max_value=1000000.0, allow_nan=False, allow_infinity=False),
        "quote_volume": st.floats(min_value=0.0, max_value=100000000.0, allow_nan=False, allow_infinity=False),
        "timestamp": st.integers(min_value=1704067200000, max_value=1704153600000),
    }
)


# =============================================================================
# Data Quality Tests using Great Expectations
# =============================================================================


def build_ticker_expectations():
    """Build list of expectations for ticker data returned by API."""
    return [
        gxe.ExpectColumnToExist(column="open"),
        gxe.ExpectColumnToExist(column="high"),
        gxe.ExpectColumnToExist(column="low"),
        gxe.ExpectColumnToExist(column="close"),
        gxe.ExpectColumnToExist(column="volume"),
        gxe.ExpectColumnValuesToNotBeNull(column="open"),
        gxe.ExpectColumnValuesToNotBeNull(column="high"),
        gxe.ExpectColumnValuesToNotBeNull(column="low"),
        gxe.ExpectColumnValuesToNotBeNull(column="close"),
        gxe.ExpectColumnValuesToNotBeNull(column="volume"),
        gxe.ExpectColumnValuesToBeBetween(column="open", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="high", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="low", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="close", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0),
    ]


def run_ticker_ge_validation(records, expectations):
    """Run GE validation on ticker records."""
    import pandas as pd

    df = pd.DataFrame(records)
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas("ticker_source")
    data_asset = data_source.add_dataframe_asset(name="ticker_data")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("ticker_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name="ticker_suite")
    for exp in expectations:
        suite.add_expectation(exp)

    return batch.validate(suite)


def test_ticker_data_quality_valid():
    """Test that valid ticker data passes all GE expectations."""
    tickers = [
        {
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 1000.0,
            "quote_volume": 50000000.0,
        },
        {"open": 3000.0, "high": 3100.0, "low": 2900.0, "close": 3050.0, "volume": 500.0, "quote_volume": 1500000.0},
    ]

    expectations = build_ticker_expectations()
    result = run_ticker_ge_validation(tickers, expectations)

    assert (
        result.success
    ), f"Valid tickers should pass. Failed: {[r.expectation_config.type for r in result.results if not r.success]}"


def test_ticker_negative_price_fails():
    """Test that ticker with negative prices fails GE validation."""
    invalid_tickers = [
        {
            "open": -100.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 1000.0,
            "quote_volume": 50000000.0,
        },
    ]

    expectations = build_ticker_expectations()
    result = run_ticker_ge_validation(invalid_tickers, expectations)

    assert not result.success, "Ticker with negative price should fail"


def test_ticker_null_values_fails():
    """Test that ticker with null values fails GE validation."""
    invalid_tickers = [
        {"open": None, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": 1000.0, "quote_volume": 50000000.0},
    ]

    expectations = build_ticker_expectations()
    result = run_ticker_ge_validation(invalid_tickers, expectations)

    assert not result.success, "Ticker with null values should fail"


def build_trade_expectations():
    """Build list of expectations for trade data returned by API."""
    return [
        gxe.ExpectColumnToExist(column="price"),
        gxe.ExpectColumnToExist(column="quantity"),
        gxe.ExpectColumnToExist(column="timestamp"),
        gxe.ExpectColumnValuesToNotBeNull(column="price"),
        gxe.ExpectColumnValuesToNotBeNull(column="quantity"),
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

    assert (
        result.success
    ), f"Valid trades should pass. Failed: {[r.expectation_config.type for r in result.results if not r.success]}"


def test_trade_negative_price_fails():
    """Test that trade with negative price fails GE validation."""
    invalid_trades = [
        {"price": -100.0, "quantity": 0.5, "timestamp": 1704067200000, "is_buyer_maker": True},
    ]

    expectations = build_trade_expectations()
    result = run_trade_ge_validation(invalid_trades, expectations)

    assert not result.success, "Trade with negative price should fail"


def test_trade_null_price_fails():
    """Test that trade with null price fails GE validation."""
    invalid_trades = [
        {"price": None, "quantity": 0.5, "timestamp": 1704067200000, "is_buyer_maker": True},
    ]

    expectations = build_trade_expectations()
    result = run_trade_ge_validation(invalid_trades, expectations)

    assert not result.success, "Trade with null price should fail"


def build_alert_api_expectations():
    """Build list of expectations for alert data returned by API."""
    return [
        gxe.ExpectColumnToExist(column="symbol"),
        gxe.ExpectColumnToExist(column="alert_type"),
        gxe.ExpectColumnToExist(column="severity"),
        gxe.ExpectColumnToExist(column="timestamp"),
        gxe.ExpectColumnValuesToNotBeNull(column="symbol"),
        gxe.ExpectColumnValuesToNotBeNull(column="alert_type"),
        gxe.ExpectColumnValuesToNotBeNull(column="severity"),
        gxe.ExpectColumnValuesToBeInSet(
            column="alert_type", value_set=["whale", "price_spike", "volume_anomaly", "volatility"]
        ),
        gxe.ExpectColumnValuesToBeInSet(column="severity", value_set=["info", "warning", "critical"]),
    ]


def run_alert_api_ge_validation(records, expectations):
    """Run GE validation on alert records from API."""
    import pandas as pd

    df = pd.DataFrame(records)
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas("alert_api_source")
    data_asset = data_source.add_dataframe_asset(name="alert_api_data")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("alert_api_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name="alert_api_suite")
    for exp in expectations:
        suite.add_expectation(exp)

    return batch.validate(suite)


def test_alert_api_data_quality_valid():
    """Test that valid alert data (API format) passes all GE expectations."""
    alerts = [
        {
            "symbol": "BTCUSDT",
            "alert_type": "whale",
            "severity": "critical",
            "timestamp": 1704067200000,
            "message": "Whale detected",
        },
        {
            "symbol": "ETHUSDT",
            "alert_type": "price_spike",
            "severity": "warning",
            "timestamp": 1704067260000,
            "message": "Price spike",
        },
    ]

    expectations = build_alert_api_expectations()
    result = run_alert_api_ge_validation(alerts, expectations)

    assert (
        result.success
    ), f"Valid alerts should pass. Failed: {[r.expectation_config.type for r in result.results if not r.success]}"


def test_alert_api_invalid_type_fails():
    """Test that alert with invalid alert_type fails GE validation."""
    invalid_alerts = [
        {
            "symbol": "BTCUSDT",
            "alert_type": "unknown_type",
            "severity": "critical",
            "timestamp": 1704067200000,
            "message": "Test",
        },
    ]

    expectations = build_alert_api_expectations()
    result = run_alert_api_ge_validation(invalid_alerts, expectations)

    assert not result.success, "Alert with invalid alert_type should fail"


def test_alert_api_invalid_severity_fails():
    """Test that alert with invalid severity fails GE validation."""
    invalid_alerts = [
        {"symbol": "BTCUSDT", "alert_type": "whale", "severity": "high", "timestamp": 1704067200000, "message": "Test"},
        # severity should be info/warning/critical, not high
    ]

    expectations = build_alert_api_expectations()
    result = run_alert_api_ge_validation(invalid_alerts, expectations)

    assert not result.success, "Alert with invalid severity should fail"


@pytest.fixture
def mock_query_router():
    """Create a mock Router for testing tier selection logic."""
    mock_redis = MagicMock()
    mock_postgres = MagicMock()

    router = Router(
        redis=mock_redis,
        postgres=mock_postgres,
    )
    return router


class TestQueryTierSelection:
    """Property tests for query tier selection based on time range (2-tier)."""

    # Class constants for tier names
    TIER_REDIS = "redis"
    TIER_POSTGRES = "postgres"

    @given(offset_minutes=st.integers(min_value=1, max_value=59))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_tier_selection_for_recent_data(self, mock_query_router, offset_minutes):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        """
        now = datetime.now()
        start = now - timedelta(minutes=offset_minutes)

        selected_tier = mock_query_router.select_tier(start)

        assert selected_tier == self.TIER_REDIS

    @given(offset_hours=st.integers(min_value=2, max_value=24 * 365))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_postgres_tier_selection_for_older_data(self, mock_query_router, offset_hours):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        In 2-tier architecture, all data older than 1 hour goes to PostgreSQL.
        """
        now = datetime.now()
        start = now - timedelta(hours=offset_hours)

        selected_tier = mock_query_router.select_tier(start)

        assert selected_tier == self.TIER_POSTGRES


@pytest.fixture
def test_client_redis(redis_storage):
    """Create test client with mocked Redis dependency."""
    with rate_tracker._lock:
        rate_tracker._requests.clear()

    def override_get_redis():
        return redis_storage

    app.dependency_overrides[get_redis] = override_get_redis
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()

    with rate_tracker._lock:
        rate_tracker._requests.clear()


@skip_if_no_redis
class TestAlertsLimitConstraint:
    """Property tests for alerts limit constraint."""

    @given(limit=limit_strategy, alerts=st.lists(alert_strategy, min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_alerts_limit_constraint(self, redis_storage, test_client_redis, limit, alerts):
        """
        Feature: fastapi-backend, Property 6: Alerts limit constraint
        Validates: Requirements 3.1
        """
        redis_storage.client.flushdb()

        for alert in alerts:
            redis_storage.write_alert(alert)

        response = test_client_redis.get(f"/api/v1/analytics/alerts/recent?limit={limit}")

        assert response.status_code == 200
        data = response.json()

        expected_max = min(limit, 1000)
        assert len(data) <= expected_max
        assert len(data) <= len(alerts)


class TestRateLimitEnforcement:
    """Property tests for rate limit enforcement."""

    def test_rate_limit_headers_present(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1, 6.4
        """
        response = fresh_client.get("/api/v1/market/ticker-health")

        assert "X-RateLimit-Limit" in response.headers
        assert "X-RateLimit-Remaining" in response.headers
        assert "X-RateLimit-Reset" in response.headers

        assert response.headers["X-RateLimit-Limit"] == str(RATE_LIMIT_PER_MINUTE)
        remaining = int(response.headers["X-RateLimit-Remaining"])
        assert 0 <= remaining <= RATE_LIMIT_PER_MINUTE

    def test_rate_limit_remaining_decreases(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        """
        response1 = fresh_client.get("/api/v1/market/ticker-health")
        remaining1 = int(response1.headers["X-RateLimit-Remaining"])

        response2 = fresh_client.get("/api/v1/market/ticker-health")
        remaining2 = int(response2.headers["X-RateLimit-Remaining"])

        assert remaining2 < remaining1

    def test_rate_limit_exceeded_returns_429(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        """
        for _i in range(RATE_LIMIT_PER_MINUTE):
            response = fresh_client.get("/api/v1/market/ticker-health")
            if response.status_code == 429:
                break

        response = fresh_client.get("/api/v1/market/ticker-health")

        assert response.status_code == 429
        assert "Retry-After" in response.headers
        assert response.headers["X-RateLimit-Remaining"] == "0"

    def test_health_endpoint_not_rate_limited(self, fresh_client):
        """
        Feature: fastapi-backend, Property 9: Rate limit enforcement
        Validates: Requirements 6.1
        """
        for _ in range(RATE_LIMIT_PER_MINUTE + 5):
            fresh_client.get("/api/v1/market/ticker-health")

        response = fresh_client.get("/api/v1/system/health")

        assert response.status_code != 429


@pytest.fixture
def test_client_with_fallback(redis_storage, postgres_storage):
    """Create test client with all storage dependencies (2-tier)."""
    with rate_tracker._lock:
        rate_tracker._requests.clear()

    def override_get_redis():
        return redis_storage

    def override_get_postgres():
        return postgres_storage

    def override_get_query_router():
        return Router(redis_storage, postgres_storage)

    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_postgres] = override_get_postgres
    app.dependency_overrides[get_query_router] = override_get_query_router

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()

    with rate_tracker._lock:
        rate_tracker._requests.clear()


@skip_if_no_redis
class TestFallbackChainExecution:
    """Property tests for fallback chain execution (2-tier)."""

    @given(symbol=symbol_strategy, stats=ticker_stats_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_primary_returns_redis_source(
        self, redis_storage, postgres_storage, test_client_with_fallback, symbol, stats
    ):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.1, 7.2, 7.4
        """
        redis_storage.write_latest_ticker(symbol, stats)

        response = test_client_with_fallback.get(f"/api/v1/market/ticker/{symbol}")

        assert response.status_code == 200
        assert "X-Data-Source" in response.headers
        assert response.headers["X-Data-Source"] == "redis"

    @given(symbol=symbol_strategy)
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_all_sources_empty_returns_404(self, redis_storage, postgres_storage, test_client_with_fallback, symbol):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.3
        """
        redis_storage.client.flushdb()
        postgres_storage.query_candles.return_value = []

        response = test_client_with_fallback.get(f"/api/v1/market/ticker/{symbol}")

        assert response.status_code == 404


class TestHealthStatusLogic:
    """Property tests for health status logic."""

    @given(redis_healthy=st.booleans(), duckdb_healthy=st.booleans(), kafka_healthy=st.booleans())
    @settings(max_examples=100)
    def test_health_status_reflects_service_state(self, redis_healthy, duckdb_healthy, kafka_healthy):
        """
        Feature: fastapi-backend, Property 8: Health status reflects service state
        Validates: Requirements 5.1, 5.4
        """
        status = determine_overall_status(redis_healthy, duckdb_healthy, kafka_healthy)

        services = [redis_healthy, duckdb_healthy, kafka_healthy]
        healthy_count = sum(services)
        total_services = len(services)

        if healthy_count == total_services:
            assert status == "healthy"
        elif healthy_count == 0:
            assert status == "unhealthy"
        else:
            assert status == "degraded"

    def test_all_healthy_returns_healthy(self):
        """Verify all services up returns 'healthy'."""
        status = determine_overall_status(True, True, True)
        assert status == "healthy"

    def test_all_unhealthy_returns_unhealthy(self):
        """Verify all services down returns 'unhealthy'."""
        status = determine_overall_status(False, False, False)
        assert status == "unhealthy"

    def test_partial_failure_returns_degraded(self):
        """Verify partial failure returns 'degraded'."""
        assert determine_overall_status(False, True, True) == "degraded"
        assert determine_overall_status(True, False, True) == "degraded"
        assert determine_overall_status(True, True, False) == "degraded"
        assert determine_overall_status(False, False, True) == "degraded"
