"""
Consolidated test module for FastAPI Backend.
Contains all tests for market, analytics, alerts, rate limiting, fallback, and system endpoints.

Table of Contents:
- Imports and Setup (line ~20)
- Market Router Tests (line ~150)
- Analytics Router Tests (line ~300)
- Alerts Router Tests (line ~450)
- Rate Limit Tests (line ~550)
- Fallback Chain Tests (line ~700)
- System Router Tests (line ~850)

Requirements: 6.4
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from hypothesis import given, settings, strategies as st, HealthCheck
from fastapi.testclient import TestClient

from src.api.app import app, rate_tracker, RATE_LIMIT_PER_MINUTE, get_redis, get_postgres, get_minio, get_query_router, get_ticker_storage, determine_overall_status
from src.storage.redis import RedisStorage
from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage
from src.storage.query_router import QueryRouter


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
# MOCK STORAGE FACTORIES
# ============================================================================

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
    mock.query_indicators.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


def create_mock_minio():
    """Create a mock MinIO storage that returns empty results."""
    mock = MagicMock()
    mock.read_klines.return_value = []
    mock.read_indicators.return_value = []
    mock.read_alerts.return_value = []
    return mock


def create_mock_ticker_storage():
    """Create a mock RedisTickerStorage that returns empty results."""
    mock = MagicMock()
    mock.ping.return_value = True
    mock.get_ticker.return_value = None
    mock.get_all_tickers.return_value = []
    return mock


# ============================================================================
# TEST FIXTURES
# ============================================================================

@pytest.fixture
def redis_storage():
    """Create RedisStorage instance and clean up after test."""
    storage = RedisStorage(host="localhost", port=6379, db=15)
    # Use client.flushdb() directly since flush_db method was removed
    storage.client.flushdb()
    yield storage
    storage.client.flushdb()


@pytest.fixture
def postgres_storage():
    """Create mock PostgresStorage instance for testing."""
    mock = MagicMock(spec=PostgresStorage)
    mock.query_candles.return_value = []
    mock.query_indicators.return_value = []
    mock.query_alerts.return_value = []
    mock._execute_with_retry.return_value = [{"result": 1}]
    return mock


@pytest.fixture
def minio_storage():
    """Create mock MinioStorage instance for testing."""
    mock = MagicMock(spec=MinioStorage)
    mock.read_klines.return_value = []
    mock.read_indicators.return_value = []
    mock.read_alerts.return_value = []
    return mock


@pytest.fixture
def test_client(redis_storage, postgres_storage, minio_storage):
    """Create test client with mocked storage dependencies."""
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    def override_get_redis():
        return redis_storage
    
    def override_get_postgres():
        return postgres_storage
    
    def override_get_minio():
        return minio_storage
    
    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_postgres] = override_get_postgres
    app.dependency_overrides[get_minio] = override_get_minio
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
    app.dependency_overrides[get_minio] = create_mock_minio
    app.dependency_overrides[get_ticker_storage] = create_mock_ticker_storage
    
    client = TestClient(app)
    yield client
    
    app.dependency_overrides.clear()
    with rate_tracker._lock:
        rate_tracker._requests.clear()


# ============================================================================
# STRATEGIES FOR GENERATING TEST DATA
# ============================================================================

symbol_strategy = st.sampled_from([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"
])

price_strategy = st.floats(min_value=0.00001, max_value=1000000.0, allow_nan=False, allow_infinity=False)
volume_strategy = st.floats(min_value=0.0, max_value=1000000000.0, allow_nan=False, allow_infinity=False)
timestamp_strategy = st.integers(min_value=1600000000000, max_value=2000000000000)
limit_strategy = st.integers(min_value=1, max_value=1000)

ticker_stats_strategy = st.fixed_dictionaries({
    "open": price_strategy,
    "high": price_strategy,
    "low": price_strategy,
    "close": price_strategy,
    "volume": volume_strategy,
    "quote_volume": volume_strategy,
    "price_change_24h": st.floats(min_value=-100.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    "timestamp": timestamp_strategy,
})

trade_strategy = st.fixed_dictionaries({
    "price": price_strategy,
    "quantity": volume_strategy,
    "timestamp": timestamp_strategy,
    "is_buyer_maker": st.booleans(),
})

alert_type_strategy = st.sampled_from(["whale", "price_spike", "volume_anomaly", "volatility"])
severity_strategy = st.sampled_from(["info", "warning", "critical"])

alert_strategy = st.fixed_dictionaries({
    "symbol": symbol_strategy,
    "alert_type": alert_type_strategy,
    "severity": severity_strategy,
    "message": st.text(min_size=1, max_size=100, alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'Z'))),
    "timestamp": timestamp_strategy,
    "metadata": st.none() | st.fixed_dictionaries({
        "value": st.floats(min_value=0, max_value=1000000, allow_nan=False, allow_infinity=False),
    }),
})

valid_indicators = ["rsi", "macd", "macd_signal", "sma_20", "ema_12", "ema_26", "bb_upper", "bb_lower", "atr"]
indicator_strategy = st.sampled_from(valid_indicators)
indicator_subset_strategy = st.lists(indicator_strategy, min_size=1, max_size=5, unique=True)


# ============================================================================
# MARKET ROUTER TESTS
# ============================================================================

@skip_if_no_redis
class TestTickerDataConsistency:
    """Property tests for ticker data consistency."""
    
    @given(symbol=symbol_strategy, stats=ticker_stats_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_ticker_data_consistency(self, redis_storage, test_client, symbol, stats):
        """
        Feature: fastapi-backend, Property 1: Redis data retrieval consistency
        Validates: Requirements 1.1
        """
        redis_storage.write_latest_ticker(symbol, stats)
        
        response = test_client.get(f"/api/v1/market/ticker/{symbol}")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["symbol"] == symbol
        assert abs(data["price"] - stats["close"]) < 1e-6
        assert abs(data["volume"] - stats["volume"]) < 1e-6


@skip_if_no_redis
class TestPriceDataConsistency:
    """Property tests for price data consistency."""
    
    @given(symbol=symbol_strategy, price=price_strategy, volume=volume_strategy, timestamp=timestamp_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_price_data_consistency(self, redis_storage, test_client, symbol, price, volume, timestamp):
        """
        Feature: fastapi-backend, Property 2: Price endpoint data consistency
        Validates: Requirements 1.2
        """
        redis_storage.write_latest_price(symbol, price, volume, timestamp)
        
        response = test_client.get(f"/api/v1/market/price/{symbol}")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["symbol"] == symbol
        assert abs(data["price"] - price) < 1e-6
        assert abs(data["volume"] - volume) < 1e-6
        assert data["timestamp"] == timestamp


@skip_if_no_redis
class TestTradesLimitConstraint:
    """Property tests for trades limit constraint."""
    
    @given(
        symbol=symbol_strategy,
        limit=limit_strategy,
        trades=st.lists(trade_strategy, min_size=1, max_size=100)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_trades_limit_constraint(self, redis_storage, test_client, symbol, limit, trades):
        """
        Feature: fastapi-backend, Property 3: Trades limit constraint
        Validates: Requirements 1.3
        """
        redis_storage.client.flushdb()
        
        for trade in trades:
            redis_storage.write_recent_trade(symbol, trade)
        
        response = test_client.get(f"/api/v1/market/trades/{symbol}?limit={limit}")
        
        assert response.status_code == 200
        data = response.json()
        
        expected_max = min(limit, 1000)
        assert len(data) <= expected_max
        assert len(data) <= len(trades)


# ============================================================================
# ANALYTICS ROUTER TESTS
# ============================================================================

@pytest.fixture
def mock_query_router():
    """Create a mock QueryRouter for testing tier selection logic."""
    mock_redis = MagicMock()
    mock_postgres = MagicMock()
    mock_minio = MagicMock()
    
    router = QueryRouter(
        redis=mock_redis,
        postgres=mock_postgres,
        minio=mock_minio,
    )
    return router


class TestQueryTierSelection:
    """Property tests for query tier selection based on time range."""
    
    # Class constants for tier names
    TIER_REDIS = "redis"
    TIER_POSTGRES = "postgres"
    TIER_MINIO = "minio"
    
    @given(offset_minutes=st.integers(min_value=1, max_value=59))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_tier_selection_for_recent_data(self, mock_query_router, offset_minutes):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = now - timedelta(minutes=offset_minutes)
        
        selected_tier = mock_query_router._select_tier(start)
        
        assert selected_tier == self.TIER_REDIS
    
    @given(offset_hours=st.integers(min_value=2, max_value=24 * 89))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_postgres_tier_selection_for_warm_data(self, mock_query_router, offset_hours):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = now - timedelta(hours=offset_hours)
        
        selected_tier = mock_query_router._select_tier(start)
        
        assert selected_tier == self.TIER_POSTGRES
    
    @given(offset_days=st.integers(min_value=91, max_value=364))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_minio_tier_selection_for_cold_data(self, mock_query_router, offset_days):
        """
        Feature: fastapi-backend, Property 4: Query tier selection based on time range
        Validates: Requirements 2.1
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = now - timedelta(days=offset_days)
        
        selected_tier = mock_query_router._select_tier(start)
        
        assert selected_tier == self.TIER_MINIO


# ============================================================================
# ALERTS ROUTER TESTS
# ============================================================================

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
    
    @given(
        limit=limit_strategy,
        alerts=st.lists(alert_strategy, min_size=1, max_size=50)
    )
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


# ============================================================================
# RATE LIMIT TESTS
# ============================================================================

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
        for i in range(RATE_LIMIT_PER_MINUTE):
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


# ============================================================================
# FALLBACK CHAIN TESTS
# ============================================================================

@pytest.fixture
def test_client_with_fallback(redis_storage, postgres_storage, minio_storage):
    """Create test client with all storage dependencies."""
    with rate_tracker._lock:
        rate_tracker._requests.clear()
    
    def override_get_redis():
        return redis_storage
    
    def override_get_postgres():
        return postgres_storage
    
    def override_get_minio():
        return minio_storage
    
    def override_get_query_router():
        return QueryRouter(redis_storage, postgres_storage, minio_storage)
    
    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_postgres] = override_get_postgres
    app.dependency_overrides[get_minio] = override_get_minio
    app.dependency_overrides[get_query_router] = override_get_query_router
    
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
    
    with rate_tracker._lock:
        rate_tracker._requests.clear()


@skip_if_no_redis
class TestFallbackChainExecution:
    """Property tests for fallback chain execution."""
    
    @given(symbol=symbol_strategy, stats=ticker_stats_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_redis_primary_returns_redis_source(
        self, redis_storage, postgres_storage, minio_storage, test_client_with_fallback, symbol, stats
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
    def test_all_sources_empty_returns_404(
        self, redis_storage, postgres_storage, minio_storage, test_client_with_fallback, symbol
    ):
        """
        Feature: fastapi-backend, Property 10: Fallback chain execution
        Validates: Requirements 7.3
        """
        redis_storage.client.flushdb()
        postgres_storage.query_candles.return_value = []
        minio_storage.read_klines.return_value = []
        
        response = test_client_with_fallback.get(f"/api/v1/market/ticker/{symbol}")
        
        assert response.status_code == 404


# ============================================================================
# SYSTEM ROUTER TESTS
# ============================================================================

class TestHealthStatusLogic:
    """Property tests for health status logic."""
    
    @given(
        redis_healthy=st.booleans(),
        duckdb_healthy=st.booleans(),
        kafka_healthy=st.booleans()
    )
    @settings(max_examples=100)
    def test_health_status_reflects_service_state(
        self, redis_healthy, duckdb_healthy, kafka_healthy
    ):
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
