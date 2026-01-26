"""Tests for FastAPI REST API - endpoints, health checks, rate limiting."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st

from api.app import (
    app,
    determine_overall_status,
    get_postgres,
    get_query_router,
    get_redis,
    get_ticker_storage,
    rate_tracker,
)
from storage.query_router import Router
from util.constant import RATE_LIMIT

# ==========================================
# Fixtures
# ==========================================


def _create_mock_redis():
    mock = MagicMock()
    mock.ping.return_value = True
    mock.get_ticker.return_value = None
    mock.get_ticker_all.return_value = []
    mock.get_trade.return_value = []
    mock.get_alerts.return_value = []
    mock.get_kline.return_value = None
    mock.get_klines.return_value = []
    return mock


def _create_mock_postgres():
    mock = MagicMock()
    mock.run.return_value = [{"result": 1}]
    mock.get_klines.return_value = []
    mock.get_alerts.return_value = []
    mock.get_trades_count.return_value = []
    return mock


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    rate_tracker._requests.clear()
    yield
    rate_tracker._requests.clear()


@pytest.fixture
def client():
    mock_redis = _create_mock_redis()
    mock_postgres = _create_mock_postgres()
    mock_router = Router(redis=mock_redis, postgres=mock_postgres)

    app.dependency_overrides[get_redis] = lambda: mock_redis
    app.dependency_overrides[get_postgres] = lambda: mock_postgres
    app.dependency_overrides[get_ticker_storage] = lambda: mock_redis
    app.dependency_overrides[get_query_router] = lambda: mock_router

    yield TestClient(app)

    app.dependency_overrides.clear()


# ==========================================
# Basic Endpoints
# ==========================================


class TestBasicEndpoints:
    def test_root(self, client):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Crypto Data API"
        assert "version" in data

    def test_docs_available(self, client):
        response = client.get("/docs")
        assert response.status_code == 200

    def test_openapi_json(self, client):
        response = client.get("/openapi.json")
        assert response.status_code == 200
        assert "paths" in response.json()


# ==========================================
# Health Status Logic
# ==========================================


class TestHealthStatusLogic:
    def test_all_healthy(self):
        assert determine_overall_status(True, True, True) == "healthy"

    def test_all_unhealthy(self):
        assert determine_overall_status(False, False, False) == "unhealthy"

    def test_partial_healthy_is_degraded(self):
        assert determine_overall_status(True, False, False) == "degraded"
        assert determine_overall_status(False, True, False) == "degraded"
        assert determine_overall_status(True, True, False) == "degraded"

    @given(redis=st.booleans(), postgres=st.booleans(), kafka=st.booleans())
    @settings(max_examples=50)
    def test_health_status_consistency(self, redis, postgres, kafka):
        status = determine_overall_status(redis, postgres, kafka)
        services = [redis, postgres, kafka]
        if all(services):
            assert status == "healthy"
        elif not any(services):
            assert status == "unhealthy"
        else:
            assert status == "degraded"


# ==========================================
# Rate Limiting
# ==========================================


class TestRateLimiting:
    def test_rate_limit_headers_present(self, client):
        response = client.get("/api/v1/market/realtime")
        assert "x-ratelimit-limit" in response.headers
        assert "x-ratelimit-remaining" in response.headers
        assert "x-ratelimit-reset" in response.headers

    def test_remaining_decreases(self, client):
        r1 = client.get("/api/v1/market/realtime")
        r2 = client.get("/api/v1/market/realtime")
        remaining1 = int(r1.headers["x-ratelimit-remaining"])
        remaining2 = int(r2.headers["x-ratelimit-remaining"])
        assert remaining2 < remaining1

    def test_health_not_rate_limited(self, client):
        response = client.get("/api/v1/system/health")
        assert "x-ratelimit-limit" not in response.headers

    def test_rate_limit_exceeded_returns_429(self, client):
        for _ in range(RATE_LIMIT):
            client.get("/api/v1/market/realtime")

        response = client.get("/api/v1/market/realtime")
        assert response.status_code == 429
        assert "retry-after" in response.headers


# ==========================================
# Market Endpoints
# ==========================================


class TestMarketEndpoints:
    def test_ticker_health(self, client):
        response = client.get("/api/v1/market/ticker-health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "ticker_count" in data

    def test_realtime_tickers(self, client):
        response = client.get("/api/v1/market/realtime")
        assert response.status_code == 200
        data = response.json()
        assert "tickers" in data
        assert "count" in data

    def test_market_summary(self, client):
        response = client.get("/api/v1/market/summary")
        assert response.status_code == 200
        data = response.json()
        assert "total_symbols" in data
        assert "total_trades" in data

    def test_top_by_trades(self, client):
        response = client.get("/api/v1/market/top-by-trades")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_top_by_volume(self, client):
        response = client.get("/api/v1/market/top-by-volume")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_recent_trades(self, client):
        response = client.get("/api/v1/market/recent-trades?symbol=BTCUSDT")
        assert response.status_code == 200
        assert isinstance(response.json(), list)


# ==========================================
# Analytics Endpoints
# ==========================================


class TestAnalyticsEndpoints:
    def test_klines(self, client):
        response = client.get("/api/v1/analytics/klines/BTCUSDT")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_klines_invalid_interval(self, client):
        response = client.get("/api/v1/analytics/klines/BTCUSDT?interval=invalid")
        assert response.status_code == 400

    def test_price_spikes(self, client):
        response = client.get("/api/v1/analytics/alerts/price-spikes")
        assert response.status_code == 200

    def test_volume_spikes(self, client):
        response = client.get("/api/v1/analytics/alerts/volume-spikes")
        assert response.status_code == 200

    def test_trade_count_spikes(self, client):
        response = client.get("/api/v1/analytics/alerts/trade-count-spikes")
        assert response.status_code == 200

    def test_buy_sell_imbalance(self, client):
        response = client.get("/api/v1/analytics/alerts/buy-sell-imbalance")
        assert response.status_code == 200


# ==========================================
# System Endpoints
# ==========================================


class TestSystemEndpoints:
    def test_health(self, client):
        response = client.get("/api/v1/system/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "redis" in data
        assert "postgres" in data
