"""Tests for storage layer - health checks, query router, data quality validation."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from processing.validator.aggregation_validator import build_aggregation_expectations
from processing.validator.aggregation_validator import run_ge_validation as run_aggregation_ge_validation
from processing.validator.anomaly_validator import build_anomaly_expectations
from processing.validator.anomaly_validator import run_ge_validation as run_anomaly_ge_validation
from storage.query_router import Router

# ==========================================
# Redis Health Check
# ==========================================


class TestRedisHealthCheck:
    @patch("storage.redis.redis.Redis")
    def test_success(self, mock_redis_cls):
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis_cls.return_value = mock_client

        from storage.redis import check_health

        result = check_health(host="localhost", port=6379, db=0)
        assert result["status"] == "healthy"
        assert result["service"] == "redis"
        assert result["tier"] == "hot"
        mock_client.ping.assert_called_once()
        mock_client.close.assert_called_once()

    @patch("storage.redis.redis.Redis")
    def test_failure_raises(self, mock_redis_cls):
        mock_client = Mock()
        mock_client.ping.side_effect = ConnectionError("Connection refused")
        mock_redis_cls.return_value = mock_client

        from storage.redis import check_health

        with pytest.raises(Exception, match="Redis health check failed"):
            check_health(host="localhost", port=6379, db=0)


# ==========================================
# PostgreSQL Health Check
# ==========================================


class TestPostgresHealthCheck:
    @patch("storage.postgres.psycopg2.connect")
    def test_success(self, mock_connect):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        from storage.postgres import check_health

        result = check_health(
            host="localhost", port=5432, user="crypto", password="crypto", database="crypto_data", retries=1
        )
        assert result["status"] == "healthy"
        assert result["service"] == "postgresql"
        assert result["tier"] == "warm"
        mock_conn.close.assert_called_once()

    @patch("storage.postgres.psycopg2.connect")
    def test_failure_raises(self, mock_connect):
        mock_connect.side_effect = Exception("Connection refused")

        from storage.postgres import check_health

        with pytest.raises(Exception, match="PostgreSQL health check failed"):
            check_health(
                host="localhost", port=5432, user="test", password="test", database="test", retries=1, delay=0.01
            )


# ==========================================
# Query Router - Tier Selection
# ==========================================


class TestRouterTierSelection:
    def setup_method(self):
        self.router = Router(redis=MagicMock(), postgres=MagicMock())

    @given(minutes_ago=st.integers(min_value=0, max_value=59))
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_selects_redis_for_recent_queries(self, minutes_ago):
        start = datetime.now() - timedelta(minutes=minutes_ago)
        assert self.router.select_tier(start) == "redis"

    @given(hours_ago=st.integers(min_value=1, max_value=2000))
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_selects_postgres_for_old_queries(self, hours_ago):
        start = datetime.now() - timedelta(hours=hours_ago, minutes=1)
        assert self.router.select_tier(start) == "postgres"

    def test_boundary_exactly_one_hour(self):
        start = datetime.now() - timedelta(hours=1, seconds=1)
        assert self.router.select_tier(start) == "postgres"

    def test_just_under_one_hour(self):
        start = datetime.now() - timedelta(minutes=59)
        assert self.router.select_tier(start) == "redis"


# ==========================================
# Data Quality - Aggregation Validation (GE)
# ==========================================


class TestAggregationValidation:
    def test_valid_ohlcv_passes(self):
        records = [
            {"symbol": "BTCUSDT", "open": 50000.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": 100.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is True

    def test_high_less_than_low_fails(self):
        records = [
            {"symbol": "BTCUSDT", "open": 50000.0, "high": 48000.0, "low": 49000.0, "close": 50500.0, "volume": 100.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is False

    def test_negative_volume_fails(self):
        records = [
            {"symbol": "BTCUSDT", "open": 50000.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": -1.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is False

    def test_null_symbol_fails(self):
        records = [
            {"symbol": None, "open": 50000.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": 100.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is False

    def test_high_less_than_open_fails(self):
        records = [
            {"symbol": "BTCUSDT", "open": 51000.0, "high": 50000.0, "low": 49000.0, "close": 50500.0, "volume": 100.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is False

    def test_high_less_than_close_fails(self):
        records = [
            {"symbol": "BTCUSDT", "open": 48000.0, "high": 49000.0, "low": 47000.0, "close": 50000.0, "volume": 100.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is False


# ==========================================
# Data Quality - Anomaly Validation (GE)
# ==========================================


class TestAnomalyValidation:
    def _make_alert(self, **overrides):
        base = {
            "alert_id": "a1",
            "symbol": "BTCUSDT",
            "alert_type": "VOLUME_SPIKE",
            "alert_level": "HIGH",
            "timestamp": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat(),
        }
        base.update(overrides)
        return base

    def test_valid_alert_passes(self):
        result = run_anomaly_ge_validation([self._make_alert()], build_anomaly_expectations())
        assert result.success is True

    def test_invalid_alert_type_fails(self):
        result = run_anomaly_ge_validation([self._make_alert(alert_type="INVALID_TYPE")], build_anomaly_expectations())
        assert result.success is False

    def test_invalid_alert_level_fails(self):
        result = run_anomaly_ge_validation([self._make_alert(alert_level="CRITICAL")], build_anomaly_expectations())
        assert result.success is False

    def test_null_symbol_fails(self):
        result = run_anomaly_ge_validation([self._make_alert(symbol=None)], build_anomaly_expectations())
        assert result.success is False
