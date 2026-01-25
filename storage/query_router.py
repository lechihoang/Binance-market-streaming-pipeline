"""Query router with automatic tier selection (Redis/PostgreSQL).

Routes data queries to the appropriate storage tier based on time range:
- Recent data (< CACHE_HOUR): Redis (hot tier, fast access)
- Historical data (>= CACHE_HOUR): PostgreSQL (warm tier, persistent storage)

All methods return Pydantic models or dicts for backward compatibility.
"""

from datetime import datetime, timedelta
from typing import Any

from loguru import logger

from schema.market import Alert, Kline
from util.constant import CACHE_HOUR

from .postgres import Postgres
from .redis import Redis


class Router:
    """Unified query interface for Redis and PostgreSQL storage."""

    TIER = ["redis", "postgres"]
    KLINE = "klines"
    ALERT = "alerts"
    TRADE = "trades"

    def __init__(self, redis: Redis, postgres: Postgres):
        self.redis = redis
        self.pg = postgres

    def select_tier(self, start: datetime) -> str:
        """Select storage tier based on query start time."""
        now = datetime.now()
        cutoff = now - timedelta(hours=CACHE_HOUR)
        return "redis" if start > cutoff else "postgres"

    # ==========================================
    # Kline Queries
    # ==========================================

    def get_klines(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[Kline]:
        """Get klines with automatic tier selection, returns Kline models."""
        tier = self.select_tier(start)

        # Try Redis first if appropriate
        if tier == "redis":
            now = datetime.now()
            time_diff = (now - start).total_seconds() / 60
            # Redis only has latest record, use for very recent queries only
            if time_diff <= 5:
                klines = self.redis.get_klines(symbol, interval)
                if klines:
                    logger.debug(f"Klines from redis: {symbol}, interval={interval}")
                    return klines

        # Fall back to Postgres
        try:
            klines = self.pg.get_klines(symbol, start, end, interval)
            if klines:
                logger.debug(f"Klines from postgres: {symbol}, interval={interval}")
                return klines
        except Exception as e:
            logger.warning(f"Postgres kline query failed: {e}")

        return []

    # ==========================================
    # Alert Queries
    # ==========================================

    def get_alerts(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[Alert]:
        """Get alerts with automatic tier selection, returns Alert models."""
        tier = self.select_tier(start)

        # Try Redis first if appropriate
        if tier == "redis":
            alerts = self.redis.get_alerts()
            # Filter by symbol and time range
            filtered = [
                a for a in alerts
                if a.symbol == symbol and start <= a.timestamp <= end
            ]
            if filtered:
                logger.debug(f"Alerts from redis: {symbol}")
                return filtered

        # Fall back to Postgres
        try:
            alerts = self.pg.get_alerts(symbol, start, end)
            if alerts:
                logger.debug(f"Alerts from postgres: {symbol}")
                return alerts
        except Exception as e:
            logger.warning(f"Postgres alert query failed: {e}")

        return []

    # ==========================================
    # Legacy Interface (backward compatibility)
    # ==========================================

    def query(
        self, data_type: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, Any]]:
        """Legacy query method - returns dicts for backward compatibility."""
        if data_type == self.KLINE:
            klines = self.get_klines(symbol, start, end, interval)
            return [k.model_dump() for k in klines]

        if data_type == self.ALERT:
            alerts = self.get_alerts(symbol, start, end)
            result = []
            for alert in alerts:
                d = alert.model_dump()
                # Keep 'metadata' key for backward compatibility
                d["metadata"] = d.pop("details", {})
                result.append(d)
            return result

        if data_type == self.TRADE:
            # Trades only from Redis (real-time cache)
            return self.redis.get_trade(symbol, limit=1000)

        return []

    def query_tier(
        self, tier: str, data_type: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, Any]]:
        """Legacy: Query specific tier directly."""
        if data_type == self.KLINE:
            if tier == "redis":
                return self.redis.get_agg_list(symbol, interval)
            return self.pg.get_kline_agg(symbol, start, end, interval)

        if data_type == self.ALERT:
            if tier == "redis":
                return self.redis.get_alert(limit=1000)
            return self.pg.get_alert(symbol, start, end)

        if data_type == self.TRADE:
            if tier == "redis":
                return self.redis.get_trade(symbol, limit=1000)

        return []
