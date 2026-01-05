import os
from typing import Any
from datetime import datetime, timedelta

import requests

API_URL = os.getenv("API_URL", "http://localhost:8000")


def _get(endpoint: str, params: dict = None) -> Any:
    try:
        resp = requests.get(f"{API_URL}{endpoint}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_realtime_tickers() -> list[dict]:
    data = _get("/api/v1/market/realtime")
    if isinstance(data, dict):
        return data.get("tickers", [])
    return data or []


def get_market_summary() -> dict:
    return _get("/api/v1/market/summary") or {}


def get_top_by_volume(limit: int = 5) -> list[dict]:
    return _get("/api/v1/market/top-by-volume", {"limit": limit}) or []


def get_top_by_trades(limit: int = 5) -> list[dict]:
    return _get("/api/v1/market/top-by-trades", {"limit": limit}) or []


def get_klines(symbol: str, interval: str = "1m", minutes: int = 60) -> list[dict]:
    return _get(f"/api/v1/analytics/klines/{symbol}", {"interval": interval, "minutes": minutes}) or []


def get_trades_count(symbol: str, interval: str = "1h", hours: int = 24) -> list[dict]:
    return _get("/api/v1/analytics/trades-count", {"symbol": symbol, "interval": interval, "hours": hours}) or []


def get_whale_alerts(hours: int = 24) -> list[dict]:
    return _get("/api/v1/analytics/alerts/whale-alerts", {"hours": hours}) or []


def get_system_health() -> dict:
    return _get("/api/v1/system/health") or {}


def get_ticker_health() -> dict:
    return _get("/api/v1/market/ticker-health") or {}


def get_available_symbols() -> list[str]:
    tickers = get_realtime_tickers()
    return sorted([t.get("symbol", "") for t in tickers if t.get("symbol")])
