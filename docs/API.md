# API Documentation

Complete REST API reference for the Real-Time Cryptocurrency Data Pipeline.

## Table of Contents

- [Overview](#overview)
- [Base URL](#base-url)
- [Authentication](#authentication)
- [Rate Limiting](#rate-limiting)
- [Market Data Endpoints](#market-data-endpoints)
- [Analytics Endpoints](#analytics-endpoints)
- [Alert Endpoints](#alert-endpoints)
- [System Endpoints](#system-endpoints)
- [Query Parameters](#query-parameters)
- [Response Format](#response-format)
- [Error Handling](#error-handling)

## Overview

The API is built with FastAPI and provides real-time access to cryptocurrency market data, analytics, and alerts. All endpoints return JSON responses and support OpenAPI (Swagger) documentation.

**Interactive Documentation:**
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Base URL

```
http://localhost:8000
```

All endpoints are prefixed with `/api/v1` for versioning.

## Authentication

Currently, the API does not require authentication. This is suitable for local development and internal networks.

For production deployment, consider adding:
- API key authentication
- JWT tokens
- OAuth2

## Rate Limiting

**Current limits:**
- 100 requests per minute per IP
- Configurable in API settings

Exceed the rate limit and you'll receive a `429 Too Many Requests` response.

## Market Data Endpoints

### Get Real-Time Ticker Data

Get all real-time ticker data cached in Redis (last 1 hour).

**Endpoint:** `GET /api/v1/market/realtime`

**Response:**
```json
[
  {
    "symbol": "BTCUSDT",
    "price": "43250.50",
    "price_change_percent": "2.45",
    "high_24h": "44000.00",
    "low_24h": "42000.00",
    "volume_24h": "125000.50",
    "quote_volume_24h": "5400000000.00",
    "last_update": "2024-01-15T10:30:45.123Z"
  }
]
```

**Example:**
```bash
curl http://localhost:8000/api/v1/market/realtime
```

---

### Get Market Summary

Get aggregated statistics across all trading pairs.

**Endpoint:** `GET /api/v1/market/summary`

**Response:**
```json
{
  "total_symbols": 15,
  "total_24h_volume": "15000000000.00",
  "avg_price_change": "1.25",
  "top_gainer": {
    "symbol": "ETHUSDT",
    "price_change_percent": "5.60"
  },
  "top_loser": {
    "symbol": "ADAUSDT",
    "price_change_percent": "-2.30"
  },
  "timestamp": "2024-01-15T10:30:45.123Z"
}
```

**Example:**
```bash
curl http://localhost:8000/api/v1/market/summary
```

---

### Get Ticker Service Health

Check the health of the ticker data feed.

**Endpoint:** `GET /api/v1/market/ticker-health`

**Response:**
```json
{
  "status": "healthy",
  "active_symbols": 15,
  "last_update": "2024-01-15T10:30:45.123Z",
  "stale_symbols": []
}
```

---

### Get Top Symbols by Trade Count

Get symbols with the highest number of trades.

**Endpoint:** `GET /api/v1/market/top-by-trades`

**Query Parameters:**
- `limit` (optional, default: 10): Number of symbols to return

**Response:**
```json
[
  {
    "symbol": "BTCUSDT",
    "trade_count": 125000,
    "rank": 1
  },
  {
    "symbol": "ETHUSDT",
    "trade_count": 98000,
    "rank": 2
  }
]
```

**Example:**
```bash
curl "http://localhost:8000/api/v1/market/top-by-trades?limit=5"
```

---

### Get Top Symbols by Volume

Get symbols with the highest trading volume.

**Endpoint:** `GET /api/v1/market/top-by-volume`

**Query Parameters:**
- `limit` (optional, default: 10): Number of symbols to return

**Response:**
```json
[
  {
    "symbol": "BTCUSDT",
    "quote_volume_24h": "5400000000.00",
    "rank": 1
  },
  {
    "symbol": "ETHUSDT",
    "quote_volume_24h": "3200000000.00",
    "rank": 2
  }
]
```

**Example:**
```bash
curl "http://localhost:8000/api/v1/market/top-by-volume?limit=5"
```

## Analytics Endpoints

### Get OHLCV Klines

Get candlestick (OHLCV) data for a specific symbol with automatic tier routing.

**Endpoint:** `GET /api/v1/analytics/klines/{symbol}`

**Path Parameters:**
- `symbol` (required): Trading pair (e.g., BTCUSDT)

**Query Parameters:**
- `interval` (required): Time interval - `1m`, `5m`, or `15m`
- `start_time` (optional): Start time in ISO format or Unix timestamp
- `end_time` (optional): End time in ISO format or Unix timestamp
- `limit` (optional, default: 100): Max number of klines to return

**Response:**
```json
[
  {
    "symbol": "BTCUSDT",
    "timestamp": "2024-01-15T10:30:00.000Z",
    "open": "43200.00",
    "high": "43350.00",
    "low": "43180.00",
    "close": "43250.00",
    "volume": "125.50",
    "quote_volume": "5425000.00",
    "trade_count": 1250,
    "vwap": "43225.00",
    "buy_count": 680,
    "sell_count": 570,
    "buy_ratio": 0.544
  }
]
```

**Examples:**
```bash
# Get latest 1-minute klines
curl "http://localhost:8000/api/v1/analytics/klines/BTCUSDT?interval=1m&limit=100"

# Get klines for a specific time range
curl "http://localhost:8000/api/v1/analytics/klines/ETHUSDT?interval=5m&start_time=2024-01-15T00:00:00Z&end_time=2024-01-15T12:00:00Z"

# Get last 50 15-minute klines
curl "http://localhost:8000/api/v1/analytics/klines/BNBUSDT?interval=15m&limit=50"
```

**Storage Tier Routing:**
- Data < 1 hour old: Served from **Redis** (< 1ms latency)
- Data > 1 hour old: Served from **PostgreSQL** (< 50ms latency)

---

### Get Trade Count Aggregations

Get aggregated trade counts by symbol and time period.

**Endpoint:** `GET /api/v1/analytics/trades-count`

**Query Parameters:**
- `symbol` (optional): Filter by specific symbol
- `start_time` (optional): Start time
- `end_time` (optional): End time
- `limit` (optional, default: 100): Max results

**Response:**
```json
[
  {
    "symbol": "BTCUSDT",
    "timestamp": "2024-01-15T10:30:00.000Z",
    "trade_count": 1250,
    "buy_count": 680,
    "sell_count": 570
  }
]
```

**Example:**
```bash
curl "http://localhost:8000/api/v1/analytics/trades-count?symbol=BTCUSDT&limit=50"
```

## Alert Endpoints

### Get Price Spike Alerts

Get alerts for significant price movements (> 2% change in 1 minute).

**Endpoint:** `GET /api/v1/analytics/alerts/price-spikes`

**Query Parameters:**
- `symbol` (optional): Filter by specific symbol
- `min_change_percent` (optional, default: 2.0): Minimum price change percentage
- `start_time` (optional): Start time
- `end_time` (optional): End time
- `limit` (optional, default: 100): Max results

**Response:**
```json
[
  {
    "symbol": "BTCUSDT",
    "timestamp": "2024-01-15T10:30:00.000Z",
    "alert_type": "price_spike",
    "price_change_percent": 3.45,
    "open": "43200.00",
    "close": "44690.00",
    "message": "Price spike detected: +3.45%"
  }
]
```

**Example:**
```bash
curl "http://localhost:8000/api/v1/analytics/alerts/price-spikes?min_change_percent=3.0&limit=20"
```

---

### Get Volume Spike Alerts

Get alerts for unusual trading volume (quote volume > $1M in 1 minute).

**Endpoint:** `GET /api/v1/analytics/alerts/volume-spikes`

**Query Parameters:**
- `symbol` (optional): Filter by specific symbol
- `min_volume` (optional, default: 1000000): Minimum quote volume threshold
- `start_time` (optional): Start time
- `end_time` (optional): End time
- `limit` (optional, default: 100): Max results

**Response:**
```json
[
  {
    "symbol": "ETHUSDT",
    "timestamp": "2024-01-15T10:30:00.000Z",
    "alert_type": "volume_spike",
    "quote_volume": "2500000.00",
    "trade_count": 3500,
    "message": "Volume spike detected: $2.5M"
  }
]
```

**Example:**
```bash
curl "http://localhost:8000/api/v1/analytics/alerts/volume-spikes?symbol=ETHUSDT&limit=20"
```

---

### Get Whale Trade Alerts

Get alerts for large trades (> $100k per trade).

**Endpoint:** `GET /api/v1/analytics/alerts/whale-trades`

**Query Parameters:**
- `symbol` (optional): Filter by specific symbol
- `min_amount` (optional, default: 100000): Minimum trade amount in USD
- `start_time` (optional): Start time
- `end_time` (optional): End time
- `limit` (optional, default: 100): Max results

**Response:**
```json
[
  {
    "symbol": "BTCUSDT",
    "timestamp": "2024-01-15T10:30:45.123Z",
    "alert_type": "whale_trade",
    "trade_amount_usd": "250000.00",
    "price": "43250.00",
    "quantity": "5.78",
    "side": "buy",
    "message": "Whale trade detected: $250k BUY"
  }
]
```

**Example:**
```bash
curl "http://localhost:8000/api/v1/analytics/alerts/whale-trades?min_amount=200000&limit=10"
```

## System Endpoints

### Health Check

Check the overall system health and component status.

**Endpoint:** `GET /api/v1/system/health`

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:45.123Z",
  "components": {
    "redis": {
      "status": "healthy",
      "latency_ms": 0.5
    },
    "postgres": {
      "status": "healthy",
      "latency_ms": 2.3
    },
    "kafka": {
      "status": "healthy",
      "topics": ["binance.trades", "binance.tickers"]
    }
  },
  "uptime_seconds": 86400
}
```

**Example:**
```bash
curl http://localhost:8000/api/v1/system/health
```

---

### Prometheus Metrics

Get Prometheus-formatted metrics for monitoring.

**Endpoint:** `GET /metrics`

**Response:** (Prometheus text format)
```
# HELP api_requests_total Total API requests
# TYPE api_requests_total counter
api_requests_total{method="GET",endpoint="/api/v1/market/realtime"} 12500

# HELP api_request_duration_seconds API request duration
# TYPE api_request_duration_seconds histogram
api_request_duration_seconds_bucket{le="0.005"} 10000
api_request_duration_seconds_bucket{le="0.01"} 11500
```

**Example:**
```bash
curl http://localhost:8000/metrics
```

## Query Parameters

### Time Parameters

Time parameters accept multiple formats:

**ISO 8601 format:**
```
2024-01-15T10:30:00Z
2024-01-15T10:30:00.000Z
2024-01-15T10:30:00+00:00
```

**Unix timestamp (seconds):**
```
1705318200
```

**Unix timestamp (milliseconds):**
```
1705318200000
```

### Pagination

Use `limit` and `offset` for pagination:

```bash
# First page (items 1-100)
curl "http://localhost:8000/api/v1/analytics/klines/BTCUSDT?interval=1m&limit=100"

# Second page (items 101-200)
curl "http://localhost:8000/api/v1/analytics/klines/BTCUSDT?interval=1m&limit=100&offset=100"
```

## Response Format

### Success Response

All successful responses return JSON with appropriate HTTP status codes:

- `200 OK`: Successful request
- `201 Created`: Resource created
- `204 No Content`: Successful request with no content

### Error Response

Error responses include details about what went wrong:

```json
{
  "error": {
    "code": "INVALID_SYMBOL",
    "message": "Symbol 'INVALID' not found",
    "details": {
      "valid_symbols": ["BTCUSDT", "ETHUSDT", "..."]
    }
  },
  "timestamp": "2024-01-15T10:30:45.123Z"
}
```

## Error Handling

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 400 | Bad Request - Invalid parameters |
| 404 | Not Found - Resource doesn't exist |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error - Server error |
| 503 | Service Unavailable - Dependency unavailable |

### Common Errors

**Invalid Symbol:**
```json
{
  "error": {
    "code": "INVALID_SYMBOL",
    "message": "Symbol 'XYZ' is not tracked"
  }
}
```

**Invalid Time Range:**
```json
{
  "error": {
    "code": "INVALID_TIME_RANGE",
    "message": "start_time must be before end_time"
  }
}
```

**Rate Limit Exceeded:**
```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Too many requests. Limit: 100 requests/minute"
  }
}
```

## Python Client Example

```python
import requests
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000/api/v1"

# Get real-time market data
response = requests.get(f"{BASE_URL}/market/realtime")
tickers = response.json()
print(f"Tracking {len(tickers)} symbols")

# Get BTCUSDT 1-minute klines for the last hour
end_time = datetime.utcnow()
start_time = end_time - timedelta(hours=1)

response = requests.get(
    f"{BASE_URL}/analytics/klines/BTCUSDT",
    params={
        "interval": "1m",
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat()
    }
)
klines = response.json()
print(f"Received {len(klines)} klines")

# Get recent price spike alerts
response = requests.get(
    f"{BASE_URL}/analytics/alerts/price-spikes",
    params={"limit": 20, "min_change_percent": 3.0}
)
alerts = response.json()
print(f"Found {len(alerts)} price spikes > 3%")
```

## Next Steps

- [Setup Guide](SETUP.md) - Deploy and configure the pipeline
- [Technical Documentation](TECHNICAL.md) - Architecture and design details
- [Main README](../README.md) - Back to overview
