# Binance-Kafka Connector

Real-time data ingestion pipeline connecting Binance WebSocket API to Kafka.

## Overview

This service connects to Binance WebSocket streams to collect cryptocurrency market data (trades, klines, tickers) and publishes them to Kafka topics with proper validation, enrichment, and batching.

The system supports two data paths:
- **Hot Path (Real-time)**: Ticker data flows directly from Kafka to Redis for ultra-low latency (~100-200ms)
- **Analytics Path**: Trade and kline data flows through Spark for aggregation and analysis (~1-5 seconds)

## Installation

```bash
pip install -e .
```

For development:

```bash
pip install -e ".[dev]"
```

## Configuration

Configure the connector using environment variables. All settings have sensible defaults.

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| **WebSocket Configuration** |
| `BINANCE_WS_URL` | Binance WebSocket endpoint | `wss://stream.binance.com:9443/stream` | No |
| `BINANCE_STREAMS` | Comma-separated list of streams to subscribe | `btcusdt@trade,ethusdt@trade,bnbusdt@trade,btcusdt@kline_1m,ethusdt@kline_1m,btcusdt@ticker` | No |
| `PING_INTERVAL_SECONDS` | Keepalive ping interval | `180` | No |
| `RECONNECT_MAX_DELAY_SECONDS` | Maximum reconnection delay | `60` | No |
| `WS_CONNECTION_TIMEOUT` | Connection timeout in seconds | `10` | No |
| **Kafka Configuration** |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` | No |
| `KAFKA_COMPRESSION` | Compression type (snappy, gzip, lz4) | `snappy` | No |
| `KAFKA_ACKS` | Acknowledgment level (0, 1, all) | `1` | No |
| **Kafka Topics** |
| `TOPIC_RAW_TRADES` | Topic for trade events | `raw_trades` | No |
| `TOPIC_RAW_KLINES` | Topic for kline/candlestick data | `raw_klines` | No |
| `TOPIC_RAW_TICKERS` | Topic for ticker statistics | `raw_tickers` | No |
| **Batching Configuration** |
| `BATCH_SIZE` | Maximum messages per batch | `100` | No |
| `BATCH_TIMEOUT_MS` | Maximum batch age in milliseconds | `100` | No |
| `BATCH_CHECK_INTERVAL_MS` | Batch check interval in milliseconds | `10` | No |
| **Logging** |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` | No |

### Example Configuration

Create a `.env` file for local development:

```bash
# WebSocket Configuration
BINANCE_WS_URL=wss://stream.binance.com:9443/stream
BINANCE_STREAMS=btcusdt@trade,ethusdt@trade,bnbusdt@trade,btcusdt@kline_1m,ethusdt@kline_1m,btcusdt@ticker
PING_INTERVAL_SECONDS=180
RECONNECT_MAX_DELAY_SECONDS=60
WS_CONNECTION_TIMEOUT=10

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_COMPRESSION=snappy
KAFKA_ACKS=1

# Kafka Topics
TOPIC_RAW_TRADES=raw_trades
TOPIC_RAW_KLINES=raw_klines
TOPIC_RAW_TICKERS=raw_tickers

# Batching Configuration
BATCH_SIZE=100
BATCH_TIMEOUT_MS=100
BATCH_CHECK_INTERVAL_MS=10

# Logging
LOG_LEVEL=INFO
```

## Usage

### Running with Docker (Recommended)

The easiest way to run the connector is using Docker Compose, which includes Kafka and Zookeeper:

```bash
# Start all services (Zookeeper, Kafka, and Connector)
docker-compose up -d

# View logs
docker-compose logs -f binance-connector

# Stop all services
docker-compose down
```

The docker-compose setup includes:
- **Zookeeper**: Kafka coordination service (port 2181)
- **Kafka**: Message broker (ports 9092, 9093)
- **Binance Connector**: The data ingestion service

For detailed Docker deployment instructions, monitoring, troubleshooting, and production configuration, see [DOCKER.md](DOCKER.md).

#### Customizing Docker Configuration

Edit the `docker-compose.yml` file to customize environment variables:

```yaml
binance-connector:
  environment:
    BINANCE_STREAMS: "btcusdt@trade,ethusdt@trade"  # Customize streams
    BATCH_SIZE: "200"                                # Adjust batch size
    LOG_LEVEL: "DEBUG"                               # Change log level
```

#### Building the Docker Image

To build the Docker image manually:

```bash
docker build -t binance-kafka-connector:latest .
```

### Running Locally

Run the connector as a module (requires local Kafka):

```bash
python -m binance_kafka_connector
```

Or programmatically:

```python
import asyncio
from binance_kafka_connector import BinanceKafkaConnector

async def main():
    connector = BinanceKafkaConnector()
    await connector.run()

if __name__ == "__main__":
    asyncio.run(main())
```

The connector will:
1. Connect to Binance WebSocket API
2. Subscribe to configured streams
3. Process and validate incoming messages
4. Batch messages for efficient Kafka production
5. Send batches to appropriate Kafka topics
6. Handle reconnections and errors gracefully

### Graceful Shutdown

The connector handles SIGINT (Ctrl+C) and SIGTERM signals gracefully:
- Stops accepting new messages
- Flushes all pending batches to Kafka
- Closes connections cleanly

## Running Tests

```bash
pytest
```

Run with coverage:

```bash
pytest --cov=binance_kafka_connector
```

## Project Structure

```
src/
├── binance_kafka_connector/   # Binance WebSocket to Kafka connector
│   ├── __init__.py
│   ├── config.py              # Configuration management
│   ├── models.py              # Pydantic data models
│   ├── websocket_client.py    # WebSocket connection manager
│   ├── message_processor.py   # Message parsing and enrichment
│   ├── message_batcher.py     # Message batching logic
│   ├── kafka_producer.py      # Kafka producer client
│   └── connector.py           # Main application orchestrator
│
├── ticker_consumer/           # Real-time ticker consumer (Hot Path)
│   ├── __init__.py
│   ├── config.py              # Ticker consumer configuration
│   ├── consumer.py            # Kafka consumer core
│   ├── validator.py           # Ticker data validation
│   ├── late_data_handler.py   # Late/stale data handling
│   ├── error_handler.py       # Error handling and reconnection
│   ├── healthcheck.py         # Docker health check
│   └── main.py                # Service entrypoint
│
├── storage/                   # Storage layer
│   ├── redis_ticker_storage.py  # Redis ticker storage (Hot Path)
│   ├── redis_storage.py         # General Redis storage
│   ├── postgres_storage.py      # PostgreSQL storage (Warm Path)
│   └── minio_storage.py         # MinIO storage (Cold Path)
│
├── api/                       # REST API
│   ├── main.py                # FastAPI application
│   └── routers/
│       ├── ticker.py          # Real-time ticker endpoints
│       ├── market.py          # Market data endpoints
│       └── analytics.py       # Analytics endpoints
│
└── streaming/                     # Spark streaming jobs
    ├── trade_aggregation_job.py
    ├── technical_indicators_job.py
    └── anomaly_detection_job.py

dags/                          # Airflow DAGs
├── streaming_processing_dag.py  # Main streaming pipeline
├── ticker_monitor_dag.py        # Ticker consumer monitoring
└── data_quality.py              # Data quality utilities

tests/
├── test_models.py
├── test_websocket_client.py
├── test_message_processor.py
├── test_message_batcher.py
├── test_kafka_producer.py
└── test_connector.py
```

## Real-time Ticker Service

The Real-time Ticker service provides ultra-low latency cryptocurrency price data by bypassing Spark processing and writing directly to Redis.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    REAL-TIME PATH (Hot Path)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Binance WS (@ticker) → Kafka (raw_tickers) → Ticker Consumer   │
│                                                    ↓             │
│                                               Redis Hash         │
│                                                    ↓             │
│                                            API Endpoints         │
│                                                                  │
│  Latency: ~100-200ms                                            │
└─────────────────────────────────────────────────────────────────┘
```

### Supported Symbols

By default, the system tracks 15 high market cap cryptocurrencies:
- BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT
- ADAUSDT, DOGEUSDT, AVAXUSDT, DOTUSDT, MATICUSDT
- LINKUSDT, LTCUSDT, UNIUSDT, ATOMUSDT, SHIBUSDT

### Ticker Consumer Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `TICKER_SYMBOLS` | Comma-separated list of symbols to track | 15 default symbols |
| `KAFKA_TICKER_TOPIC` | Kafka topic for ticker data | `raw_tickers` |
| `TICKER_CONSUMER_GROUP` | Kafka consumer group | `ticker-consumer-group` |
| `TICKER_TTL_SECONDS` | Redis TTL for ticker data | `60` |
| `TICKER_WATERMARK_DELAY_MS` | Late data watermark delay | `10000` |
| `TICKER_MAX_FUTURE_MS` | Max clock skew tolerance | `5000` |

### API Endpoints

| Endpoint | Description | Response Time |
|----------|-------------|---------------|
| `GET /api/v1/ticker/{symbol}` | Get single ticker data | < 50ms |
| `GET /api/v1/tickers` | Get all ticker data | < 100ms |
| `GET /api/v1/ticker/health` | Ticker service health | < 50ms |

### Example API Response

```json
{
  "symbol": "BTCUSDT",
  "last_price": "42000.00",
  "price_change": "500.00",
  "price_change_pct": "1.2",
  "open": "41500.00",
  "high": "42500.00",
  "low": "41000.00",
  "volume": "50000.00",
  "quote_volume": "2000000000.00",
  "trades_count": 18051,
  "updated_at": 1672515782136,
  "complete": true
}
```

### Redis Storage Format

Ticker data is stored in Redis Hashes with the key pattern `ticker:{SYMBOL}`:

```
Key: ticker:BTCUSDT
Fields:
  - last_price: "42000.00"
  - price_change: "500.00"
  - price_change_pct: "1.2"
  - open: "41500.00"
  - high: "42500.00"
  - low: "41000.00"
  - volume: "50000.00"
  - quote_volume: "2000000000.00"
  - trades_count: "18051"
  - event_time: "1672515782136"
  - updated_at: "1672515782200"
TTL: 60 seconds
```

### Late Data Handling

The ticker consumer implements a hybrid watermark + per-symbol tracking strategy:

1. **Watermark Check**: Rejects data older than 10 seconds (configurable)
2. **Per-Symbol Tracking**: Ensures newer data is never overwritten by older data
3. **Clock Skew Protection**: Rejects data from the future (> 5 seconds ahead)

### Monitoring

The `ticker_monitor_dag` Airflow DAG monitors the ticker service:
- Runs every 2 minutes
- Checks Redis and Kafka health
- Validates ticker data freshness
- Alerts if > 50% of tickers are missing or stale

## License

MIT
