# Technical Documentation

Deep dive into the architecture, design decisions, and technical implementation of the Real-Time Cryptocurrency Data Pipeline.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Configuration Management](#configuration-management)
- [Features](#features)
- [Storage Architecture](#storage-architecture)
- [Airflow DAGs](#airflow-dags)
- [Monitoring and Dashboards](#monitoring-and-dashboards)
- [Terminology Glossary](#terminology-glossary)

## Architecture Overview

![System Architecture](../img/system_architechture.png)

The system follows an **event-driven architecture** with the following layers:

### 1. Data Ingestion Layer
- **Binance WebSocket Connector**: Establishes WebSocket connections to Binance API
- Streams real-time trade and ticker data
- Handles automatic reconnection and error recovery
- Publishes raw events to Kafka topics

### 2. Message Broker Layer
- **Apache Kafka**: Central message bus for all streaming data
- Topics:
  - `binance.trades`: Individual trade events
  - `binance.tickers`: 24h ticker statistics
- Provides durability, ordering, and replay capabilities

### 3. Stream Processing Layer
- **Apache Spark Structured Streaming**: Consumes from Kafka topics
- Jobs:
  - **Trade Aggregation**: Computes 1-minute OHLCV klines with buy/sell metrics
  - **Anomaly Detection**: Identifies price spikes, volume spikes, and whale trades
  - **Volatility Prediction**: ML model for volatility forecasting
- Writes results to both storage tiers

### 4. Storage Layer (Two-Tier Architecture)

**Primary Storage - PostgreSQL/TimescaleDB:**
- Staging table + MERGE pattern for reliable upserts
- TimescaleDB hypertables for time-series optimization
- Automatic partitioning and compression
- SQL queries for analytics

**Cache Layer - Redis:**
- In-memory storage for real-time data (< 1 hour)
- TTL-based expiration
- Sub-millisecond read latency
- Used by dashboards and real-time queries

### 5. API Layer
- **FastAPI**: RESTful API with automatic OpenAPI docs
- **QueryRouter**: Intelligent tier selection based on time range
  - Recent data (< 1h) → Redis
  - Historical data (> 1h) → PostgreSQL
- Rate limiting and metrics collection

### 6. Orchestration Layer
- **Apache Airflow**: Manages all workflows
- DAGs:
  - Connector lifecycle management
  - Spark job scheduling
  - Health checks and monitoring
- Web UI for monitoring and manual triggers

### 7. Visualization & Monitoring Layer
- **Streamlit**: Interactive dashboard for market data visualization
- **Prometheus**: Metrics collection from all components
- **Grafana**: System metrics monitoring dashboards
- Metrics: throughput, latency, error rates, resource usage

## Tech Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| Message Broker | Apache Kafka | 3.6+ | Real-time event streaming |
| Stream Processing | Apache Spark (PySpark) | 3.5+ | Trade aggregation, anomaly detection |
| Cache | Redis | 7.2+ | Real-time data cache (< 1 hour) |
| Database | PostgreSQL | 16+ | Relational storage |
| Time-Series Extension | TimescaleDB | 2.13+ | Time-series optimization |
| API Framework | FastAPI | 0.109+ | REST API with OpenAPI docs |
| Visualization | Streamlit | Latest | Interactive market data dashboard |
| Orchestration | Apache Airflow | 2.8+ | Workflow management |
| Monitoring | Prometheus | Latest | Metrics collection |
| System Dashboards | Grafana | 10.0+ | Infrastructure monitoring |
| Containerization | Docker / Docker Compose | Latest | Local deployment |
| Language | Python | 3.11+ | Primary language |

### Key Python Libraries

- **kafka-python**: Kafka producer/consumer
- **pyspark**: Spark streaming jobs
- **redis**: Redis client
- **psycopg2**: PostgreSQL driver
- **sqlalchemy**: ORM and database abstraction
- **fastapi**: Web framework
- **uvicorn**: ASGI server
- **streamlit**: Interactive data visualization
- **prometheus-client**: Metrics export
- **websockets**: Binance WebSocket client

## Configuration Management

### Centralized Configuration: util/constant.py

All project configurations are centralized in [util/constant.py](../util/constant.py). This single-file approach simplifies configuration management and makes it easy to understand all system settings at a glance.

**Configuration Categories:**

#### 1. Service Connections

```python
# Redis
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0

# Kafka
KAFKA_SERVER = "kafka:29092"
TOPIC_TRADE = "raw_trades"
TOPIC_TICKER = "raw_tickers"

# PostgreSQL
POSTGRES_HOST = "postgres-data"
POSTGRES_PORT = 5432
POSTGRES_USER = "crypto"
POSTGRES_PASSWORD = "crypto"
POSTGRES_DB = "crypto_data"
```

#### 2. Trading Configuration

```python
# Default trading symbols (40 pairs)
DEFAULT_SYMBOL = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT", "SOLUSDT",
    # ... 35+ more pairs
]

# Valid intervals for kline aggregation
VALID_INTERVAL = {"1m", "5m", "15m", "1h"}

# Cache and data retention
CACHE_HOUR = 1                    # Redis cache duration
MAX_TIME_RANGE_DAY = 365          # Max historical query range
```

#### 3. Redis Key Patterns & TTLs

```python
class RedisKey:
    AGG = "agg"          # Aggregated kline data
    TICKER = "ticker"    # 24h ticker data
    TRADE = "trade"      # Raw trade data
    ALERT = "alert"      # Anomaly alerts

class RedisTTL:
    AGG = 3600       # 1 hour for aggregated data
    TICKER = 60      # 1 minute for tickers
    TRADE = 300      # 5 minutes for trades
    ALERT = 86400    # 24 hours for alerts
```

#### 4. Anomaly Detection Thresholds

```python
# Thresholds for anomaly detection
VOLUME_THRESHOLD = 1_000_000.0         # Quote volume > $1M
PRICE_CHANGE_THRESHOLD = 2.0           # Price change > 2%
TRADE_COUNT_MULTIPLIER = 3.0           # Trade count spike detection
BUY_RATIO_LOW = 0.3                    # Buy imbalance lower bound
BUY_RATIO_HIGH = 0.7                   # Buy imbalance upper bound

# Alert types and severity levels
VALID_ALERT_TYPE = [
    "VOLUME_SPIKE",
    "PRICE_SPIKE",
    "TRADE_COUNT_SPIKE",
    "BUY_SELL_IMBALANCE"
]
VALID_ALERT_LEVEL = ["HIGH", "MEDIUM", "LOW"]
```

#### 5. Machine Learning Features

```python
# Features used for volatility prediction model
FEATURE_COLUMN = [
    "close",                # Closing price
    "volume",               # Volume traded
    "quote_volume",         # Total value traded
    "trade_count",          # Number of trades
    "return_5m",            # 5-minute price return
    "return_15m",           # 15-minute price return
    "volatility_5m",        # 5-minute rolling volatility
    "volatility_15m",       # 15-minute rolling volatility
    "volatility_30m",       # 30-minute rolling volatility
    "volatility_60m",       # 60-minute rolling volatility
    "volatility_ratio",     # Current vs historical volatility
    "price_range_pct",      # (high - low) / close × 100
    "volume_ratio_60m",     # Current vs 60-min average volume
    "buy_ratio",            # buy_count / total_trades
    "buy_sell_imbalance",   # (buy - sell) / total_trades
    "price_vs_ma_15m",      # Price deviation from 15-min MA
    "price_vs_ma_60m",      # Price deviation from 60-min MA
    "hour",                 # Hour of day (0-23)
    "symbol_encoded",       # Encoded symbol identifier
]

# Model artifacts
MODEL_DIR = "model"
MODEL_FILE = "volatility_predictor.json"
```

#### 6. API Configuration

```python
# API settings
API_URL = "http://localhost:8000"
RATE_LIMIT = 100                 # Requests per window
RATE_LIMIT_WINDOW = 60           # Window duration in seconds
```

#### 7. Spark Configuration

```python
# Spark streaming settings
SPARK_CHECKPOINT = "/opt/airflow/data/spark-checkpoints/trade-agg"
PYSPARK_PYTHON = "/usr/local/bin/python3.11"

# Job identifiers
JOB_TRADE_AGG = "trade_aggregation"
JOB_ANOMALY = "anomaly_detection"
JOB_VOLATILITY = "volatility_prediction"

# Processing parameters
BATCH_SIZE = 100                 # Records per batch
MAX_RUNTIME = 180                # Max job runtime in seconds
LOOKBACK_HOUR = 2                # Lookback window for data processing
```

### Configuration Best Practices

1. **Single Source of Truth**: All constants in one file
2. **Type Safety**: Use Python type hints where appropriate
3. **Documentation**: Inline comments explain each setting
4. **Grouping**: Related configs grouped together
5. **Naming Convention**: UPPER_CASE for constants, PascalCase for classes

### Modifying Configuration

To change any setting:

1. Edit [util/constant.py](../util/constant.py)
2. Restart affected services:
   ```bash
   docker-compose restart <service-name>
   ```
3. For comprehensive changes, restart all services:
   ```bash
   docker-compose restart
   ```

**Note:** Some changes (like Spark configurations) may require clearing checkpoints:
```bash
rm -rf data/spark-checkpoints/*
docker-compose restart
```

## Features

### Real-Time Data Ingestion

**WebSocket Connection:**
- Connects to Binance WebSocket API (`wss://stream.binance.com:9443`)
- Subscribes to multiple streams:
  - Trade streams: `@trade`
  - 24h ticker streams: `@ticker`
- Handles 100-1,000+ messages/second depending on market activity

**Supported Trading Pairs (15+ by default):**
- BTCUSDT, ETHUSDT, BNBUSDT, ADAUSDT, SOLUSDT
- XRPUSDT, DOTUSDT, DOGEUSDT, MATICUSDT, LTCUSDT
- LINKUSDT, UNIUSDT, AVAXUSDT, ATOMUSDT, SHIBUSDT

**Message Enrichment:**
- Adds ingestion timestamps
- Validates message schema
- Handles malformed messages gracefully

**Reliability Features:**
- Automatic reconnection on connection loss
- Exponential backoff retry strategy
- Health check monitoring
- Graceful shutdown handling

### Stream Processing

#### 1. Trade Aggregation Job

Computes 1-minute OHLCV (candlestick) data from raw trade events:

**Aggregations:**
- **OHLC**: Open, High, Low, Close prices
- **Volume**: Total base asset quantity
- **Quote Volume**: Total quote asset value (price × quantity)
- **Trade Count**: Number of trades
- **VWAP**: Volume-weighted average price

**Order Flow Metrics:**
- **Buy Count / Sell Count**: Buyer vs seller-initiated trades
- **Buy Ratio**: buy_count / total_count
- **Buy/Sell Imbalance**: (buy_count - sell_count) / total_count

**Processing:**
- Tumbling windows: 1 minute
- Watermark: 30 seconds (handles late arrivals)
- Output: Writes to both PostgreSQL and Redis

#### 2. Anomaly Detection Job

Identifies unusual market events in real-time:

**Whale Trade Alerts:**
- Threshold: Single trade > $100,000
- Captures: Symbol, timestamp, amount, price, side (buy/sell)

**Volume Spike Alerts:**
- Threshold: Quote volume > $1,000,000 in 1 minute
- Indicates: High trading activity or market event

**Price Spike Alerts:**
- Threshold: Price change > 2% in 1 minute
- Detects: Rapid price movements (pumps/dumps)

**Processing:**
- Real-time evaluation on streaming data
- Writes alerts to PostgreSQL `anomaly_alerts` table
- Available via REST API for dashboards

#### 3. Volatility Prediction Job

Machine learning model for forecasting short-term volatility:

**Features:**
- Price range percentage
- Volume ratios (current vs moving average)
- Rolling volatility (5m, 15m, 30m, 60m windows)
- Price deviation from moving average

**Model:**
- Lightweight Random Forest regressor
- Trained on historical kline data
- Updates predictions every minute

**Output:**
- Volatility forecast for next 1-5 minutes
- Used for risk assessment and trading signals

### Storage Architecture

#### Two-Tier Design Rationale

```
Kafka → Spark Streaming ─┬→ PostgreSQL/TimescaleDB (Primary)
                         └→ Redis (Cache)
```

| Aspect | PostgreSQL/TimescaleDB | Redis |
|--------|------------------------|-------|
| **Purpose** | Long-term storage, analytics | Real-time dashboards |
| **Data Retention** | Unlimited (compressed after 7 days) | 1 hour (TTL-based) |
| **Write Pattern** | Staging + MERGE (handles duplicates) | Direct write (last-write-wins) |
| **Read Latency** | < 50ms | < 1ms |
| **Query Capability** | Full SQL, aggregations, joins | Key-value lookup |
| **Storage Cost** | Low (compressed) | High (in-memory) |

#### PostgreSQL/TimescaleDB Schema

**Tables:**

1. **klines_1m** (TimescaleDB hypertable)
   - Partitioned by time (daily chunks)
   - Columns: symbol, timestamp, open, high, low, close, volume, quote_volume, trade_count, vwap, buy_count, sell_count, buy_ratio
   - Indexes: (symbol, timestamp), (symbol)

2. **klines_1m_staging** (Regular table)
   - Same schema as klines_1m
   - Used for staging incoming data before MERGE

3. **anomaly_alerts**
   - Columns: symbol, timestamp, alert_type, value, message
   - Indexes: (symbol, timestamp), (alert_type)

**MERGE Pattern:**
```sql
-- Stage incoming data
INSERT INTO klines_1m_staging VALUES (...);

-- Merge into main table (handles duplicates)
INSERT INTO klines_1m
SELECT * FROM klines_1m_staging
ON CONFLICT (symbol, timestamp)
DO UPDATE SET
  close = EXCLUDED.close,
  high = GREATEST(klines_1m.high, EXCLUDED.high),
  low = LEAST(klines_1m.low, EXCLUDED.low),
  volume = klines_1m.volume + EXCLUDED.volume,
  ...;

-- Clear staging
TRUNCATE klines_1m_staging;
```

**Compression Policy:**
```sql
SELECT add_compression_policy('klines_1m', INTERVAL '7 days');
```

**Continuous Aggregates:**
```sql
-- 5-minute aggregates
CREATE MATERIALIZED VIEW klines_5m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 minutes', timestamp) AS timestamp,
  symbol,
  first(open, timestamp) AS open,
  max(high) AS high,
  min(low) AS low,
  last(close, timestamp) AS close,
  sum(volume) AS volume,
  sum(quote_volume) AS quote_volume
FROM klines_1m
GROUP BY time_bucket('5 minutes', timestamp), symbol;
```

#### Redis Schema

**Key Patterns:**

- `ticker:{symbol}` - Latest ticker data (hash)
- `kline:1m:{symbol}:{timestamp}` - 1-minute kline (hash)
- `recent:klines:{symbol}` - Sorted set of recent klines

**TTL:**
- All keys expire after 3600 seconds (1 hour)
- Refreshed on every write

**Example:**
```
HSET ticker:BTCUSDT price 43250.50 volume 125000.50 ...
EXPIRE ticker:BTCUSDT 3600
```

### REST API

See [API.md](API.md) for complete endpoint documentation.

**Key Features:**
- Automatic query routing based on time range
- Rate limiting (100 req/min)
- OpenAPI docs at `/docs`
- Prometheus metrics at `/metrics`

## Airflow DAGs

### 1. Binance Connector DAG

**Schedule:** Manual trigger
**Purpose:** Manage WebSocket connection lifecycle

![Binance Connector DAG](../img/dag1.png)

**Tasks:**

1. **check_kafka_health**
   - Type: PythonOperator
   - Verifies Kafka broker connectivity
   - Creates topics if not exist

2. **run_binance_connector**
   - Type: BashOperator
   - Starts WebSocket client
   - Runs continuously until manually stopped
   - Command: `python ingestion/connector.py`

3. **run_ticker_consumer**
   - Type: BashOperator
   - Consumes ticker data from Kafka → Redis
   - Runs in parallel with connector
   - Command: `python ingestion/ticker_consumer.py`

**Configuration:**
```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'binance_connector_dag',
    default_args=default_args,
    description='Binance WebSocket connector',
    schedule_interval=None,  # Manual trigger
    catchup=False,
)
```

### 2. Streaming Processing DAG

**Schedule:** Every 5 minutes
**Purpose:** Execute Spark streaming jobs

![Streaming Processing DAG](../img/dag2.png)

**Tasks:**

1. **health_checks**
   - Type: PythonOperator
   - Verifies Redis and PostgreSQL connectivity
   - Checks Kafka consumer lag

2. **trade_aggregation**
   - Type: BashOperator
   - Spark job: Trade → OHLCV aggregation
   - Command: `spark-submit processing/trade_aggregation_job.py`
   - Duration: ~2-3 minutes

3. **anomaly_detection**
   - Type: BashOperator
   - Spark job: Anomaly detection
   - Command: `spark-submit processing/anomaly_detection_job.py`
   - Duration: ~1-2 minutes

4. **volatility_prediction**
   - Type: BashOperator
   - Spark job: ML volatility forecasting
   - Command: `spark-submit processing/volatility_prediction_job.py`
   - Duration: ~2-3 minutes

5. **cleanup_streaming**
   - Type: PythonOperator
   - Cleans up Spark checkpoints
   - Removes old temporary files

**Configuration:**
```python
dag = DAG(
    'streaming_processing_dag',
    default_args=default_args,
    description='Spark streaming jobs',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
)
```

## Monitoring and Dashboards

The project uses **two separate visualization systems**:

### 1. Streamlit Dashboard (Main Visualization)

**Access:** http://localhost:8501

**Purpose:** Interactive market data visualization and analysis

**Features:**
- **Real-time Market Data:**
  - Current prices, volumes, and 24h changes
  - Top gainers/losers
  - Market summary statistics

- **OHLCV Charts:**
  - Interactive candlestick charts
  - Multiple timeframes (1m, 5m, 15m)
  - Volume overlays

- **Trading Analytics:**
  - Buy/sell ratio visualization
  - Trade count analysis
  - Price and volume trends

- **Anomaly Alerts:**
  - Price spike notifications
  - Volume spike alerts
  - Whale trade detection

- **Symbol Deep Dive:**
  - Detailed analysis for individual trading pairs
  - Historical data exploration
  - Technical indicators

**Technology:** Built with Streamlit for rapid prototyping and interactive data apps.

![Streamlit Dashboard Preview](../img/dashboard1.png)

---

### 2. Grafana + Prometheus (System Metrics)

**Access:** http://localhost:3000

**Purpose:** Infrastructure monitoring and system performance tracking

**Dashboards:**

#### System Health Dashboard

**Panels:**
- Component health status (Redis, PostgreSQL, Kafka)
- API request rate and latency
- Error rates by endpoint
- Kafka consumer lag
- Redis memory usage
- PostgreSQL query performance
- Container resource usage (CPU, memory, disk)

![System Health Dashboard](../img/dashboard4.png)

### Prometheus Metrics

**API Metrics:**
- `api_requests_total{method, endpoint}` - Total requests
- `api_request_duration_seconds{method, endpoint}` - Request latency histogram
- `api_errors_total{method, endpoint, error_type}` - Error count

**Storage Metrics:**
- `redis_write_success_total` - Successful Redis writes
- `redis_write_failure_total` - Failed Redis writes
- `postgres_write_success_total` - Successful PostgreSQL writes
- `postgres_write_failure_total` - Failed PostgreSQL writes

**Kafka Metrics:**
- `kafka_consumer_lag{topic, partition}` - Consumer lag
- `kafka_messages_consumed_total{topic}` - Messages consumed

**Spark Metrics:**
- `spark_streaming_batch_duration_seconds` - Batch processing time
- `spark_streaming_records_processed_total` - Records processed

## Terminology Glossary

### Market Data Terms

**Kline**
Binance term for candlestick/OHLCV data representing price action in a time interval.

**OHLCV**
Open, High, Low, Close, Volume - standard format for price data:
- **Open**: First trade price in the interval
- **High**: Highest trade price in the interval
- **Low**: Lowest trade price in the interval
- **Close**: Last trade price in the interval
- **Volume**: Total quantity traded in the interval

**VWAP**
Volume Weighted Average Price - average price weighted by trade volume.
Formula: `VWAP = Σ(price × volume) / Σ(volume)`

**Quote Volume**
Total value traded (sum of price × quantity for all trades).
Also known as "turnover" or "notional volume".

**Moving Average (MA)**
Rolling average of prices over a time window (e.g., 5-minute MA, 15-minute MA).

### Order Flow Metrics

**Buy/Sell Count**
Number of buyer-initiated vs seller-initiated trades.
- Buyer-initiated: Market buy order (aggressive)
- Seller-initiated: Market sell order (aggressive)

**Buy Ratio**
`buy_count / total_count` - Proportion of buyer-initiated trades.
Values > 0.5 indicate buying pressure.

**Buy/Sell Imbalance**
`(buy_count - sell_count) / total_count`
Range: -1 to +1. Positive = buying pressure, Negative = selling pressure.

### Machine Learning Features

**Price Range Percentage**
`(high - low) / close × 100` - Measures volatility within the interval.

**Price Body Percentage**
`|close - open| / close × 100` - Size of candlestick body.

**Volatility**
Standard deviation of price returns over rolling windows (5m, 15m, 30m, 60m).

**Volume Ratio**
`current_volume / moving_average_volume` - Detects unusual volume.

**Price vs MA**
`(price - moving_average) / moving_average × 100` - Price deviation from trend.

### Anomaly Types

**Whale Trade**
Single trade with value > $100,000 USD. Indicates large institutional or individual player.

**Volume Spike**
Quote volume > $1,000,000 USD in 1 minute. Signals high trading activity.

**Price Spike**
Price change > 2% in 1 minute. Indicates rapid price movement (pump or dump).

### Technical Terms

**Watermark**
In stream processing, the maximum delay tolerated for late-arriving events.
Our watermark: 30 seconds.

**Tumbling Window**
Non-overlapping fixed-size time windows (e.g., 1-minute windows: 10:00-10:01, 10:01-10:02).

**Hypertable**
TimescaleDB concept - a table that is automatically partitioned by time.

**Continuous Aggregate**
Materialized view that incrementally updates as new data arrives (TimescaleDB feature).

## Next Steps

- [Setup Guide](SETUP.md) - Deploy and configure the pipeline
- [API Documentation](API.md) - REST API reference
- [Main README](../README.md) - Back to overview
