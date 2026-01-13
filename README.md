# Real-Time Cryptocurrency Data Pipeline

A production-grade data engineering project that ingests, processes, and visualizes real-time cryptocurrency market data from Binance. Built with modern streaming technologies using PostgreSQL/TimescaleDB for persistent storage and Redis for real-time caching.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Storage Architecture](#storage-architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Airflow DAGs](#airflow-dags)
- [API Endpoints](#api-endpoints)
- [Monitoring and Dashboards](#monitoring-and-dashboards)
- [Configuration](#configuration)
- [Testing](#testing)

## Overview

This project demonstrates a complete real-time data pipeline designed to handle high-throughput streaming data (100-1,000+ messages/second) from cryptocurrency exchanges. The pipeline:

1. Connects to Binance WebSocket API to receive live trade and ticker data
2. Streams data through Apache Kafka for reliable message delivery
3. Processes data using Apache Spark Structured Streaming for aggregations and anomaly detection
4. Stores data in a **two-tier storage architecture**:
   - **PostgreSQL/TimescaleDB**: Primary storage with staging + MERGE pattern for reliable upserts
   - **Redis**: Real-time cache for sub-millisecond access
5. Exposes data through a FastAPI REST API with automatic tier routing
6. Visualizes metrics and data through Grafana dashboards
7. Orchestrates all workflows using Apache Airflow

## Architecture

![System Architecture](img/system_architechture.png)

The system follows an event-driven architecture with the following components:

- **Data Ingestion Layer**: Binance WebSocket connector pushes real-time data to Kafka
- **Stream Processing Layer**: Spark Structured Streaming jobs consume from Kafka, compute aggregations, and detect anomalies
- **Storage Layer**: PostgreSQL/TimescaleDB (primary) + Redis (cache)
- **API Layer**: FastAPI service with intelligent query routing based on time range
- **Orchestration Layer**: Airflow manages all pipeline workflows and health checks
- **Monitoring Layer**: Prometheus metrics with Grafana dashboards

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Message Broker | Apache Kafka | Real-time event streaming |
| Stream Processing | Apache Spark (PySpark) | Trade aggregation, anomaly detection |
| Cache | Redis | Real-time data cache (< 1 hour) |
| Database | PostgreSQL/TimescaleDB | Primary storage with staging + MERGE |
| API Framework | FastAPI | REST API with OpenAPI docs |
| Orchestration | Apache Airflow | Workflow management |
| Monitoring | Prometheus + Grafana | Metrics and visualization |
| Containerization | Docker Compose | Local development and deployment |

## Features

### Real-Time Data Ingestion
- WebSocket connection to Binance with automatic reconnection
- High-throughput message processing (100-1,000+ messages/second depending on market activity)
- Support for multiple trading pairs (15+ symbols by default)
- Trade and ticker data streams
- Message enrichment with ingestion timestamps

### Stream Processing
- 1-minute OHLCV (Open, High, Low, Close, Volume) candle aggregation
- Derived metrics: VWAP, price change percentage, buy/sell ratio
- Real-time anomaly detection:
  - Whale alerts (trades > $100,000)
  - Volume spikes (quote volume > $1M)
  - Price spikes (> 2% change in 1 minute)

### Storage Architecture
- **PostgreSQL/TimescaleDB**: Primary storage with staging table + MERGE pattern for handling late-arriving data
- **Redis (Cache)**: Sub-millisecond access for real-time data (< 1 hour)

### REST API
- Automatic query routing based on time range
- Multi-timeframe klines (1m, 5m, 15m intervals)
- Rate limiting (100 requests/minute)
- Prometheus metrics endpoint
- OpenAPI documentation

### Monitoring
- Pre-configured Grafana dashboards
- System health monitoring
- Trading analytics visualization
- Real-time market overview

## Storage Architecture

The pipeline implements a **two-tier storage architecture**:

```
Kafka → Spark Streaming ─┬→ PostgreSQL/TimescaleDB (Primary - staging + MERGE)
                         └→ Redis (Cache - Real-time)
```

| Tier | Storage | Purpose | Features | Query Latency |
|------|---------|---------|----------|---------------|
| Primary | PostgreSQL/TimescaleDB | API queries, analytics | SQL, Indexing, UPSERT via staging, hypertables | < 50ms |
| Cache | Redis | Real-time dashboards | In-memory, TTL-based expiry | < 1ms |

### Why This Architecture?

1. **Staging + MERGE Pattern**: Reliable upserts without duplicate key errors
2. **TimescaleDB Hypertables**: Optimized time-series storage with automatic partitioning
3. **Compression**: TimescaleDB native compression for historical data
4. **Continuous Aggregates**: Materialized views for faster queries
5. **Redis Cache**: Sub-millisecond access for real-time dashboards

The `QueryRouter` automatically selects the appropriate storage tier based on the requested time range.

## Project Structure

```
.
├── dags/                          # Airflow DAG definitions
│   ├── binance_connector_dag.py   # WebSocket connector orchestration
│   └── streaming_processing_dag.py # Spark jobs orchestration
├── api/
│   └── app.py                     # FastAPI application
├── ingestion/                     # Data ingestion layer
│   ├── connector.py               # Binance WebSocket → Kafka (producer)
│   └── ticker_consumer.py         # Kafka → Redis (consumer)
├── processing/                    # Stream processing layer
│   ├── trade_aggregation_job.py   # OHLCV aggregation
│   ├── anomaly_detection_job.py   # Alert generation
│   ├── volatility_prediction_job.py # Volatility prediction
│   └── validators/                # Data quality validation
│       ├── aggregation_validator.py   # Aggregation output validation
│       └── anomaly_validator.py       # Anomaly output validation
├── storage/
│   ├── redis.py                   # Redis storage operations
│   ├── postgres.py                # PostgreSQL storage operations
│   └── query_router.py            # Automatic tier selection
├── util/
│   ├── kafka.py                   # Kafka utilities
│   ├── logging.py                 # Structured logging
│   ├── metrics.py                 # Prometheus metrics
│   ├── retry.py                   # Retry with backoff
│   └── shutdown.py                # Graceful shutdown handling
├── docker/                        # Docker configurations
│   ├── airflow/                   # Airflow image
│   ├── api/                       # FastAPI image
│   ├── consumer/                  # Ticker consumer image
│   └── streamlit/                 # Streamlit dashboard image
├── grafana/
│   ├── dashboards/                # Pre-configured dashboards
│   ├── provisioning/              # Auto-provisioning config
│   └── prometheus.yml             # Prometheus configuration
├── streamlit_app/                 # Streamlit dashboard source
├── tests/                         # Test suite├── docker-compose.yml             # Container orchestration
├── Dockerfile                     # Multi-purpose container image
└── requirements.txt               # Python dependencies
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- 8GB+ RAM recommended
- Python 3.11+ (for local development)

### Quick Start

1. Clone the repository:
```bash
git clone https://github.com/lechihoang/Binance-market-streaming-pipeline.git
cd Binance-market-streaming-pipeline
```

2. Copy environment configuration:
```bash
cp .env.example .env
```

3. Start all services:
```bash
docker-compose up -d
```

4. Wait for services to initialize (approximately 2-3 minutes)

5. Access the services:
   - Airflow UI: http://localhost:8080 (admin/admin)
   - Grafana: http://localhost:3000 (admin/admin)
   - API Docs: http://localhost:8000/docs

6. Enable the Airflow DAGs:
   - `binance_connector_dag` - Start data ingestion
   - `streaming_processing_dag` - Start stream processing

### Stopping Services

```bash
docker-compose down
```

To remove all data volumes:
```bash
docker-compose down -v
```

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `binance_connector_dag` | Manual trigger | Runs WebSocket connector for data ingestion |
| `streaming_processing_dag` | Every 5 minutes | Executes Spark streaming jobs |

### 1. Binance Connector DAG

Manages the WebSocket connection to Binance API for real-time data ingestion. Runs continuously to stream trade and ticker data to Kafka topics.

**Tasks:**
- `check_kafka_health`: Verify Kafka broker connectivity
- `run_binance_connector`: Start WebSocket client for trade/ticker streams
- `run_ticker_consumer`: Consume ticker data from Kafka to Redis

![Binance Connector DAG](img/dag1.png)

### 2. Streaming Processing DAG

Orchestrates Spark streaming jobs for data processing. Runs every 5 minutes to aggregate trades and detect anomalies.

**Tasks:**
- `health_checks`: Verify Redis and PostgreSQL connectivity
- `trade_aggregation`: Compute 1-minute OHLCV candles with buy/sell metrics
- `anomaly_detection`: Detect whale trades, price spikes, and volume anomalies
- `cleanup_streaming`: Clean up resources after processing

![Streaming Processing DAG](img/dag2.png)

## API Endpoints

### Market Data
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/market/realtime` | All real-time ticker data |
| `GET /api/v1/market/summary` | Market summary statistics |
| `GET /api/v1/market/ticker-health` | Ticker service health |
| `GET /api/v1/market/top-by-trades` | Top symbols by trade count |
| `GET /api/v1/market/top-by-volume` | Top symbols by volume |

### Analytics
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/analytics/klines/{symbol}` | OHLCV candles (1m, 5m, 15m) |
| `GET /api/v1/analytics/trades-count` | Trade count aggregations |

### Alerts
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/analytics/alerts/price-spikes` | Price spike alerts (>2% change) |
| `GET /api/v1/analytics/alerts/volume-spikes` | Volume spike alerts (>$1M) |

### System
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/system/health` | System health status |
| `GET /metrics` | Prometheus metrics |

## Monitoring and Dashboards

### Pre-configured Dashboards

#### 1. Market Overview
Real-time prices, volumes, and market summary across all trading pairs.

![Market Overview Dashboard](img/dashboard1.png)

#### 2. Symbol Deep Dive
Detailed analysis for individual trading pairs with OHLCV charts and trade metrics.

![Symbol Deep Dive Dashboard](img/dashboard2.png)

#### 3. Trading Analytics
Trade patterns, price spikes, volume spikes, and market anomalies.

![Trading Analytics Dashboard](img/dashboard3.png)

#### 4. System Health
Infrastructure monitoring, service status, and performance metrics.

![System Health Dashboard](img/dashboard4.png)

### Metrics Collected

- Message processing rates and latencies
- Storage tier write success/failure rates
- Kafka consumer lag
- Redis memory usage
- API request rates and response times

## Configuration

Key environment variables (see `.env.example` for full list):

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Redis (Cache)
REDIS_HOST=redis
REDIS_PORT=6379

# PostgreSQL (Primary Storage)
POSTGRES_HOST=postgres-data
POSTGRES_PORT=5432
POSTGRES_USER=crypto
POSTGRES_PASSWORD=crypto
POSTGRES_DB=crypto_data

# Trading Pairs
TICKER_SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT,...
```

## Testing

Run the test suite:

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

Test categories:
- Unit tests for storage operations
- Integration tests for API endpoints
- Property-based tests using Hypothesis
- End-to-end pipeline tests


