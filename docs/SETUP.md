# Setup and Configuration Guide

This guide covers everything you need to deploy and configure the Real-Time Cryptocurrency Data Pipeline.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Starting the Pipeline](#starting-the-pipeline)
- [Enabling Airflow DAGs](#enabling-airflow-dags)
- [Testing](#testing)
- [Stopping Services](#stopping-services)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements
- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+
- **RAM**: 8GB+ recommended
- **Disk Space**: 10GB+ for containers and data
- **Python**: 3.11+ (for local development only)

### Port Requirements
Ensure the following ports are available:
- `8501`: Streamlit Dashboard
- `8080`: Airflow Web UI
- `8000`: FastAPI
- `3000`: Grafana
- `9090`: Prometheus
- `5432`: PostgreSQL
- `6379`: Redis
- `9092`: Kafka (external)
- `29092`: Kafka (internal)
- `2181`: Zookeeper

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/lechihoang/Binance-market-streaming-pipeline.git
cd Binance-market-streaming-pipeline
```

### 2. Start All Services

```bash
docker-compose up -d
```

This will start:
- Kafka & Zookeeper
- Redis (cache layer)
- PostgreSQL/TimescaleDB (primary storage)
- Airflow (scheduler, webserver, worker)
- FastAPI (REST API)
- Streamlit (interactive dashboard)
- Grafana (metrics monitoring)
- Prometheus (metrics collection)

### 3. Wait for Initialization

Services take approximately 2-3 minutes to fully initialize. You can monitor the logs:

```bash
docker-compose logs -f
```

### 4. Access the Services

Once all services are running:

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| Streamlit Dashboard | http://localhost:8501 | - | Main market data visualization |
| Airflow Web UI | http://localhost:8080 | admin / admin | Workflow orchestration |
| API Documentation | http://localhost:8000/docs | - | REST API |
| Grafana | http://localhost:3000 | admin / admin | System metrics only |
| Prometheus | http://localhost:9090 | - | Metrics collection |

### 5. Enable the Pipeline

Navigate to Airflow UI (http://localhost:8080) and enable these DAGs:

1. **`binance_connector_dag`** - Starts data ingestion from Binance WebSocket
2. **`streaming_processing_dag`** - Starts Spark streaming jobs for processing

The data should start flowing within a few minutes. Access the Streamlit dashboard at http://localhost:8501 to view real-time market data.

## Configuration

### Central Configuration File

**All project configurations are centralized in [util/constant.py](../util/constant.py).**

This file contains:
- **Service connections**: Redis, Kafka, PostgreSQL, Binance API
- **Trading symbols**: List of tracked cryptocurrency pairs
- **Anomaly detection thresholds**: Volume, price change, trade count
- **ML model features**: Features for volatility prediction
- **API settings**: Rate limits, cache duration
- **Data retention**: TTLs for Redis keys

To modify any configuration, edit `util/constant.py` and restart affected services.

### Common Configuration Changes

#### 1. Trading Pairs

Edit the `DEFAULT_SYMBOL` list in [util/constant.py](../util/constant.py):

```python
DEFAULT_SYMBOL = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    # Add or remove symbols as needed
]
```

Then restart the services:
```bash
docker-compose restart
```

### Spark Configuration

Adjust Spark resources in `docker-compose.yml`:

```yaml
environment:
  SPARK_DRIVER_MEMORY: 2g
  SPARK_EXECUTOR_MEMORY: 2g
  SPARK_EXECUTOR_CORES: 2
```

#### 2. Anomaly Detection Thresholds

Edit threshold values in [util/constant.py](../util/constant.py):

```python
# ANOMALY DETECTION
VOLUME_THRESHOLD = 1_000_000.0        # Quote volume > $1M
PRICE_CHANGE_THRESHOLD = 2.0          # Price change > 2%
TRADE_COUNT_MULTIPLIER = 3.0          # Trade count spike multiplier
BUY_RATIO_LOW = 0.3                   # Buy ratio imbalance lower bound
BUY_RATIO_HIGH = 0.7                  # Buy ratio imbalance upper bound
```

#### 3. Redis Cache TTL

Adjust cache duration in [util/constant.py](../util/constant.py):

```python
class RedisTTL:
    AGG = 3600      # Aggregated data: 1 hour
    TICKER = 60     # Ticker data: 1 minute
    TRADE = 300     # Trade data: 5 minutes
    ALERT = 86400   # Alert data: 24 hours
```

#### 4. API Rate Limiting

Configure API limits in [util/constant.py](../util/constant.py):

```python
# API
RATE_LIMIT = 100              # Requests per window
RATE_LIMIT_WINDOW = 60        # Window in seconds
```

### Airflow Configuration

Modify DAG schedules directly in:
- [dags/binance_connector_dag.py](../dags/binance_connector_dag.py)
- [dags/streaming_processing_dag.py](../dags/streaming_processing_dag.py)

## Key Configuration Values

Default configurations are defined in [util/constant.py](../util/constant.py):

### Service Connections

**Kafka:**
- Bootstrap servers: `kafka:29092` (defined in `KAFKA_SERVER`)
- Topics: `raw_trades`, `raw_tickers` (defined in `TOPIC_TRADE`, `TOPIC_TICKER`)

**Redis (Cache Layer):**
- Host: `redis` (defined in `REDIS_HOST`)
- Port: `6379` (defined in `REDIS_PORT`)
- TTLs: 1h for aggregations, 1min for tickers, 5min for trades

**PostgreSQL (Primary Storage):**
- Host: `postgres-data` (defined in `POSTGRES_HOST`)
- Port: `5432` (defined in `POSTGRES_PORT`)
- Database: `crypto_data` (defined in `POSTGRES_DB`)
- Connection pool: 1-10 connections

**Binance:**
- WebSocket URL: `wss://stream.binance.com:9443/stream`
- Historical data URL: `https://data.binance.vision/data/spot/monthly/klines`

### Default Trading Pairs

40 symbols tracked by default (see `DEFAULT_SYMBOL` in constant.py):
- Major: BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT
- DeFi: UNIUSDT, AAVEUSDT, LINKUSDT
- Trending: TRUMPUSDT, PEPEUSDT, WLDUSDT, BONKUSDT
- And 28+ more pairs

## Starting the Pipeline

### Start All Services
```bash
docker-compose up -d
```

### Start Specific Services
```bash
# Start only storage layer
docker-compose up -d postgres redis

# Start only messaging layer
docker-compose up -d kafka zookeeper

# Start only orchestration
docker-compose up -d airflow-webserver airflow-scheduler
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f api
```

## Enabling Airflow DAGs

1. Access Airflow UI at http://localhost:8080
2. Login with `admin` / `admin`
3. Toggle the DAGs to ON:
   - `binance_connector_dag` - Click the toggle switch to enable
   - `streaming_processing_dag` - Click the toggle switch to enable

The connector DAG should be triggered manually for the first time. The streaming processing DAG will run automatically every 5 minutes.

## Testing

### Run the Full Test Suite

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Test Categories

- **Unit tests**: Storage operations, utilities
- **Integration tests**: API endpoints, database operations
- **Property-based tests**: Using Hypothesis for data validation
- **End-to-end tests**: Full pipeline testing

### Run Specific Tests

```bash
# Test storage layer
pytest tests/test_storage.py

# Test API endpoints
pytest tests/test_api.py

# Test streaming jobs
pytest tests/test_streaming.py
```

## Stopping Services

### Stop All Services
```bash
docker-compose down
```

### Stop and Remove Volumes (Clean Reset)
```bash
docker-compose down -v
```

This will delete all data including:
- Kafka topics
- PostgreSQL databases
- Redis cache
- Airflow metadata

### Stop Specific Services
```bash
docker-compose stop airflow-scheduler
docker-compose stop api
```

## Troubleshooting

### Services Won't Start

**Check port conflicts:**
```bash
lsof -i :8080  # Airflow
lsof -i :5432  # PostgreSQL
lsof -i :6379  # Redis
```

**Check Docker resources:**
```bash
docker system df
docker system prune  # Clean up unused resources
```

### Kafka Connection Issues

**Check Kafka broker health:**
```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Recreate Kafka topics:**
```bash
docker-compose exec kafka kafka-topics --delete --topic binance.trades --bootstrap-server localhost:9092
docker-compose exec kafka kafka-topics --delete --topic binance.tickers --bootstrap-server localhost:9092
```

### Airflow DAGs Not Showing

**Check DAG folder permissions:**
```bash
docker-compose exec airflow-scheduler ls -la /opt/airflow/dags
```

**Force DAG refresh:**
- Go to Airflow UI → Admin → Connections
- Or restart the scheduler: `docker-compose restart airflow-scheduler`

### Database Connection Issues

**Check PostgreSQL connectivity:**
```bash
docker-compose exec postgres-data psql -U crypto -d crypto_data -c "SELECT 1;"
```

**Check Redis connectivity:**
```bash
docker-compose exec redis redis-cli ping
```

### Low Performance

**Increase Docker resources:**
- Docker Desktop → Settings → Resources
- Increase Memory to 8GB+
- Increase CPUs to 4+

**Reduce symbol count:**
Edit `.env` and reduce the number of symbols in `TICKER_SYMBOLS`.

### View Container Resources

```bash
docker stats
```

### Reset Everything

If things are completely broken:

```bash
# Stop and remove everything
docker-compose down -v

# Remove all images (optional)
docker-compose down --rmi all

# Start fresh
docker-compose up -d
```

## Next Steps

- [API Documentation](API.md) - Learn about available API endpoints
- [Technical Documentation](TECHNICAL.md) - Deep dive into architecture and design
- [Main README](../README.md) - Back to overview
