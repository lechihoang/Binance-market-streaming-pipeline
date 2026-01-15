# Real-Time Cryptocurrency Data Pipeline

A production-grade streaming data pipeline that ingests, processes, and visualizes real-time cryptocurrency market data from Binance. Built with Apache Kafka, Spark Streaming, PostgreSQL/TimescaleDB, Redis, and FastAPI.

## Overview

This project demonstrates a complete event-driven data pipeline handling **100-1,000+ messages/second** from cryptocurrency exchanges:

- **Ingests** real-time trade and ticker data via Binance WebSocket API
- **Streams** events through Apache Kafka for reliable message delivery
- **Processes** data with Apache Spark for OHLCV aggregation, anomaly detection, and volatility prediction
- **Stores** data in a two-tier architecture (PostgreSQL/TimescaleDB + Redis) for optimal performance
- **Exposes** data through a FastAPI REST API with intelligent query routing
- **Visualizes** market data through an interactive Streamlit dashboard
- **Monitors** system metrics with Prometheus and Grafana
- **Orchestrates** workflows using Apache Airflow

## Architecture

For detailed architecture and design decisions, see [Technical Documentation](docs/TECHNICAL.md).

## Key Features

- **Real-time ingestion** from Binance WebSocket (15+ trading pairs)
- **Stream processing** with Spark: OHLCV aggregation, anomaly detection, volatility prediction
- **Two-tier storage**: PostgreSQL/TimescaleDB (primary) + Redis (cache) for optimal performance
- **REST API** with automatic tier routing based on time range
- **Interactive dashboard** with Streamlit for market data visualization
- **Metrics monitoring** with Prometheus and Grafana for system health
- **Workflow orchestration** with Airflow DAGs

## Project Structure

```
.
├── api/                           # FastAPI REST API
│   ├── app.py                     # Main API application with all endpoints
│   └── schema.py                 # Pydantic models for request/response
│
├── dags/                          # Airflow DAG definitions
│   ├── binance_connector_dag.py   # WebSocket data ingestion orchestration
│   └── streaming_processing_dag.py # Spark streaming jobs orchestration
│
├── ingestion/                     # Real-time data ingestion
│   ├── connector.py               # Binance WebSocket → Kafka producer
│   └── ticker_consumer.py         # Kafka → Redis consumer for ticker data
│
├── processing/                    # Spark streaming jobs
│   ├── trade_aggregation_job.py   # 1-minute OHLCV aggregation
│   ├── anomaly_detection_job.py   # Anomaly detection (spikes, whales)
│   └── volatility_prediction_job.py # ML-based volatility prediction
│
├── storage/                       # Storage layer
│   ├── postgres.py                # PostgreSQL/TimescaleDB operations
│   ├── redis.py                   # Redis cache operations
│   └── query_router.py            # Intelligent tier selection (Redis/Postgres)
│
├── validator/                     # Data quality validation
│   ├── aggregation_validator.py   # Validate aggregation outputs
│   └── anomaly_validator.py       # Validate anomaly detection outputs
│
├── util/                          # Shared utilities
│   ├── constant.py                # **All configuration constants** ⚙️
│   ├── kafka.py                   # Kafka producer/consumer utilities
│   ├── logging.py                 # Structured logging
│   ├── metric.py                 # Prometheus metrics
│   ├── retry.py                   # Retry logic with exponential backoff
│   └── cleanup.py                 # Resource cleanup utilities
│
├── streamlit_app/                 # Interactive dashboard
│   ├── app.py                     # Main Streamlit application
│   ├── components/                # Reusable UI components
│   │   ├── api.py                 # API client
│   │   └── chart.py              # Chart rendering functions
│   └── pages/                     # Multi-page dashboard
│       ├── 1_Market_Overview.py   # Market summary and top movers
│       ├── 2_Symbol_Deep_Dive.py  # Individual symbol analysis
│       └── 3_Prediction.py        # Volatility predictions
│
├── test/                          # Test suite
│   ├── test_api.py                # API endpoint tests
│   ├── test_storage.py            # Storage layer tests
│   ├── test_streaming.py          # Streaming job tests
│   └── test_airflow.py            # Airflow DAG tests
│
├── script/                        # Utility scripts
│   ├── download_binance_data.py   # Download historical data from Binance
│   └── gen.py                     # Data generation utilities
│
├── notebook/                      # Jupyter notebooks
│   └── train_volatility_predictor.ipynb # ML model training
│
├── model/                         # ML model artifacts
│   └── volatility_predictor.json  # Trained volatility prediction model
│
├── docs/                          # Documentation
│   ├── SETUP.md                   # Setup and configuration guide
│   ├── API.md                     # Complete API reference
│   └── TECHNICAL.md               # Architecture and technical details
│
├── grafana/                       # Monitoring configuration
│   ├── dashboards/                # Pre-configured Grafana dashboards
│   └── provisioning/              # Grafana auto-provisioning config
│
├── docker/                        # Docker configurations
│   ├── airflow/                   # Airflow Dockerfile
│   ├── api/                       # FastAPI Dockerfile
│   ├── consumer/                  # Consumer Dockerfile
│   └── streamlit/                 # Streamlit Dockerfile
│
├── data/                          # Data storage (gitignored)
│   ├── historical/                # Historical kline data
│   ├── parquet/                   # Parquet files
│   └── spark-checkpoints/         # Spark streaming checkpoints
│
├── log/                           # Airflow logs (gitignored)
├── docker-compose.yml             # Container orchestration
├── Dockerfile                     # Multi-purpose base image
└── requirements.txt               # Python dependencies
```

**Key Configuration:** All project settings can be modified in [util/constant.py](util/constant.py) including:
- Trading symbols, Redis/Kafka/PostgreSQL configs
- Anomaly detection thresholds, ML model features
- API rate limits, data retention policies

## Quick Start

**Prerequisites:** Docker, Docker Compose, 8GB+ RAM

```bash
# 1. Clone the repository
git clone https://github.com/lechihoang/Binance-market-streaming-pipeline.git
cd Binance-market-streaming-pipeline

# 2. Start all services
docker-compose up -d

# 3. Wait ~2-3 minutes for initialization

# 4. Access services
# - Streamlit Dashboard: http://localhost:8501 (Main visualization)
# - Airflow: http://localhost:8080 (admin/admin)
# - API Docs: http://localhost:8000/docs
# - Grafana: http://localhost:3000 (admin/admin - Metrics only)

# 5. Enable DAGs in Airflow UI
# - binance_connector_dag
# - streaming_processing_dag
```

For detailed setup instructions and troubleshooting, see [Setup Guide](docs/SETUP.md).

## Documentation

- [Setup Guide](docs/SETUP.md) - Installation, configuration, environment variables, testing
- [API Reference](docs/API.md) - Complete REST API documentation with examples
- [Technical Documentation](docs/TECHNICAL.md) - Architecture, design decisions, monitoring, glossary

## Tech Stack

| Component | Technology |
|-----------|------------|
| Message Broker | Apache Kafka |
| Stream Processing | Apache Spark (PySpark) |
| Cache | Redis |
| Database | PostgreSQL/TimescaleDB |
| API | FastAPI |
| Visualization | Streamlit |
| Orchestration | Apache Airflow |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker Compose |

## Visualization & Monitoring

### Streamlit Dashboard (Main Visualization)
Access the interactive dashboard at http://localhost:8501 for:
- Real-time market data visualization
- OHLCV charts and trading analytics
- Market overview and symbol analysis
- Anomaly alerts and trading patterns

### Grafana + Prometheus (System Metrics)
Access metrics monitoring at http://localhost:3000:
- API performance metrics
- System health monitoring
- Infrastructure resource usage
- Kafka, Redis, PostgreSQL metrics

![Dashboard Preview](img/dashboard1.png)

For detailed documentation, see [Technical Documentation](docs/TECHNICAL.md#monitoring-and-dashboards).


