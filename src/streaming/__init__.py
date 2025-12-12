"""
PySpark Streaming Processor

Real-time data processing pipeline using Apache Spark Structured Streaming
to process cryptocurrency market data from Kafka.

Structure:
- core.py: Configuration classes and connector utilities
- trade_aggregation_job.py: Aggregates raw trades into OHLCV candles
- technical_indicators_job.py: Technical indicators calculation (SMA, EMA, RSI, MACD, BB, ATR)
- anomaly_detection_job.py: Anomaly detection job (whale alerts, volume/price spikes, etc.)
- Graceful shutdown imported from src.utils.shutdown

Storage Architecture:
- Hot Path: Redis (real-time queries)
- Warm Path: PostgreSQL (90-day analytics)
- Cold Path: MinIO (historical archive)
"""

__version__ = "0.1.0"

# Configuration classes from core.py
from .core import (
    Config,
    KafkaConfig,
    SparkConfig,
    RedisConfig,
    PostgresConfig,
    MinioConfig,
)

# Connectors from core.py
from .core import (
    KafkaConnector,
    RedisConnector,
)

# Job classes - each from its own file
from .trade_aggregation_job import TradeAggregationJob
from .technical_indicators_job import (
    TechnicalIndicatorsJob,
    IndicatorCalculator,
    CandleState,
)
from .anomaly_detection_job import AnomalyDetectionJob

# Graceful shutdown utility (from utils)
from src.utils.shutdown import GracefulShutdown, ShutdownState, ShutdownEvent

__all__ = [
    # Configuration
    "Config",
    "KafkaConfig",
    "SparkConfig",
    "RedisConfig",
    "PostgresConfig",
    "MinioConfig",
    # Connectors
    "KafkaConnector",
    "RedisConnector",
    # Jobs
    "TradeAggregationJob",
    "TechnicalIndicatorsJob",
    "AnomalyDetectionJob",
    # Utilities
    "IndicatorCalculator",
    "CandleState",
    # Graceful shutdown (re-exported from utils)
    "GracefulShutdown",
    "ShutdownState",
    "ShutdownEvent",
]
