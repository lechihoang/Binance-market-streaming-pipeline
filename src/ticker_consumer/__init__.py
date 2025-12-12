"""
Ticker Consumer Module

Provides real-time ticker data processing from Kafka to Redis.
Bypasses Spark processing for low-latency ticker updates (~100-200ms).

Components:
- TickerConfig: Configuration dataclass
- TickerConsumer: Core consumer service
- validate_ticker: Message validation function
- LateDataFilter: Late data filtering with watermark
- main: CLI entrypoint
"""

from src.ticker_consumer.consumer import (
    TickerConfig,
    TickerConsumer,
    validate_ticker,
    LateDataFilter,
    main,
    health_check_main,
)

# Re-export ExponentialBackoff from utils for backward compatibility
from src.utils.retry import ExponentialBackoff

__all__ = [
    "TickerConfig",
    "TickerConsumer",
    "validate_ticker",
    "LateDataFilter",
    "ExponentialBackoff",
    "main",
    "health_check_main",
]
