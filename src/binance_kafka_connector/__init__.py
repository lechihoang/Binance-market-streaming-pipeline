"""Binance-Kafka Connector: Real-time cryptocurrency data ingestion pipeline."""

__version__ = "0.1.0"

from .connector import (
    BinanceKafkaConnector,
    Config,
    config,
    EnrichedMessage,
    process_message,
    BinanceWebSocketClient,
    KafkaProducerClient,
)
from src.utils.logging import setup_logging, get_logger

__all__ = [
    "BinanceKafkaConnector",
    "Config",
    "config",
    "EnrichedMessage",
    "process_message",
    "BinanceWebSocketClient",
    "KafkaProducerClient",
    "setup_logging",
    "get_logger",
]
