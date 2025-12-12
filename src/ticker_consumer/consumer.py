"""
Ticker Consumer Module - Consolidated.

Reads ticker messages from Kafka and writes to Redis with low latency.
Contains all configuration, validation, filtering, and consumer logic.
"""

import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.utils.config import get_env_str, get_env_int, get_env_list, get_env_optional
from src.utils.logging import setup_logging, get_logger
from src.utils.metrics import record_message_processed, PROCESSING_LATENCY
from src.utils.retry import ExponentialBackoff

logger = get_logger(__name__)

# Default symbols
DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
    "LINKUSDT", "LTCUSDT", "UNIUSDT", "ATOMUSDT", "SHIBUSDT",
]

# Required fields for validation
REQUIRED_FIELDS = {'s', 'c', 'p', 'P', 'o', 'h', 'l', 'v', 'q'}
NUMERIC_FIELDS = {'c', 'p', 'P', 'o', 'h', 'l', 'v', 'q'}


@dataclass
class TickerConfig:
    """Ticker consumer configuration."""
    
    # Kafka
    kafka_servers: str = "localhost:9092"
    kafka_topic: str = "raw_tickers"
    consumer_group: str = "ticker-consumer-group"
    
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    ticker_ttl: int = 60
    
    # Symbols
    symbols: List[str] = field(default_factory=lambda: DEFAULT_SYMBOLS.copy())
    
    # Late data
    watermark_delay_ms: int = 10000  # 10 seconds
    max_future_ms: int = 5000  # 5 seconds
    
    @classmethod
    def from_env(cls) -> "TickerConfig":
        """Load config from environment variables."""
        symbols = get_env_list("TICKER_SYMBOLS", DEFAULT_SYMBOLS.copy())
        return cls(
            kafka_servers=get_env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=get_env_str("KAFKA_TICKER_TOPIC", "raw_tickers"),
            consumer_group=get_env_str("TICKER_CONSUMER_GROUP", "ticker-consumer-group"),
            redis_host=get_env_str("REDIS_HOST", "localhost"),
            redis_port=get_env_int("REDIS_PORT", 6379),
            redis_db=get_env_int("REDIS_DB", 0),
            redis_password=get_env_optional("REDIS_PASSWORD"),
            ticker_ttl=get_env_int("TICKER_TTL_SECONDS", 60),
            symbols=[s.upper() for s in symbols],
            watermark_delay_ms=get_env_int("TICKER_WATERMARK_DELAY_MS", 10000),
            max_future_ms=get_env_int("TICKER_MAX_FUTURE_MS", 5000),
        )
    
    def log_config(self) -> None:
        """Log configuration on startup."""
        logger.info(f"Kafka: {self.kafka_servers}, Topic: {self.kafka_topic}")
        logger.info(f"Redis: {self.redis_host}:{self.redis_port}, TTL: {self.ticker_ttl}s")
        logger.info(f"Symbols ({len(self.symbols)}): {', '.join(self.symbols)}")


def validate_ticker(data: Dict[str, Any]) -> bool:
    """Validate ticker message has required fields with valid values."""
    # Check required fields
    for fld in REQUIRED_FIELDS:
        if fld not in data or data[fld] is None:
            return False
    
    # Check numeric fields
    for fld in NUMERIC_FIELDS:
        value = data.get(fld)
        if value is None:
            return False
        if isinstance(value, str):
            try:
                float(value)
            except ValueError:
                return False
    
    return True


class LateDataFilter:
    """Simple late data filter using watermark and per-symbol tracking."""
    
    def __init__(self, watermark_delay_ms: int = 10000, max_future_ms: int = 5000):
        self.watermark_delay_ms = watermark_delay_ms
        self.max_future_ms = max_future_ms
        self._last_times: Dict[str, int] = {}
    
    def should_process(self, symbol: str, event_time: int) -> bool:
        """Check if message should be processed based on event time."""
        current_time = int(time.time() * 1000)
        symbol = symbol.upper()
        
        # Reject future data
        if event_time > current_time + self.max_future_ms:
            return False
        
        # Reject too old data
        if event_time < current_time - self.watermark_delay_ms:
            return False
        
        # Reject stale data (older than last processed)
        last_time = self._last_times.get(symbol, 0)
        if event_time <= last_time:
            return False
        
        self._last_times[symbol] = event_time
        return True



class TickerConsumer:
    """Core ticker consumer service."""
    
    def __init__(self, config: TickerConfig):
        from src.storage import RedisTickerStorage
        
        self.config = config
        self._configured_symbols = set(s.upper() for s in config.symbols)
        self._late_filter = LateDataFilter(
            watermark_delay_ms=config.watermark_delay_ms,
            max_future_ms=config.max_future_ms,
        )
        self._backoff = ExponentialBackoff(initial_delay_ms=1000, max_delay_ms=60000)
        
        self._redis: Optional["RedisTickerStorage"] = None
        self._kafka = None
        self._shutdown = False
        self._validate = validate_ticker
        
        logger.info(f"TickerConsumer initialized with {len(self._configured_symbols)} symbols")
    
    def _init_redis(self) -> None:
        """Initialize Redis connection."""
        from src.storage import RedisTickerStorage
        
        self._redis = RedisTickerStorage(
            host=self.config.redis_host,
            port=self.config.redis_port,
            db=self.config.redis_db,
            ttl_seconds=self.config.ticker_ttl,
        )
        logger.info("Redis connected")
    
    def _init_kafka(self) -> None:
        """Initialize Kafka consumer with retry."""
        from kafka import KafkaConsumer
        
        while not self._shutdown:
            try:
                self._kafka = KafkaConsumer(
                    self.config.kafka_topic,
                    bootstrap_servers=self.config.kafka_servers,
                    group_id=self.config.consumer_group,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                )
                self._backoff.reset()
                logger.info("Kafka connected")
                return
            except Exception as e:
                delay = self._backoff.next_delay_ms() / 1000
                logger.error(f"Kafka connection failed: {e}, retrying in {delay:.1f}s")
                time.sleep(delay)
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """Process a single ticker message."""
        topic = self.config.kafka_topic
        start = time.time()
        
        # Extract data
        data = message.get("original_data", message)
        symbol = (message.get("symbol") or data.get("s", "")).upper()
        
        # Filter by symbol
        if symbol not in self._configured_symbols:
            record_message_processed("ticker_consumer", topic, "skipped_symbol")
            return False
        
        # Validate
        if not self._validate(data):
            record_message_processed("ticker_consumer", topic, "skipped_validation")
            logger.warning(f"Validation failed for {symbol}")
            return False
        
        # Late data filter
        event_time = data.get("E", 0)
        if event_time and not self._late_filter.should_process(symbol, event_time):
            record_message_processed("ticker_consumer", topic, "skipped_late")
            return False
        
        # Write to Redis
        try:
            self._redis.write_ticker(symbol, data)
            record_message_processed("ticker_consumer", topic, "success")
            
            # Record latency
            latency_ms = (time.time() - start) * 1000
            PROCESSING_LATENCY.labels(service="ticker_consumer", operation="process").observe(latency_ms)
            return True
            
        except Exception as e:
            record_message_processed("ticker_consumer", topic, "error")
            logger.error(f"Redis write failed for {symbol}: {e}")
            return False
    
    def start(self) -> None:
        """Start the consumer."""
        logger.info("Starting Ticker Consumer...")
        self.config.log_config()
        self._init_redis()
        self._init_kafka()
        logger.info("Ticker Consumer started")
    
    def stop(self) -> None:
        """Stop the consumer."""
        logger.info("Stopping Ticker Consumer...")
        self._shutdown = True
        if self._kafka:
            self._kafka.close()
        logger.info("Ticker Consumer stopped")
    
    def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        self._shutdown = True
    
    def run(self) -> None:
        """Run the consumer loop."""
        self.start()
        
        try:
            for msg in self._kafka:
                if self._shutdown:
                    break
                self.process_message(msg.value)
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
        finally:
            self.stop()


# Module-level consumer for signal handling
_consumer: Optional[TickerConsumer] = None


def _signal_handler(signum: int, frame) -> None:
    """Handle shutdown signals."""
    if _consumer:
        _consumer.request_shutdown()


def main() -> int:
    """Main entry point for ticker consumer CLI."""
    global _consumer
    
    # Setup logging using utils
    setup_logging(level="INFO")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    
    logger = get_logger(__name__)
    logger.info("Ticker Consumer Service Starting")
    
    try:
        config = TickerConfig.from_env()
        
        # Signal handlers
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        
        _consumer = TickerConsumer(config)
        _consumer.run()
        return 0
        
    except KeyboardInterrupt:
        if _consumer:
            _consumer.request_shutdown()
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


def health_check_main() -> int:
    """Health check entry point for Docker healthcheck.
    
    Returns 0 if Redis is reachable, 1 otherwise.
    """
    try:
        from src.storage import RedisTickerStorage
        
        config = TickerConfig.from_env()
        redis = RedisTickerStorage(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            ttl_seconds=config.ticker_ttl,
        )
        # Simple ping check
        redis._client.ping()
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    sys.exit(main())
