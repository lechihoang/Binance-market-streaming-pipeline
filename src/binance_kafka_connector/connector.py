"""
Binance-Kafka Connector - Simplified Module

Real-time cryptocurrency data ingestion pipeline that connects to Binance WebSocket API
and produces messages to Kafka topics.

Simplified architecture (~250 lines):
- Single async loop (no dual-task architecture)
- Uses Kafka's built-in batching (no custom MessageBatcher)
- Simple shutdown via flag + flush + close

Table of Contents:
==================
1. IMPORTS (line ~25)
2. CONFIGURATION (line ~50)
3. DATA MODELS (line ~105)
   - EnrichedMessage
4. MESSAGE PROCESSOR (line ~120)
   - process_message() function
5. WEBSOCKET CLIENT (line ~165)
6. KAFKA PRODUCER (line ~335)
7. MAIN CONNECTOR (line ~390)
"""

# ============================================================================
# 1. IMPORTS
# ============================================================================

import asyncio
import json
import logging
import signal
import time
from typing import Any, AsyncIterator, Dict, List, Optional

import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel
from websockets.asyncio.client import ClientConnection

# Import utilities from utils module
from src.utils.config import get_env_str, get_env_int, get_env_list
from src.utils.logging import setup_logging, get_logger
from src.utils.retry import ExponentialBackoff
from src.utils.metrics import record_error, record_message_processed


# ============================================================================
# 2. CONFIGURATION
# ============================================================================

class Config:
    """Configuration class that loads settings from environment variables.
    
    Uses utils config helpers for type-safe environment variable loading.
    """

    # Default streams for Binance WebSocket
    _DEFAULT_STREAMS = [
        "btcusdt@trade", "ethusdt@trade", "bnbusdt@trade", "solusdt@trade", "xrpusdt@trade",
        "btcusdt@kline_1m", "ethusdt@kline_1m", "bnbusdt@kline_1m", "solusdt@kline_1m", "xrpusdt@kline_1m",
        "btcusdt@ticker", "ethusdt@ticker", "bnbusdt@ticker", "solusdt@ticker", "xrpusdt@ticker",
        "adausdt@ticker", "dogeusdt@ticker", "avaxusdt@ticker", "dotusdt@ticker", "maticusdt@ticker",
        "linkusdt@ticker", "ltcusdt@ticker", "uniusdt@ticker", "atomusdt@ticker", "shibusdt@ticker",
    ]

    # WebSocket Configuration
    BINANCE_WS_URL: str = get_env_str("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
    BINANCE_STREAMS: List[str] = get_env_list("BINANCE_STREAMS", _DEFAULT_STREAMS)
    RECONNECT_MAX_DELAY_SECONDS: int = get_env_int("RECONNECT_MAX_DELAY_SECONDS", 60)
    WS_CONNECTION_TIMEOUT: int = get_env_int("WS_CONNECTION_TIMEOUT", 10)

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = get_env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Logging Configuration
    LOG_LEVEL: str = get_env_str("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls) -> None:
        """Validate required configuration settings.

        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        if not cls.BINANCE_WS_URL:
            raise ValueError("BINANCE_WS_URL is required")

        if not cls.BINANCE_STREAMS:
            raise ValueError("BINANCE_STREAMS is required")

        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")

        if cls.RECONNECT_MAX_DELAY_SECONDS <= 0:
            raise ValueError("RECONNECT_MAX_DELAY_SECONDS must be positive")


# Create a singleton instance
config = Config()



# ============================================================================
# 3. DATA MODELS
# ============================================================================

class EnrichedMessage(BaseModel):
    """Model for enriched messages with metadata."""
    original_data: Dict[str, Any]
    ingestion_timestamp: int
    symbol: str
    stream_type: str
    topic: str



# ============================================================================
# 4. MESSAGE PROCESSOR
# ============================================================================

logger = get_logger(__name__)

# Event type to (stream type, topic) mapping
EVENT_MAPPING = {
    'trade': ('trade', 'raw_trades'),
    'kline': ('kline', 'raw_klines'),
    '24hrTicker': ('ticker', 'raw_tickers'),
}


def process_message(raw_json: str) -> Optional[EnrichedMessage]:
    """Parse and enrich a raw WebSocket message.
    
    Args:
        raw_json: Raw JSON string from WebSocket
        
    Returns:
        EnrichedMessage if successful, None if any step fails
    """
    try:
        data = json.loads(raw_json)
        actual_data = data['data']
        event_type = actual_data.get('e')
        
        if event_type not in EVENT_MAPPING:
            return None
        
        stream_type, topic = EVENT_MAPPING[event_type]
        
        return EnrichedMessage(
            original_data=actual_data,
            ingestion_timestamp=time.time_ns() // 1_000_000,
            symbol=actual_data.get('s', '').upper(),
            stream_type=stream_type,
            topic=topic
        )
    except Exception:
        return None



# ============================================================================
# 5. WEBSOCKET CLIENT
# ============================================================================

class BinanceWebSocketClient:
    """Async WebSocket client for Binance streaming API.
    
    Features:
    - Automatic reconnection with exponential backoff
    - Multi-stream subscription support
    
    Note: Ping/pong is handled automatically by the websockets library.
    Binance server sends ping every 3 minutes, client responds automatically.
    """

    def __init__(self, url: str, streams: List[str]):
        """Initialize the WebSocket client.
        
        Args:
            url: WebSocket URL (e.g., wss://stream.binance.com:9443/stream)
            streams: List of stream names to subscribe to
        """
        self.url = url
        self.streams = streams
        
        # Connection state
        self.websocket: Optional[ClientConnection] = None
        self.is_connected = False
        
        # Reconnection backoff using utils ExponentialBackoff
        self._backoff = ExponentialBackoff(
            initial_delay_ms=1000,  # 1 second initial delay
            max_delay_ms=config.RECONNECT_MAX_DELAY_SECONDS * 1000,  # Convert to ms
            multiplier=2.0,
            jitter_factor=0.1,
        )

    async def connect(self) -> None:
        """Connect to the Binance WebSocket API."""
        try:
            logger.info(f"Connecting to WebSocket: {self.url}")
            self.websocket = await asyncio.wait_for(
                websockets.connect(self.url),
                timeout=config.WS_CONNECTION_TIMEOUT
            )
            self.is_connected = True
            logger.info("WebSocket connection established successfully")
            # Reset backoff on successful connection
            self._backoff.reset()
            
        except asyncio.TimeoutError:
            logger.error(
                f"Connection timeout after {config.WS_CONNECTION_TIMEOUT} seconds",
                extra={
                    "url": self.url,
                    "error_type": "TimeoutError",
                    "error_details": f"Connection timeout after {config.WS_CONNECTION_TIMEOUT} seconds"
                }
            )
            self.is_connected = False
            # Record connection timeout error
            record_error("binance_connector", "connection_timeout", "error")
            raise
        except Exception as e:
            logger.error(
                f"Connection error to WebSocket",
                extra={
                    "url": self.url,
                    "error_type": type(e).__name__,
                    "error_details": str(e)
                },
                exc_info=True
            )
            self.is_connected = False
            # Record connection error
            record_error("binance_connector", "connection_error", "error")
            raise

    async def subscribe(self) -> None:
        """Subscribe to the configured streams."""
        if not self.websocket or not self.is_connected:
            raise RuntimeError("WebSocket is not connected. Call connect() first.")
        
        subscription_message = {
            "method": "SUBSCRIBE",
            "params": self.streams,
            "id": 1
        }
        
        try:
            await self.websocket.send(json.dumps(subscription_message))
        except Exception as e:
            logger.error(f"Failed to send subscription message: {type(e).__name__}: {e}")
            raise

    async def reconnect_with_backoff(self) -> None:
        """Reconnect to WebSocket with exponential backoff strategy."""
        while True:
            try:
                delay_ms = self._backoff.next_delay_ms()
                delay_seconds = delay_ms / 1000.0
                
                logger.info(f"Reconnecting in {delay_seconds:.1f}s (attempt {self._backoff.attempt_count})")
                await asyncio.sleep(delay_seconds)
                await self.connect()
                await self.subscribe()
                
                logger.info("Reconnection successful")
                break
                
            except Exception as e:
                logger.error(f"Reconnection failed: {type(e).__name__}: {e}")
                record_error("binance_connector", "reconnection_failed", "warning")

    async def receive_messages(self) -> AsyncIterator[str]:
        """Async generator that yields raw JSON strings from WebSocket."""
        while True:
            try:
                if not self.websocket or not self.is_connected:
                    logger.warning("WebSocket not connected, attempting reconnection...")
                    await self.reconnect_with_backoff()
                
                message = await self.websocket.recv()
                
                if isinstance(message, str):
                    yield message
                else:
                    logger.warning(f"Received non-string message: {type(message)}")
                    
            except websockets.exceptions.ConnectionClosed as e:
                logger.error(
                    "WebSocket connection closed",
                    extra={
                        "url": self.url,
                        "error_type": "ConnectionClosed",
                        "error_details": str(e)
                    }
                )
                self.is_connected = False
                # Record connection closed error
                record_error("binance_connector", "connection_closed", "warning")
                
            except Exception as e:
                logger.error(
                    "Error receiving message from WebSocket",
                    extra={
                        "url": self.url,
                        "error_type": type(e).__name__,
                        "error_details": str(e)
                    },
                    exc_info=True
                )
                self.is_connected = False
                # Record receive error
                record_error("binance_connector", "receive_error", "error")

    async def close(self) -> None:
        """Close the WebSocket connection gracefully."""
        logger.info("Closing WebSocket connection...")
        
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info("WebSocket connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebSocket: {type(e).__name__}: {e}")
        
        self.is_connected = False
        self.websocket = None



# ============================================================================
# 6. KAFKA PRODUCER
# ============================================================================

class KafkaProducerClient:
    """Kafka producer client using built-in batching.
    
    Uses Kafka's native batching via linger_ms and batch_size parameters
    instead of custom MessageBatcher for efficient message delivery.
    """
    
    def __init__(self, bootstrap_servers: str = None):
        """Initialize Kafka producer with built-in batching configuration."""
        self.bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            compression_type='snappy',
            acks=1,
            linger_ms=100,        # Wait up to 100ms to batch messages
            batch_size=16384,     # 16KB batch size
            value_serializer=lambda m: json.dumps(m.model_dump()).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )

    def send(self, message: EnrichedMessage) -> None:
        """Send single message to Kafka (Kafka handles batching internally).
        
        Args:
            message: EnrichedMessage to send
        """
        try:
            self.producer.send(topic=message.topic, value=message, key=message.symbol)
            record_message_processed("binance_connector", message.topic, "success")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            record_message_processed("binance_connector", message.topic, "error")
            record_error("binance_connector", "kafka_send_error", "error")

    def close(self) -> None:
        """Flush pending messages and close the Kafka producer."""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
            raise



# ============================================================================
# 7. MAIN CONNECTOR
# ============================================================================

class BinanceKafkaConnector:
    """Main connector with single run loop.
    
    Simplified architecture:
    - Single async loop that receives messages and sends to Kafka
    - Uses Kafka's built-in batching (no custom MessageBatcher)
    - Simple shutdown via flag + flush + close
    """
    
    def __init__(self):
        """Initialize the connector with WebSocket client and Kafka producer."""
        config.validate()
        
        self.ws_client = BinanceWebSocketClient(
            url=config.BINANCE_WS_URL,
            streams=config.BINANCE_STREAMS
        )
        self.kafka_producer = KafkaProducerClient(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
        )
        self.shutdown_flag = False

    def _handle_shutdown(self) -> None:
        """Signal handler - just sets shutdown flag."""
        logger.info("Shutdown signal received")
        self.shutdown_flag = True

    async def run(self) -> None:
        """Single main loop - receive, process, send.
        
        This is the simplified architecture:
        1. Setup signal handlers
        2. Connect and subscribe to WebSocket
        3. Main loop: receive → process → send to Kafka
        4. On shutdown: flush Kafka and close connections
        """
        # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_shutdown)
        
        logger.info("Starting BinanceKafkaConnector...")
        
        try:
            # Connect and subscribe
            await self.ws_client.connect()
            await self.ws_client.subscribe()
            
            logger.info(f"BinanceKafkaConnector started - WS: {config.BINANCE_WS_URL}, "
                       f"Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}, "
                       f"Streams: {len(config.BINANCE_STREAMS)}")
            
            # Main loop - receive, process, send
            async for raw_message in self.ws_client.receive_messages():
                if self.shutdown_flag:
                    break
                
                enriched = process_message(raw_message)
                if enriched:
                    self.kafka_producer.send(enriched)
                    
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            raise
        finally:
            # Cleanup: flush and close
            logger.info("Shutting down BinanceKafkaConnector...")
            try:
                self.kafka_producer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
            
            try:
                await self.ws_client.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
            
            logger.info("BinanceKafkaConnector stopped")





# ============================================================================
# 8. CLI ENTRYPOINT
# ============================================================================

def main():
    """Main entry point for the application."""
    import sys
    
    # Set up logging
    setup_logging(level=config.LOG_LEVEL, json_output=True)
    
    _logger = get_logger(__name__)
    _logger.info("Starting Binance-Kafka Connector...")
    
    try:
        connector = BinanceKafkaConnector()
        asyncio.run(connector.run())
        
    except KeyboardInterrupt:
        _logger.info("Received keyboard interrupt")
    except Exception as e:
        _logger.error(f"Fatal error: {type(e).__name__}: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
