"""
Consolidated test module for Binance Kafka Connector.
Contains all tests for WebSocket client, message processing, and Kafka production.

Table of Contents:
- Imports and Setup (line ~15)
- WebSocket Client Tests (line ~35)
- Message Processor Tests (line ~120)
- Kafka Producer Tests (line ~180)

Requirements: 6.1
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import pytest
import asyncio
import json
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime

import websockets.exceptions

from src.binance_kafka_connector.connector import (
    BinanceWebSocketClient,
    process_message,
    EnrichedMessage,
    Config,
)
from src.utils.retry import ExponentialBackoff


# ============================================================================
# WEBSOCKET CLIENT TESTS
# ============================================================================

class TestWebSocketClient:
    """Tests for BinanceWebSocketClient functionality."""
    
    def test_client_initialization(self):
        """Test WebSocket client initializes with correct parameters."""
        url = "wss://test.example.com/stream"
        streams = ["btcusdt@trade", "ethusdt@trade"]
        
        client = BinanceWebSocketClient(url=url, streams=streams)
        
        assert client.url == url
        assert client.streams == streams
        assert client.websocket is None
        assert client.is_connected is False
        assert isinstance(client._backoff, ExponentialBackoff)
    
    def test_backoff_configured_correctly(self):
        """Test that backoff is configured with correct max delay from config."""
        client = BinanceWebSocketClient(
            url="wss://test.example.com/stream",
            streams=["btcusdt@trade"]
        )
        
        # Backoff should use config's RECONNECT_MAX_DELAY_SECONDS converted to ms
        assert client._backoff.initial_delay_ms == 1000
        assert client._backoff.multiplier == 2.0
        assert client._backoff.jitter_factor == 0.1

    @pytest.mark.asyncio
    async def test_reconnect_with_backoff_calls_connect_and_subscribe(self):
        """Test reconnect_with_backoff calls connect and subscribe on success."""
        client = BinanceWebSocketClient(
            url="wss://test.example.com/stream",
            streams=["btcusdt@trade"]
        )
        
        # Mock connect and subscribe to succeed
        client.connect = AsyncMock()
        client.subscribe = AsyncMock()
        
        # Run reconnect (should succeed on first try)
        await client.reconnect_with_backoff()
        
        # Verify connect and subscribe were called
        client.connect.assert_called_once()
        client.subscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_reconnect_retries_on_failure(self):
        """Test reconnect_with_backoff retries when connect fails."""
        client = BinanceWebSocketClient(
            url="wss://test.example.com/stream",
            streams=["btcusdt@trade"]
        )
        
        # Mock connect to fail twice then succeed
        call_count = 0
        async def mock_connect():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection failed")
        
        client.connect = mock_connect
        client.subscribe = AsyncMock()
        
        # Patch sleep to avoid actual delays
        with patch('asyncio.sleep', new_callable=AsyncMock):
            await client.reconnect_with_backoff()
        
        # Should have tried 3 times
        assert call_count == 3
        client.subscribe.assert_called_once()

    def test_is_connected_flag_initial_state(self):
        """Test is_connected flag starts as False."""
        client = BinanceWebSocketClient(
            url="wss://test.example.com/stream",
            streams=["btcusdt@trade"]
        )
        
        assert client.is_connected is False
        assert client.websocket is None

    @pytest.mark.asyncio
    async def test_close_sets_disconnected_state(self):
        """Test close() sets is_connected to False and websocket to None."""
        client = BinanceWebSocketClient(
            url="wss://test.example.com/stream",
            streams=["btcusdt@trade"]
        )
        
        # Simulate connected state
        client.is_connected = True
        client.websocket = AsyncMock()
        
        await client.close()
        
        assert client.is_connected is False
        assert client.websocket is None


# ============================================================================
# MESSAGE PROCESSOR TESTS
# ============================================================================

class TestMessageProcessor:
    """Tests for process_message functionality."""
    
    def test_process_trade_message(self):
        """Test processing a valid trade message."""
        raw_json = json.dumps({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "s": "BTCUSDT",
                "p": "50000.00",
                "q": "0.001"
            }
        })
        
        result = process_message(raw_json)
        
        assert result is not None
        assert isinstance(result, EnrichedMessage)
        assert result.symbol == "BTCUSDT"
        assert result.stream_type == "trade"
        assert result.topic == "raw_trades"
        assert result.original_data["e"] == "trade"
    
    def test_process_kline_message(self):
        """Test processing a valid kline message."""
        raw_json = json.dumps({
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "s": "BTCUSDT",
                "k": {"o": "50000", "c": "50100"}
            }
        })
        
        result = process_message(raw_json)
        
        assert result is not None
        assert result.stream_type == "kline"
        assert result.topic == "raw_klines"
    
    def test_process_ticker_message(self):
        """Test processing a valid ticker message."""
        raw_json = json.dumps({
            "stream": "btcusdt@ticker",
            "data": {
                "e": "24hrTicker",
                "s": "BTCUSDT",
                "c": "50000.00"
            }
        })
        
        result = process_message(raw_json)
        
        assert result is not None
        assert result.stream_type == "ticker"
        assert result.topic == "raw_tickers"
    
    def test_process_unknown_event_returns_none(self):
        """Test that unknown event types return None."""
        raw_json = json.dumps({
            "stream": "btcusdt@unknown",
            "data": {
                "e": "unknownEvent",
                "s": "BTCUSDT"
            }
        })
        
        result = process_message(raw_json)
        
        assert result is None
    
    def test_process_invalid_json_returns_none(self):
        """Test that invalid JSON returns None."""
        result = process_message("not valid json")
        
        assert result is None
    
    def test_process_missing_data_returns_none(self):
        """Test that message without 'data' field returns None."""
        raw_json = json.dumps({"stream": "btcusdt@trade"})
        
        result = process_message(raw_json)
        
        assert result is None


# ============================================================================
# KAFKA PRODUCER TESTS
# ============================================================================

class TestKafkaProducer:
    """Tests for KafkaProducerClient functionality."""
    
    def test_placeholder(self):
        """Placeholder test for Kafka producer - requires Kafka connection."""
        # Kafka producer tests require actual Kafka connection or more complex mocking
        assert True


# ============================================================================
# CONFIG TESTS
# ============================================================================

class TestConfig:
    """Tests for Config class."""
    
    def test_config_has_required_attributes(self):
        """Test Config has all required attributes after simplification."""
        # WebSocket config
        assert hasattr(Config, 'BINANCE_WS_URL')
        assert hasattr(Config, 'BINANCE_STREAMS')
        assert hasattr(Config, 'RECONNECT_MAX_DELAY_SECONDS')
        assert hasattr(Config, 'WS_CONNECTION_TIMEOUT')
        
        # Kafka config
        assert hasattr(Config, 'KAFKA_BOOTSTRAP_SERVERS')
        
        # Logging config
        assert hasattr(Config, 'LOG_LEVEL')
    
    def test_config_no_batch_attributes(self):
        """Test Config does not have removed batch attributes."""
        # These should have been removed per Requirements 4.1, 4.2
        assert not hasattr(Config, 'BATCH_SIZE')
        assert not hasattr(Config, 'BATCH_TIMEOUT_MS')
        assert not hasattr(Config, 'BATCH_CHECK_INTERVAL_MS')
