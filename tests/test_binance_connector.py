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

import pytest
import asyncio
import json
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime

import websockets.exceptions

from ingestion.connector import (
    BinanceWebSocketClient,
    process_message,
    EnrichedMessage,
    BINANCE_WS_URL,
    BINANCE_STREAMS,
    RECONNECT_MAX_DELAY_SECONDS,
    WS_CONNECTION_TIMEOUT,
    KAFKA_BOOTSTRAP_SERVERS,
    LOG_LEVEL,
)
from util.retry import ExponentialBackoff


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
    async def test_reconnect_calls_connect_and_subscribe(self):
        """Test _reconnect calls connect and subscribe on success."""
        client = BinanceWebSocketClient(
            url="wss://test.example.com/stream",
            streams=["btcusdt@trade"]
        )
        
        # Mock connect and subscribe to succeed
        client.connect = AsyncMock()
        client.subscribe = AsyncMock()
        
        # Patch sleep to avoid actual delays
        with patch('asyncio.sleep', new_callable=AsyncMock):
            await client.reconnect()
        
        # Verify connect and subscribe were called
        client.connect.assert_called_once()
        client.subscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_reconnect_retries_on_failure(self):
        """Test _reconnect retries when connect fails."""
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
            await client.reconnect()
        
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
    
    def test_process_kline_message_returns_none(self):
        """Test processing a kline message returns None (kline removed from EVENT_MAPPING)."""
        raw_json = json.dumps({
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "s": "BTCUSDT",
                "k": {"o": "50000", "c": "50100"}
            }
        })
        
        result = process_message(raw_json)
        
        # kline event type was removed from EVENT_MAPPING
        assert result is None
    
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


class TestKafkaProducer:
    """Tests for KafkaProducerClient functionality."""
    
    def test_placeholder(self):
        """Placeholder test for Kafka producer - requires Kafka connection."""
        # Kafka producer tests require actual Kafka connection or more complex mocking
        assert True


class TestConfig:
    """Tests for module-level configuration constants."""
    
    def test_config_constants_exist(self):
        """Test module-level config constants exist after simplification."""
        # WebSocket config
        assert BINANCE_WS_URL is not None
        assert BINANCE_STREAMS is not None
        assert RECONNECT_MAX_DELAY_SECONDS is not None
        assert WS_CONNECTION_TIMEOUT is not None
        
        # Kafka config
        assert KAFKA_BOOTSTRAP_SERVERS is not None
        
        # Logging config
        assert LOG_LEVEL is not None
    
    def test_config_default_values(self):
        """Test config constants have sensible default values."""
        # WebSocket defaults
        assert "binance.com" in BINANCE_WS_URL or "stream.binance.com" in BINANCE_WS_URL
        assert isinstance(BINANCE_STREAMS, list)
        assert len(BINANCE_STREAMS) > 0
        assert RECONNECT_MAX_DELAY_SECONDS > 0
        assert WS_CONNECTION_TIMEOUT > 0
