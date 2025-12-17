"""
Consolidated test module for PySpark Streaming Processor.
Contains all tests for aggregations, anomaly detection, connectors, indicators, and micro-batch processing.

Table of Contents:
- Imports and Setup (line ~20)
- Aggregation Tests (line ~60)
- Anomaly Detector Tests (line ~300)
- Anomaly Detection Property Tests (line ~500)
- Connector Tests (line ~800)
- Technical Indicator Tests (line ~1000)
- Micro-Batch Property Tests (line ~1200)

Requirements: 6.2
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import json
import uuid
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import Mock, patch, MagicMock

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st


# ============================================================================
# AGGREGATION HELPER FUNCTIONS
# ============================================================================

def calculate_ohlcv(trades: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Calculate OHLCV from list of trades.
    
    Args:
        trades: List of trade dictionaries with 'price' and 'quantity'
        
    Returns:
        Dictionary with OHLCV values
    """
    if not trades:
        return {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": 0.0
        }

    prices = [t["price"] for t in trades]
    quantities = [t["quantity"] for t in trades]
    
    return {
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(quantities)
    }


def calculate_vwap(trades: List[Dict[str, Any]]) -> float:
    """Calculate Volume Weighted Average Price."""
    if not trades:
        return 0.0
    
    total_value = sum(t["price"] * t["quantity"] for t in trades)
    total_volume = sum(t["quantity"] for t in trades)
    
    if total_volume == 0:
        return 0.0
    
    return total_value / total_volume


def calculate_price_change_pct(open_price: float, close_price: float) -> float:
    """Calculate price change percentage."""
    if open_price == 0:
        return 0.0
    return ((close_price - open_price) / open_price) * 100


def calculate_buy_sell_ratio(buy_count: int, sell_count: int) -> float:
    """Calculate buy-sell ratio."""
    if sell_count == 0:
        return float('inf')
    return buy_count / sell_count


# ============================================================================
# ANOMALY DETECTION HELPER FUNCTIONS
# ============================================================================

def is_whale_alert(price: float, quantity: float, threshold: float = 100000.0) -> bool:
    """Check if trade qualifies as whale alert."""
    value = price * quantity
    return value > threshold


def is_volume_spike(current_volume: float, historical_volumes: list, multiplier: float = 3.0) -> bool:
    """Check if current volume is a spike."""
    if not historical_volumes:
        return False
    avg_volume = sum(historical_volumes) / len(historical_volumes)
    return current_volume > (avg_volume * multiplier)


def is_price_spike(open_price: float, close_price: float, threshold_pct: float = 2.0) -> bool:
    """Check if price change is a spike."""
    if open_price == 0:
        return False
    price_change_pct = abs(((close_price - open_price) / open_price) * 100)
    return price_change_pct > threshold_pct


def is_rsi_extreme(rsi: float, overbought: float = 70.0, oversold: float = 30.0) -> bool:
    """Check if RSI is in extreme territory."""
    return rsi > overbought or rsi < oversold


def is_bb_breakout(price: float, bb_upper: float, bb_lower: float) -> bool:
    """Check if price breaks out of Bollinger Bands."""
    return price > bb_upper or price < bb_lower


def is_macd_crossover(prev_macd: float, prev_signal: float, 
                      curr_macd: float, curr_signal: float) -> bool:
    """Check if MACD crosses signal line."""
    bullish = (prev_macd <= prev_signal) and (curr_macd > curr_signal)
    bearish = (prev_macd >= prev_signal) and (curr_macd < curr_signal)
    return bullish or bearish


def create_alert(
    timestamp: datetime,
    symbol: str,
    alert_type: str,
    alert_level: str,
    details: Dict[str, Any]
) -> Dict[str, Any]:
    """Create an alert dictionary with all required fields."""
    return {
        "alert_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
        "symbol": symbol,
        "alert_type": alert_type,
        "alert_level": alert_level,
        "details": details,
        "created_at": datetime.now(timezone.utc).isoformat()
    }


# ============================================================================
# OHLCV TESTS
# ============================================================================

class TestOHLCV:
    """Test OHLCV calculation."""
    
    def test_ohlcv_single_trade(self):
        """Test OHLCV calculation with single trade."""
        trades = [{"price": 100.0, "quantity": 1.0}]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 100.0
        assert ohlcv["low"] == 100.0
        assert ohlcv["close"] == 100.0
        assert ohlcv["volume"] == 1.0
    
    def test_ohlcv_multiple_trades(self):
        """Test OHLCV calculation with multiple trades."""
        trades = [
            {"price": 100.0, "quantity": 1.0},
            {"price": 105.0, "quantity": 2.0},
            {"price": 95.0, "quantity": 1.5},
            {"price": 102.0, "quantity": 1.0},
        ]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 105.0
        assert ohlcv["low"] == 95.0
        assert ohlcv["close"] == 102.0
        assert ohlcv["volume"] == 5.5
    
    def test_ohlcv_empty_trades(self):
        """Test OHLCV calculation with empty trades."""
        trades = []
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] is None
        assert ohlcv["high"] is None
        assert ohlcv["low"] is None
        assert ohlcv["close"] is None
        assert ohlcv["volume"] == 0.0
    
    def test_ohlcv_constant_price(self):
        """Test OHLCV with constant price."""
        trades = [
            {"price": 50.0, "quantity": 1.0},
            {"price": 50.0, "quantity": 2.0},
            {"price": 50.0, "quantity": 1.5},
        ]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 50.0
        assert ohlcv["high"] == 50.0
        assert ohlcv["low"] == 50.0
        assert ohlcv["close"] == 50.0
        assert ohlcv["volume"] == 4.5
    
    def test_ohlcv_descending_prices(self):
        """Test OHLCV with descending prices."""
        trades = [
            {"price": 110.0, "quantity": 1.0},
            {"price": 105.0, "quantity": 1.0},
            {"price": 100.0, "quantity": 1.0},
            {"price": 95.0, "quantity": 1.0},
        ]
        ohlcv = calculate_ohlcv(trades)
        
        assert ohlcv["open"] == 110.0
        assert ohlcv["high"] == 110.0
        assert ohlcv["low"] == 95.0
        assert ohlcv["close"] == 95.0


# ============================================================================
# VWAP TESTS
# ============================================================================

class TestVWAP:
    """Test VWAP calculation."""
    
    def test_vwap_calculation(self):
        """Test VWAP calculation."""
        trades = [
            {"price": 100.0, "quantity": 1.0},
            {"price": 110.0, "quantity": 2.0},
        ]
        vwap = calculate_vwap(trades)
        assert abs(vwap - 106.67) < 0.01
    
    def test_vwap_single_trade(self):
        """Test VWAP with single trade."""
        trades = [{"price": 50.0, "quantity": 2.0}]
        vwap = calculate_vwap(trades)
        assert vwap == 50.0
    
    def test_vwap_equal_quantities(self):
        """Test VWAP with equal quantities."""
        trades = [
            {"price": 100.0, "quantity": 1.0},
            {"price": 200.0, "quantity": 1.0},
        ]
        vwap = calculate_vwap(trades)
        assert vwap == 150.0
    
    def test_vwap_empty_trades(self):
        """Test VWAP with empty trades."""
        trades = []
        vwap = calculate_vwap(trades)
        assert vwap == 0.0
    
    def test_vwap_zero_volume(self):
        """Test VWAP with zero total volume."""
        trades = [
            {"price": 100.0, "quantity": 0.0},
            {"price": 200.0, "quantity": 0.0},
        ]
        vwap = calculate_vwap(trades)
        assert vwap == 0.0


# ============================================================================
# PRICE CHANGE TESTS
# ============================================================================

class TestPriceChange:
    """Test price change percentage calculation."""
    
    def test_price_change_positive(self):
        """Test positive price change."""
        pct = calculate_price_change_pct(100.0, 110.0)
        assert pct == 10.0
    
    def test_price_change_negative(self):
        """Test negative price change."""
        pct = calculate_price_change_pct(100.0, 90.0)
        assert pct == -10.0
    
    def test_price_change_zero(self):
        """Test zero price change."""
        pct = calculate_price_change_pct(100.0, 100.0)
        assert pct == 0.0
    
    def test_price_change_zero_open(self):
        """Test price change with zero open price."""
        pct = calculate_price_change_pct(0.0, 100.0)
        assert pct == 0.0


# ============================================================================
# BUY-SELL RATIO TESTS
# ============================================================================

class TestBuySellRatio:
    """Test buy-sell ratio calculation."""
    
    def test_buy_sell_ratio_equal(self):
        """Test buy-sell ratio with equal counts."""
        ratio = calculate_buy_sell_ratio(10, 10)
        assert ratio == 1.0
    
    def test_buy_sell_ratio_more_buys(self):
        """Test buy-sell ratio with more buys."""
        ratio = calculate_buy_sell_ratio(20, 10)
        assert ratio == 2.0
    
    def test_buy_sell_ratio_zero_sells(self):
        """Test buy-sell ratio with zero sells."""
        ratio = calculate_buy_sell_ratio(10, 0)
        assert ratio == float('inf')


# ============================================================================
# WHALE ALERT TESTS
# ============================================================================

class TestWhaleAlert:
    """Test whale alert detection logic."""
    
    def test_whale_alert_at_exact_threshold(self):
        """Test whale alert at exact $100k threshold."""
        assert is_whale_alert(50000.0, 2.0) == False
        assert is_whale_alert(50000.0, 2.001) == True
    
    def test_whale_alert_above_threshold(self):
        """Test whale alert above threshold."""
        assert is_whale_alert(50000.0, 3.0) == True
        assert is_whale_alert(100000.0, 2.0) == True
    
    def test_whale_alert_below_threshold(self):
        """Test whale alert below threshold."""
        assert is_whale_alert(50000.0, 1.0) == False
        assert is_whale_alert(10000.0, 5.0) == False


# ============================================================================
# VOLUME SPIKE TESTS
# ============================================================================

class TestVolumeSpike:
    """Test volume spike detection logic."""
    
    def test_volume_spike_at_exact_threshold(self):
        """Test volume spike at exact 3x threshold."""
        volumes = [100.0] * 20
        assert is_volume_spike(300.0, volumes) == False
        assert is_volume_spike(300.1, volumes) == True
    
    def test_volume_spike_above_threshold(self):
        """Test volume spike above threshold."""
        volumes = [100.0] * 20
        assert is_volume_spike(400.0, volumes) == True
    
    def test_volume_spike_empty_history(self):
        """Test volume spike with empty history."""
        assert is_volume_spike(1000.0, []) == False


# ============================================================================
# PRICE SPIKE TESTS
# ============================================================================

class TestPriceSpike:
    """Test price spike detection logic."""
    
    def test_price_spike_at_exact_threshold(self):
        """Test price spike at exact 2% threshold."""
        assert is_price_spike(100.0, 102.0) == False
        assert is_price_spike(100.0, 102.01) == True
    
    def test_price_spike_above_threshold(self):
        """Test price spike above threshold."""
        assert is_price_spike(100.0, 105.0) == True
        assert is_price_spike(100.0, 95.0) == True
    
    def test_price_spike_zero_open(self):
        """Test price spike with zero open price."""
        assert is_price_spike(0.0, 100.0) == False


# ============================================================================
# RSI EXTREME TESTS
# ============================================================================

class TestRSIExtreme:
    """Test RSI extreme detection logic."""
    
    def test_rsi_extreme_at_boundaries(self):
        """Test RSI extreme at exact boundaries."""
        assert is_rsi_extreme(70.0) == False
        assert is_rsi_extreme(30.0) == False
        assert is_rsi_extreme(70.1) == True
        assert is_rsi_extreme(29.9) == True
    
    def test_rsi_extreme_overbought(self):
        """Test RSI extreme in overbought territory."""
        assert is_rsi_extreme(71.0) == True
        assert is_rsi_extreme(80.0) == True
    
    def test_rsi_extreme_oversold(self):
        """Test RSI extreme in oversold territory."""
        assert is_rsi_extreme(29.0) == True
        assert is_rsi_extreme(20.0) == True


# ============================================================================
# BOLLINGER BAND BREAKOUT TESTS
# ============================================================================

class TestBBBreakout:
    """Test Bollinger Band breakout detection logic."""
    
    def test_bb_breakout_above_upper(self):
        """Test BB breakout above upper band."""
        assert is_bb_breakout(111.0, 110.0, 90.0) == True
    
    def test_bb_breakout_below_lower(self):
        """Test BB breakout below lower band."""
        assert is_bb_breakout(89.0, 110.0, 90.0) == True
    
    def test_bb_breakout_inside_bands(self):
        """Test BB breakout inside bands."""
        assert is_bb_breakout(100.0, 110.0, 90.0) == False
    
    def test_bb_breakout_at_bands(self):
        """Test BB breakout at exact band values."""
        assert is_bb_breakout(110.0, 110.0, 90.0) == False
        assert is_bb_breakout(90.0, 110.0, 90.0) == False


# ============================================================================
# MACD CROSSOVER TESTS
# ============================================================================

class TestMACDCrossover:
    """Test MACD crossover detection logic."""
    
    def test_macd_crossover_bullish(self):
        """Test bullish MACD crossover."""
        assert is_macd_crossover(-1.0, 0.0, 1.0, 0.0) == True
        assert is_macd_crossover(0.0, 0.0, 1.0, 0.0) == True
    
    def test_macd_crossover_bearish(self):
        """Test bearish MACD crossover."""
        assert is_macd_crossover(1.0, 0.0, -1.0, 0.0) == True
        assert is_macd_crossover(0.0, 0.0, -1.0, 0.0) == True
    
    def test_macd_crossover_no_cross(self):
        """Test no MACD crossover."""
        assert is_macd_crossover(1.0, 0.0, 2.0, 0.0) == False
        assert is_macd_crossover(-2.0, 0.0, -1.0, 0.0) == False


# ============================================================================
# ALERT COMPLETENESS PROPERTY TESTS
# ============================================================================

# Strategies for generating test data
symbol_strategy = st.sampled_from(["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"])
alert_type_strategy = st.sampled_from([
    "WHALE_ALERT", "VOLUME_SPIKE", "PRICE_SPIKE", 
    "RSI_EXTREME", "BB_BREAKOUT", "MACD_CROSSOVER"
])
alert_level_strategy = st.sampled_from(["HIGH", "MEDIUM", "LOW"])
timestamp_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2030, 12, 31)
)

details_strategy = st.one_of(
    st.fixed_dictionaries({
        "price": st.floats(min_value=0.01, max_value=1000000.0, allow_nan=False, allow_infinity=False),
        "quantity": st.floats(min_value=0.001, max_value=10000.0, allow_nan=False, allow_infinity=False),
        "value": st.floats(min_value=100001.0, max_value=10000000.0, allow_nan=False, allow_infinity=False),
    }),
    st.fixed_dictionaries({
        "rsi_14": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        "condition": st.sampled_from(["OVERBOUGHT", "OVERSOLD"]),
    }),
)


class TestAlertCompleteness:
    """
    **Feature: pyspark-simplification, Property 1: Alert completeness**
    **Validates: Requirements 3.1**
    """
    
    @settings(max_examples=100)
    @given(
        timestamp=timestamp_strategy,
        symbol=symbol_strategy,
        alert_type=alert_type_strategy,
        alert_level=alert_level_strategy,
        details=details_strategy,
    )
    def test_alert_contains_all_required_fields(
        self,
        timestamp: datetime,
        symbol: str,
        alert_type: str,
        alert_level: str,
        details: Dict[str, Any],
    ):
        """For any anomaly data, the generated alert must contain all required fields."""
        alert = create_alert(
            timestamp=timestamp,
            symbol=symbol,
            alert_type=alert_type,
            alert_level=alert_level,
            details=details,
        )
        
        required_fields = ["alert_id", "timestamp", "symbol", "alert_type", "alert_level", "details", "created_at"]
        
        for field in required_fields:
            assert field in alert, f"Missing required field: {field}"
            assert alert[field] is not None, f"Field {field} is None"
        
        try:
            uuid.UUID(alert["alert_id"])
        except ValueError:
            pytest.fail(f"alert_id is not a valid UUID: {alert['alert_id']}")


# ============================================================================
# ANOMALY TYPE COVERAGE PROPERTY TESTS
# ============================================================================

class TestAnomalyTypeCoverage:
    """
    **Feature: pyspark-simplification, Property 3: Anomaly type coverage**
    **Validates: Requirements 7.1**
    """
    
    @settings(max_examples=100)
    @given(
        price=st.floats(min_value=1000.0, max_value=100000.0, allow_nan=False, allow_infinity=False),
        quantity=st.floats(min_value=0.1, max_value=1000.0, allow_nan=False, allow_infinity=False),
    )
    def test_whale_alert_detection(self, price: float, quantity: float):
        """Whale alerts should be correctly detected when trade value > $100,000."""
        trade_value = price * quantity
        is_whale = is_whale_alert(price, quantity)
        
        if trade_value > 100000.0:
            assert is_whale, f"Should detect whale alert for value ${trade_value:,.2f}"
        else:
            assert not is_whale, f"Should not detect whale alert for value ${trade_value:,.2f}"

    @settings(max_examples=100)
    @given(
        rsi=st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    def test_rsi_extreme_detection(self, rsi: float):
        """RSI extremes should be correctly detected when RSI > 70 or RSI < 30."""
        is_extreme = is_rsi_extreme(rsi)
        
        if rsi > 70.0 or rsi < 30.0:
            assert is_extreme, f"Should detect RSI extreme for RSI={rsi:.2f}"
        else:
            assert not is_extreme, f"Should not detect RSI extreme for RSI={rsi:.2f}"


# ============================================================================
# REDIS CONNECTOR TESTS
# ============================================================================

class TestRedisConnector:
    """Test Redis connector operations."""
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_write_hash_success(self, mock_pool, mock_redis):
        """Test Redis write with hash type."""
        from src.storage.redis import RedisConnector
        
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.return_value = True
        
        connector = RedisConnector(host="localhost", port=6379)
        connector.write_to_redis(
            key="test:key",
            value={"field1": "value1", "field2": "value2"},
            ttl_seconds=3600,
            value_type="hash"
        )
        
        mock_client.hset.assert_called_once_with(
            "test:key",
            mapping={"field1": "value1", "field2": "value2"}
        )
        mock_client.expire.assert_called_once_with("test:key", 3600)
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_write_list_success(self, mock_pool, mock_redis):
        """Test Redis write with list type."""
        from src.storage.redis import RedisConnector
        
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.return_value = True
        
        connector = RedisConnector(host="localhost", port=6379)
        connector.write_to_redis(
            key="test:list",
            value={"alert": "data"},
            value_type="list",
            list_max_size=1000
        )
        
        mock_client.lpush.assert_called_once()
        mock_client.ltrim.assert_called_once_with("test:list", 0, 999)
    
    @patch('redis.Redis')
    @patch('redis.ConnectionPool')
    def test_redis_connection_failure(self, mock_pool, mock_redis):
        """Test Redis connection failure handling."""
        from src.storage.redis import RedisConnector
        
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.ping.side_effect = Exception("Connection refused")
        
        connector = RedisConnector(host="localhost", port=6379)
        
        with pytest.raises(Exception) as exc_info:
            _ = connector.client
        
        assert "Connection refused" in str(exc_info.value)


# ============================================================================
# TECHNICAL INDICATOR TESTS
# ============================================================================

# ============================================================================
# NOTE: Technical indicator tests (TestSMA, TestEMA, TestRSI, TestMACD, 
# TestBollingerBands, TestATR) were removed as part of the simplify-indicators
# feature. The technical_indicators_job.py module was removed to simplify
# the system for regular users who don't need complex technical indicators.
# See: .kiro/specs/simplify-indicators/requirements.md
# ============================================================================


# ============================================================================
# MICRO-BATCH CONTROLLER
# ============================================================================

class MicroBatchController:
    """Simulates the micro-batch auto-stop logic from streaming jobs."""
    
    def __init__(self, empty_batch_threshold: int = 3, max_runtime_seconds: int = 300):
        self.empty_batch_count: int = 0
        self.empty_batch_threshold: int = empty_batch_threshold
        self.max_runtime_seconds: int = max_runtime_seconds
        self.start_time: float = 0.0
        self.stop_reason: str = ""
    
    def should_stop(self, is_empty_batch: bool, current_time: float) -> bool:
        """Check if job should stop based on empty batch count or timeout."""
        if self.start_time > 0 and (current_time - self.start_time) > self.max_runtime_seconds:
            self.stop_reason = f"Max runtime {self.max_runtime_seconds}s exceeded"
            return True
        
        if is_empty_batch:
            self.empty_batch_count += 1
            if self.empty_batch_count >= self.empty_batch_threshold:
                self.stop_reason = f"{self.empty_batch_threshold} consecutive empty batches"
                return True
        else:
            self.empty_batch_count = 0
        
        return False
    
    def process_batch_sequence(self, batch_sequence: List[bool], start_time: float = 0.0) -> int:
        """Process a sequence of batches and return the index where stop was triggered."""
        self.start_time = start_time
        self.empty_batch_count = 0
        self.stop_reason = ""
        
        for i, is_empty in enumerate(batch_sequence):
            current_time = start_time + i
            if self.should_stop(is_empty, current_time):
                return i
        
        return -1


# ============================================================================
# MICRO-BATCH PROPERTY TESTS
# ============================================================================

class TestEmptyBatchCountingTriggersStop:
    """
    **Feature: micro-batch-processing, Property 1: Empty batch counting triggers stop**
    **Validates: Requirements 1.1**
    """
    
    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=1, max_value=10),
        prefix_length=st.integers(min_value=0, max_value=20),
    )
    def test_stops_after_threshold_consecutive_empty_batches(
        self,
        empty_batch_threshold: int,
        prefix_length: int,
    ):
        """When N consecutive empty batches are received, the job should stop."""
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        batch_sequence = [False] * prefix_length + [True] * empty_batch_threshold
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        expected_stop_index = prefix_length + empty_batch_threshold - 1
        
        assert stop_index == expected_stop_index, \
            f"Expected stop at index {expected_stop_index}, got {stop_index}"

    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=2, max_value=10),
        num_empty_batches=st.integers(min_value=1, max_value=20),
    )
    def test_does_not_stop_before_threshold(
        self,
        empty_batch_threshold: int,
        num_empty_batches: int,
    ):
        """When fewer than N consecutive empty batches are received, the job should not stop."""
        assume(num_empty_batches < empty_batch_threshold)
        
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        batch_sequence = [True] * num_empty_batches
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        assert stop_index == -1, \
            f"Should not stop with {num_empty_batches} empty batches (threshold={empty_batch_threshold})"


class TestNonEmptyBatchResetsCounter:
    """
    **Feature: micro-batch-processing, Property 2: Non-empty batch resets counter**
    **Validates: Requirements 1.2**
    """
    
    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=2, max_value=10),
        num_empty_before_reset=st.integers(min_value=1, max_value=20),
    )
    def test_non_empty_batch_resets_counter(
        self,
        empty_batch_threshold: int,
        num_empty_before_reset: int,
    ):
        """A non-empty batch should reset the empty batch counter to zero."""
        assume(num_empty_before_reset < empty_batch_threshold)
        
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        for _ in range(num_empty_before_reset):
            controller.should_stop(is_empty_batch=True, current_time=0)
        
        assert controller.empty_batch_count == num_empty_before_reset
        
        controller.should_stop(is_empty_batch=False, current_time=0)
        
        assert controller.empty_batch_count == 0

    @settings(max_examples=100)
    @given(
        empty_batch_threshold=st.integers(min_value=3, max_value=10),
        num_cycles=st.integers(min_value=1, max_value=5),
    )
    def test_reset_allows_more_empty_batches(
        self,
        empty_batch_threshold: int,
        num_cycles: int,
    ):
        """After reset, the job should be able to process more empty batches before stopping."""
        controller = MicroBatchController(empty_batch_threshold=empty_batch_threshold)
        
        batch_sequence = []
        for _ in range(num_cycles):
            batch_sequence.extend([True] * (empty_batch_threshold - 1))
            batch_sequence.append(False)
        
        stop_index = controller.process_batch_sequence(batch_sequence)
        
        assert stop_index == -1, \
            f"Should not stop when resetting before threshold, but stopped at index {stop_index}"


# ============================================================================
# INTEGRATED AGGREGATION TESTS
# ============================================================================

class TestIntegratedAggregation:
    """Test integrated aggregation workflow."""
    
    def test_full_aggregation_workflow(self):
        """Test complete aggregation workflow with all metrics."""
        trades = [
            {"price": 100.0, "quantity": 1.0},
            {"price": 105.0, "quantity": 2.0},
            {"price": 95.0, "quantity": 1.5},
            {"price": 102.0, "quantity": 1.0},
        ]
        
        ohlcv = calculate_ohlcv(trades)
        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 105.0
        assert ohlcv["low"] == 95.0
        assert ohlcv["close"] == 102.0
        assert ohlcv["volume"] == 5.5
        
        vwap = calculate_vwap(trades)
        assert abs(vwap - 100.818) < 0.01
        
        price_change = calculate_price_change_pct(ohlcv["open"], ohlcv["close"])
        assert price_change == 2.0
    
    def test_aggregation_with_extreme_values(self):
        """Test aggregation with extreme price movements."""
        trades = [
            {"price": 1000.0, "quantity": 0.1},
            {"price": 500.0, "quantity": 0.2},
            {"price": 2000.0, "quantity": 0.05},
        ]
        
        ohlcv = calculate_ohlcv(trades)
        assert ohlcv["open"] == 1000.0
        assert ohlcv["high"] == 2000.0
        assert ohlcv["low"] == 500.0
        assert ohlcv["close"] == 2000.0
        
        price_change = calculate_price_change_pct(ohlcv["open"], ohlcv["close"])
        assert price_change == 100.0


# ============================================================================
# KAFKA CONNECTOR TESTS
# ============================================================================

class TestKafkaConnector:
    """Test Kafka connector operations (mocked)."""
    
    @patch('kafka.KafkaProducer')
    def test_kafka_producer_creation(self, mock_producer_class):
        """Test Kafka producer can be created."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        from kafka import KafkaProducer
        
        producer = KafkaProducer(bootstrap_servers="localhost:9092")
        
        assert producer is not None
        mock_producer_class.assert_called_once()
