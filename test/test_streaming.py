"""Tests for data models, field normalization, and data quality validation."""

import uuid
from datetime import datetime
from typing import Any

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from processing.validator.aggregation_validator import build_aggregation_expectations
from processing.validator.aggregation_validator import run_ge_validation as run_aggregation_ge_validation
from processing.validator.anomaly_validator import build_anomaly_expectations
from processing.validator.anomaly_validator import run_ge_validation as run_anomaly_ge_validation
from schema.market import Alert, Kline, Ticker, Trade

# ==========================================
# Kline Model
# ==========================================


class TestKlineModel:
    def test_basic_creation(self):
        kline = Kline(
            timestamp=datetime.now(),
            symbol="BTCUSDT",
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            quote_volume=5050000.0,
            trade_count=1000,
            buy_count=600,
            sell_count=400,
        )
        assert kline.symbol == "BTCUSDT"
        assert kline.high >= kline.low

    def test_timestamp_from_epoch_ms(self):
        kline = Kline(
            timestamp=1700000000000,
            symbol="ETHUSDT",
            open=2000.0,
            high=2100.0,
            low=1900.0,
            close=2050.0,
            volume=500.0,
            quote_volume=1025000.0,
            trade_count=200,
            buy_count=100,
            sell_count=100,
        )
        assert isinstance(kline.timestamp, datetime)

    def test_string_to_float_parsing(self):
        kline = Kline(
            timestamp=datetime.now(),
            symbol="BTCUSDT",
            open="50000",
            high="51000",
            low="49000",
            close="50500",
            volume="100",
            quote_volume="5050000",
            trade_count="1000",
            buy_count="600",
            sell_count="400",
        )
        assert kline.open == 50000.0
        assert kline.trade_count == 1000

    def test_to_redis_dict_and_back(self):
        kline = Kline(
            timestamp=datetime.now(),
            symbol="BTCUSDT",
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            quote_volume=5050000.0,
            trade_count=1000,
            buy_count=600,
            sell_count=400,
        )
        redis_dict = kline.to_redis_dict()
        restored = Kline.from_redis_dict(redis_dict)
        assert restored.symbol == kline.symbol
        assert restored.open == kline.open

    def test_db_fields(self):
        fields = Kline.db_fields()
        for f in ["timestamp", "symbol", "open", "high", "low", "close", "volume"]:
            assert f in fields


# ==========================================
# Alert Model
# ==========================================


class TestAlertModel:
    def test_basic_creation(self):
        alert = Alert(
            timestamp=datetime.now(),
            symbol="BTCUSDT",
            alert_type="VOLUME_SPIKE",
            alert_level="HIGH",
        )
        assert alert.symbol == "BTCUSDT"
        assert alert.alert_type == "VOLUME_SPIKE"
        assert alert.message

    def test_auto_message_generation(self):
        alert = Alert(
            timestamp=datetime.now(),
            symbol="ETHUSDT",
            alert_type="PRICE_SPIKE",
            alert_level="MEDIUM",
        )
        assert "PRICE_SPIKE" in alert.message
        assert "ETHUSDT" in alert.message

    def test_details_from_json_string(self):
        alert = Alert(
            timestamp=datetime.now(),
            symbol="BTCUSDT",
            alert_type="VOLUME_SPIKE",
            alert_level="HIGH",
            details='{"volume": 1000000}',
        )
        assert alert.details == {"volume": 1000000}

    def test_to_redis_dict_and_back(self):
        alert = Alert(
            timestamp=datetime.now(),
            symbol="BTCUSDT",
            alert_type="VOLUME_SPIKE",
            alert_level="HIGH",
            details={"volume": 1000000},
        )
        redis_dict = alert.to_redis_dict()
        restored = Alert.from_redis_dict(redis_dict)
        assert restored.symbol == alert.symbol
        assert restored.alert_type == alert.alert_type

    def test_db_fields(self):
        fields = Alert.db_fields()
        for f in ["timestamp", "symbol", "alert_type", "alert_level"]:
            assert f in fields


# ==========================================
# Ticker Model
# ==========================================


class TestTickerModel:
    def test_basic_creation(self):
        ticker = Ticker(
            symbol="BTCUSDT",
            last_price=50000.0,
            price_change=500.0,
            price_change_pct=1.0,
            open=49500.0,
            high=51000.0,
            low=49000.0,
            volume=1000.0,
            quote_volume=50000000.0,
            trade_count=5000,
            updated_at=datetime.now(),
        )
        assert ticker.symbol == "BTCUSDT"
        assert ticker.is_complete is True

    def test_is_complete_false_when_no_trades(self):
        ticker = Ticker(symbol="BTCUSDT", trade_count=0, quote_volume=100.0)
        assert ticker.is_complete is False

    def test_binance_connector_field_mapping(self):
        data = {
            "symbol": "BTCUSDT",
            "last_price": "50000",
            "price_change": "500",
            "price_change_percent": "1.0",
            "open_price": "49500",
            "high_price": "51000",
            "low_price": "49000",
            "volume": "1000",
            "quote_volume": "50000000",
            "trade_count": 5000,
            "event_time": 1700000000000,
        }
        ticker = Ticker.model_validate(data)
        assert ticker.price_change_pct == 1.0
        assert ticker.open == 49500.0
        assert ticker.high == 51000.0
        assert ticker.low == 49000.0
        assert isinstance(ticker.updated_at, datetime)

    def test_binance_websocket_field_mapping(self):
        data = {
            "symbol": "ETHUSDT",
            "c": "2000",
            "p": "50",
            "P": "2.5",
            "o": "1950",
            "h": "2100",
            "l": "1900",
            "v": "500",
            "q": "1000000",
            "n": 3000,
            "E": 1700000000000,
        }
        ticker = Ticker.model_validate(data)
        assert ticker.last_price == 2000.0
        assert ticker.price_change == 50.0
        assert ticker.price_change_pct == 2.5
        assert ticker.open == 1950.0
        assert ticker.trade_count == 3000

    def test_to_redis_dict_and_back(self):
        ticker = Ticker(
            symbol="BTCUSDT",
            last_price=50000.0,
            volume=1000.0,
            quote_volume=50000000.0,
            trade_count=5000,
        )
        redis_dict = ticker.to_redis_dict()
        restored = Ticker.from_redis_dict(redis_dict)
        assert restored.symbol == ticker.symbol
        assert restored.last_price == ticker.last_price


# ==========================================
# Trade Model
# ==========================================


class TestTradeModel:
    def test_basic_creation(self):
        trade = Trade(
            symbol="BTCUSDT",
            trade_id=12345,
            price=50000.0,
            quantity=0.5,
            timestamp=datetime.now(),
            side="BUY",
        )
        assert trade.symbol == "BTCUSDT"
        assert trade.total == 25000.0

    def test_kafka_field_normalization(self):
        data = {
            "symbol": "BTCUSDT",
            "trade_id": 12345,
            "price": "50000",
            "quantity": "0.5",
            "trade_time": 1700000000000,
            "is_buyer_maker": True,
        }
        trade = Trade.model_validate(data)
        assert trade.side == "SELL"
        assert isinstance(trade.timestamp, datetime)

    def test_buyer_not_maker_is_buy(self):
        data = {
            "symbol": "ETHUSDT",
            "trade_id": 1,
            "price": "2000",
            "quantity": "1.0",
            "trade_time": 1700000000000,
            "is_buyer_maker": False,
        }
        trade = Trade.model_validate(data)
        assert trade.side == "BUY"

    def test_to_redis_dict_and_back(self):
        trade = Trade(
            symbol="BTCUSDT",
            trade_id=12345,
            price=50000.0,
            quantity=0.5,
            timestamp=datetime.now(),
            side="BUY",
        )
        redis_dict = trade.to_redis_dict()
        restored = Trade.from_redis_dict(redis_dict)
        assert restored.symbol == trade.symbol
        assert restored.price == trade.price


# ==========================================
# OHLCV Calculation Logic
# ==========================================


def calculate_ohlcv(trades: list[dict[str, Any]]) -> dict[str, float]:
    if not trades:
        return {"open": None, "high": None, "low": None, "close": None, "volume": 0.0}
    prices = [t["price"] for t in trades]
    quantities = [t["quantity"] for t in trades]
    return {"open": prices[0], "high": max(prices), "low": min(prices), "close": prices[-1], "volume": sum(quantities)}


class TestOHLCV:
    def test_single_trade(self):
        ohlcv = calculate_ohlcv([{"price": 100.0, "quantity": 1.0}])
        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 100.0
        assert ohlcv["low"] == 100.0
        assert ohlcv["close"] == 100.0
        assert ohlcv["volume"] == 1.0

    def test_multiple_trades(self):
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

    def test_empty_trades(self):
        ohlcv = calculate_ohlcv([])
        assert ohlcv["open"] is None
        assert ohlcv["volume"] == 0.0


# ==========================================
# Micro-Batch Processing
# ==========================================


class MicroBatchController:
    def __init__(self, empty_batch_threshold: int = 3):
        self.empty_batch_count: int = 0
        self.empty_batch_threshold: int = empty_batch_threshold

    def should_stop(self, is_empty_batch: bool) -> bool:
        if is_empty_batch:
            self.empty_batch_count += 1
            return self.empty_batch_count >= self.empty_batch_threshold
        else:
            self.empty_batch_count = 0
            return False


class TestMicroBatchStop:
    @given(
        threshold=st.integers(min_value=1, max_value=10),
        prefix=st.integers(min_value=0, max_value=10),
    )
    @settings(max_examples=50)
    def test_stops_after_threshold(self, threshold, prefix):
        controller = MicroBatchController(empty_batch_threshold=threshold)
        for _ in range(prefix):
            controller.should_stop(False)
        for _i in range(threshold):
            result = controller.should_stop(True)
        assert result is True

    @given(
        threshold=st.integers(min_value=2, max_value=10),
        n_empty=st.integers(min_value=1, max_value=20),
    )
    @settings(max_examples=50)
    def test_does_not_stop_before_threshold(self, threshold, n_empty):
        assume(n_empty < threshold)
        controller = MicroBatchController(empty_batch_threshold=threshold)
        for _i in range(n_empty):
            result = controller.should_stop(True)
        assert result is False

    def test_non_empty_resets_counter(self):
        controller = MicroBatchController(empty_batch_threshold=3)
        controller.should_stop(True)
        controller.should_stop(True)
        assert controller.empty_batch_count == 2
        controller.should_stop(False)
        assert controller.empty_batch_count == 0


# ==========================================
# Data Quality - Aggregation (GE)
# ==========================================


class TestAggregationDataQuality:
    def test_valid_records_pass(self):
        records = [
            {"symbol": "BTCUSDT", "open": 50000.0, "high": 51000.0, "low": 49000.0, "close": 50500.0, "volume": 100.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is True

    def test_invalid_ohlc_fails(self):
        records = [
            {"symbol": "BTCUSDT", "open": 50000.0, "high": 48000.0, "low": 49000.0, "close": 50500.0, "volume": 100.0}
        ]
        result = run_aggregation_ge_validation(records, build_aggregation_expectations())
        assert result.success is False


# ==========================================
# Data Quality - Anomaly (GE)
# ==========================================


class TestAnomalyDataQuality:
    def test_valid_alert_passes(self):
        records = [
            {
                "alert_id": str(uuid.uuid4()),
                "symbol": "BTCUSDT",
                "alert_type": "VOLUME_SPIKE",
                "alert_level": "HIGH",
                "timestamp": datetime.now().isoformat(),
                "created_at": datetime.now().isoformat(),
            }
        ]
        result = run_anomaly_ge_validation(records, build_anomaly_expectations())
        assert result.success is True

    def test_invalid_type_fails(self):
        records = [
            {
                "alert_id": str(uuid.uuid4()),
                "symbol": "BTCUSDT",
                "alert_type": "INVALID",
                "alert_level": "HIGH",
                "timestamp": datetime.now().isoformat(),
                "created_at": datetime.now().isoformat(),
            }
        ]
        result = run_anomaly_ge_validation(records, build_anomaly_expectations())
        assert result.success is False
