"""All project constants - Redis, Kafka, PostgreSQL, Binance configs, thresholds, feature definitions."""

# REDIS
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None


class RedisKey:
    AGG = "agg"
    TICKER = "ticker"
    TRADE = "trade"
    ALERT = "alert"
    PRICE = "price"


class RedisTTL:
    AGG = 3600
    TICKER = 60
    TRADE = 300
    ALERT = 86400


class RedisLimit:
    MAX_TRADE = 100
    MAX_ALERT = 1000


# Binance field mapping - maps both WebSocket short names AND connector long names to standard field names
# This is the single source of truth for ticker field normalization
TICKER_BINANCE_MAP = {
    # Binance WebSocket short names
    "c": "last_price",
    "p": "price_change",
    "P": "price_change_pct",
    "o": "open",
    "h": "high",
    "l": "low",
    "v": "volume",
    "q": "quote_volume",
    "n": "trade_count",
    "E": "updated_at",
    # Connector long names (from ingestion/connector.py to_ticker)
    "price_change_percent": "price_change_pct",
    "open_price": "open",
    "high_price": "high",
    "low_price": "low",
    "event_time": "updated_at",
}

# Backward compatibility alias
TICKER_MAP = TICKER_BINANCE_MAP

# KAFKA
KAFKA_SERVER = "kafka:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC_TRADE = "raw_trades"
TOPIC_TICKER = "raw_tickers"

# POSTGRESQL
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_USER = "crypto"
POSTGRES_PASSWORD = "crypto"
POSTGRES_DB = "crypto_data"

POSTGRES_MIN_CONN = 1
POSTGRES_MAX_CONN = 10
POSTGRES_MAX_RETRY = 3
POSTGRES_RETRY_DELAY = 1.0

# Note: TRADE_FIELD and ALERT_FIELD moved to model/market.py
# Use Kline.db_fields() and Alert.db_fields() instead

# BINANCE
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
BINANCE_DATA_URL = "https://data.binance.vision/data/spot/monthly/klines"

# SYMBOLS
DEFAULT_SYMBOL = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "BCHUSDT",
    "XLMUSDT",
    "LINKUSDT",
    "SUIUSDT",
    "ZECUSDT",
    "HBARUSDT",
    "AVAXUSDT",
    "LTCUSDT",
    "SHIBUSDT",
    "UNIUSDT",
    "WLFIUSDT",
    "TONUSDT",
    "DOTUSDT",
    "TAOUSDT",
    "PEPEUSDT",
    "WLDUSDT",
    "AAVEUSDT",
    "TRUMPUSDT",
    "NEARUSDT",
    "ETCUSDT",
    "ONDOUSDT",
    "ICPUSDT",
    "ENAUSDT",
    "ARBUSDT",
    "FILUSDT",
    "APTUSDT",
    "OPUSDT",
    "ATOMUSDT",
    "ALGOUSDT",
    "RENDERUSDT",
    "VETUSDT",
    "BONKUSDT",
]

TICKER_SYMBOL = ",".join(DEFAULT_SYMBOL)

# TRADING
VALID_INTERVAL = {"1m", "5m", "15m", "1h"}
VALID_TRADE_COUNT_INTERVAL = {"1m", "1h", "1d"}
CACHE_HOUR = 1
MAX_TIME_RANGE_DAY = 365

# SPARK
SPARK_CHECKPOINT = "/opt/airflow/data/spark-checkpoints/trade-agg"
PYSPARK_PYTHON = "/usr/local/bin/python3.11"

# JOB
JOB_TRADE_AGG = "trade_aggregation"
JOB_ANOMALY = "anomaly_detection"
JOB_VOLATILITY = "volatility_prediction"

BATCH_SIZE = 100
MAX_RUNTIME = 180
LOOKBACK_HOUR = 2

# MODEL
MODEL_DIR = "ml/model"
MODEL_FILE = "volatility_predictor.json"

# ML model features for volatility prediction
# Terminology:
# - OHLCV: Open, High, Low, Close, Volume
# - VWAP: Volume Weighted Average Price
# - MA: Moving Average
FEATURE_COLUMN = [
    "close",  # Closing price (OHLCV)
    "volume",  # Volume traded (OHLCV)
    "quote_volume",  # Total value traded (price × quantity)
    "trade_count",  # Number of trades in interval
    "return_5m",  # 5-minute price return
    "return_15m",  # 15-minute price return
    "volatility_5m",  # 5-minute rolling volatility
    "volatility_15m",  # 15-minute rolling volatility
    "volatility_30m",  # 30-minute rolling volatility
    "volatility_60m",  # 60-minute rolling volatility
    "volatility_ratio",  # Current vs historical volatility ratio
    "price_range_pct",  # (high - low) / close × 100: percentage price range
    "volume_ratio_60m",  # Current volume vs 60-minute average
    "buy_ratio",  # buy_count / total_trades
    "buy_sell_imbalance",  # (buy_count - sell_count) / total_trades
    "price_vs_ma_15m",  # Price deviation from 15-minute MA
    "price_vs_ma_60m",  # Price deviation from 60-minute MA
    "hour",  # Hour of day (0-23) for temporal patterns
    "symbol_encoded",  # Encoded symbol identifier
]

# ANOMALY DETECTION
VOLUME_THRESHOLD = 1_000_000.0
PRICE_CHANGE_THRESHOLD = 2.0
TRADE_COUNT_MULTIPLIER = 3.0
BUY_RATIO_LOW = 0.3
BUY_RATIO_HIGH = 0.7

VALID_ALERT_TYPE = ["VOLUME_SPIKE", "PRICE_SPIKE", "TRADE_COUNT_SPIKE", "BUY_SELL_IMBALANCE"]
VALID_ALERT_LEVEL = ["HIGH", "MEDIUM", "LOW"]

# API
API_URL = "http://localhost:8000"
RATE_LIMIT = 100
RATE_LIMIT_WINDOW = 60

# DATA
DATA_DIR = "data/historical"
