-- =============================================================================
-- Binance Market Streaming Pipeline - Database Schema
-- =============================================================================
-- Run this once to initialize the database schema.
-- Docker will auto-execute this file on first container startup.
-- =============================================================================

-- 0. Create Airflow database (for orchestration)
-- This allows us to use a single PostgreSQL instance for both app data and Airflow
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- 1. TimescaleDB Extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =============================================================================
-- 2. TRADES TABLES
-- =============================================================================

-- trades_1m: 1-minute OHLCV kline aggregation from raw trades
-- OHLCV = Open, High, Low, Close, Volume (standard price/volume format)
CREATE TABLE IF NOT EXISTS trades_1m (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    -- OHLCV core fields
    open DOUBLE PRECISION,           -- Opening price (first trade in interval)
    high DOUBLE PRECISION,           -- Highest price during interval
    low DOUBLE PRECISION,            -- Lowest price during interval
    close DOUBLE PRECISION,          -- Closing price (last trade in interval)
    volume DOUBLE PRECISION,         -- Total volume (quantity) traded
    -- Derived metrics
    quote_volume DOUBLE PRECISION,   -- Total value traded (price × quantity sum)
    trade_count INTEGER,             -- Number of trades in interval
    buy_count INTEGER,               -- Number of buyer-initiated trades
    sell_count INTEGER,              -- Number of seller-initiated trades
    volume_weighted_avg_price DOUBLE PRECISION,  -- VWAP: Volume Weighted Average Price
    price_change_percent DOUBLE PRECISION,       -- Percentage change from open to close
    buy_sell_ratio DOUBLE PRECISION,             -- buy_count / total_trades
    average_price DOUBLE PRECISION,              -- Alias for VWAP
    price_volatility DOUBLE PRECISION            -- Standard deviation of trade prices
);

-- staging_trades_1m: Temporary staging table for merging into trades_1m
CREATE TABLE IF NOT EXISTS staging_trades_1m (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    trade_count INTEGER,
    buy_count INTEGER,
    sell_count INTEGER,
    volume_weighted_avg_price DOUBLE PRECISION,
    price_change_percent DOUBLE PRECISION,
    buy_sell_ratio DOUBLE PRECISION,
    average_price DOUBLE PRECISION,
    price_volatility DOUBLE PRECISION
);

-- =============================================================================
-- 3. ML TABLES
-- =============================================================================

-- ml_features: Feature engineering table for volatility prediction ML model
-- Contains time-series features computed from OHLCV kline data
CREATE TABLE IF NOT EXISTS ml_features (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    trade_count INTEGER,
    -- Return features (price change percentages)
    return_1m DOUBLE PRECISION,      -- 1-minute price return
    return_5m DOUBLE PRECISION,      -- 5-minute price return
    return_15m DOUBLE PRECISION,     -- 15-minute price return
    -- Volatility features (rolling window standard deviations)
    volatility_5m DOUBLE PRECISION,  -- 5-minute rolling volatility
    volatility_15m DOUBLE PRECISION, -- 15-minute rolling volatility
    volatility_30m DOUBLE PRECISION, -- 30-minute rolling volatility
    volatility_60m DOUBLE PRECISION, -- 60-minute rolling volatility
    volatility_ratio DOUBLE PRECISION, -- Current vs historical volatility ratio
    -- Kline pattern features (price range and body metrics)
    price_range_pct DOUBLE PRECISION,  -- (high - low) / close × 100: percentage price range
    price_body_pct DOUBLE PRECISION,   -- |close - open| / close × 100: percentage kline body size
    -- Volume features (relative volume comparisons)
    volume_ratio_15m DOUBLE PRECISION, -- Current volume vs 15-minute average
    volume_ratio_60m DOUBLE PRECISION, -- Current volume vs 60-minute average
    -- Order flow features (buy/sell pressure metrics)
    buy_ratio DOUBLE PRECISION,        -- buy_count / total_trades
    buy_sell_imbalance DOUBLE PRECISION, -- (buy_count - sell_count) / total_trades
    -- Price vs MA features (moving average comparisons)
    price_vs_ma_15m DOUBLE PRECISION,  -- Price deviation from 15-minute MA
    price_vs_ma_60m DOUBLE PRECISION,  -- Price deviation from 60-minute MA
    -- Time features (temporal patterns)
    hour INTEGER,                      -- Hour of day (0-23)
    symbol_encoded INTEGER,            -- Encoded symbol identifier
    -- Target variable (for supervised learning)
    volatility_next_5m DOUBLE PRECISION, -- Future 5-minute volatility (label)
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp)
);

-- volatility_predictions: ML model predictions for future volatility
CREATE TABLE IF NOT EXISTS volatility_predictions (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    current_volatility DOUBLE PRECISION,       -- Current observed volatility
    predicted_volatility_5m DOUBLE PRECISION,  -- Predicted 5-minute ahead volatility
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp)
);

-- =============================================================================
-- 4. ALERTS TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    message TEXT,
    metadata TEXT,
    UNIQUE (timestamp, symbol, alert_type)
);

CREATE TABLE IF NOT EXISTS staging_alerts (
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    message TEXT,
    metadata TEXT
);

-- =============================================================================
-- 5. VALIDATION & CHECKPOINTS
-- =============================================================================

CREATE TABLE IF NOT EXISTS validation_errors (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,
    record_data JSONB NOT NULL,
    failed_expectations JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_checkpoints (
    job_name VARCHAR(100) PRIMARY KEY,
    last_processed_timestamp TIMESTAMPTZ NOT NULL,
    records_processed BIGINT DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- 6. INDEXES
-- =============================================================================

-- Trades
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_1m_symbol_ts ON trades_1m(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_1m_ts ON trades_1m(timestamp DESC);

-- ML features
CREATE INDEX IF NOT EXISTS idx_ml_features_ts ON ml_features(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ml_features_symbol_ts ON ml_features(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ml_features_training ON ml_features(timestamp DESC) WHERE volatility_next_5m IS NOT NULL;

-- Volatility predictions
CREATE INDEX IF NOT EXISTS idx_volatility_predictions_ts ON volatility_predictions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_volatility_predictions_symbol_ts ON volatility_predictions(symbol, timestamp DESC);

-- Alerts
CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(timestamp DESC, symbol);

-- Validation errors
CREATE INDEX IF NOT EXISTS idx_validation_errors_created ON validation_errors(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_validation_errors_source ON validation_errors(source_type, created_at DESC);

-- Job checkpoints
CREATE INDEX IF NOT EXISTS idx_job_checkpoints_updated ON job_checkpoints(updated_at DESC);

-- =============================================================================
-- 7. TIMESCALEDB HYPERTABLE
-- =============================================================================

SELECT create_hypertable(
    'trades_1m', 
    'timestamp',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- =============================================================================
-- 8. COMPRESSION SETTINGS
-- =============================================================================

DO $$
BEGIN
    ALTER TABLE trades_1m SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'symbol'
    );
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Compression already configured or error: %', SQLERRM;
END $$;

SELECT add_compression_policy('trades_1m', INTERVAL '7 days', if_not_exists => TRUE);

-- =============================================================================
-- 9. RETENTION POLICY
-- =============================================================================

SELECT add_retention_policy('trades_1m', INTERVAL '90 days', if_not_exists => TRUE);

-- =============================================================================
-- 10. CONTINUOUS AGGREGATES
-- =============================================================================

-- 5-minute aggregate
DO $$
BEGIN
    CREATE MATERIALIZED VIEW trades_5m
    WITH (timescaledb.continuous) AS
    SELECT
        time_bucket(INTERVAL '5 minutes', timestamp) AS timestamp,
        symbol,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume,
        sum(quote_volume) AS quote_volume,
        sum(trade_count) AS trade_count,
        sum(buy_count) AS buy_count,
        sum(sell_count) AS sell_count
    FROM trades_1m
    GROUP BY time_bucket(INTERVAL '5 minutes', timestamp), symbol
    WITH NO DATA;
EXCEPTION WHEN duplicate_table THEN
    RAISE NOTICE 'trades_5m already exists';
END $$;

-- 15-minute aggregate
DO $$
BEGIN
    CREATE MATERIALIZED VIEW trades_15m
    WITH (timescaledb.continuous) AS
    SELECT
        time_bucket(INTERVAL '15 minutes', timestamp) AS timestamp,
        symbol,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume,
        sum(quote_volume) AS quote_volume,
        sum(trade_count) AS trade_count,
        sum(buy_count) AS buy_count,
        sum(sell_count) AS sell_count
    FROM trades_1m
    GROUP BY time_bucket(INTERVAL '15 minutes', timestamp), symbol
    WITH NO DATA;
EXCEPTION WHEN duplicate_table THEN
    RAISE NOTICE 'trades_15m already exists';
END $$;

-- 1-hour aggregate
DO $$
BEGIN
    CREATE MATERIALIZED VIEW trades_1h
    WITH (timescaledb.continuous) AS
    SELECT
        time_bucket(INTERVAL '1 hour', timestamp) AS timestamp,
        symbol,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume,
        sum(quote_volume) AS quote_volume,
        sum(trade_count) AS trade_count,
        sum(buy_count) AS buy_count,
        sum(sell_count) AS sell_count
    FROM trades_1m
    GROUP BY time_bucket(INTERVAL '1 hour', timestamp), symbol
    WITH NO DATA;
EXCEPTION WHEN duplicate_table THEN
    RAISE NOTICE 'trades_1h already exists';
END $$;

-- =============================================================================
-- 11. REFRESH POLICIES FOR CONTINUOUS AGGREGATES
-- =============================================================================

SELECT add_continuous_aggregate_policy('trades_5m',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('trades_15m',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('trades_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);
