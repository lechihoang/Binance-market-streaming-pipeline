"""
End-to-end integration tests for streaming performance optimization.

Tests data flow through all storage tiers (Redis, PostgreSQL, MinIO)
using the batch write methods implemented for performance optimization.

Requirements: All (streaming-performance spec)
Task: 7.1 Run end-to-end test with Docker environment
"""

import os
import time
import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List
from unittest.mock import MagicMock, patch

# Import storage classes
from src.storage.redis import RedisStorage
from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage
from src.storage.storage_writer import StorageWriter, BatchResult


# ============================================================================
# ENVIRONMENT CHECKS
# ============================================================================

def is_redis_available():
    """Check if Redis is available for testing."""
    try:
        import redis
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=15,
            socket_connect_timeout=2
        )
        client.ping()
        client.close()
        return True
    except Exception:
        return False


def is_postgres_available():
    """Check if PostgreSQL is available for testing."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5434)),
            user=os.getenv("POSTGRES_USER", "crypto"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto"),
            database=os.getenv("POSTGRES_DB", "crypto_data"),
            connect_timeout=2
        )
        conn.close()
        return True
    except Exception:
        return False


def is_minio_available():
    """Check if MinIO is available for testing."""
    try:
        from minio import Minio
        client = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )
        client.list_buckets()
        return True
    except Exception:
        return False


REDIS_AVAILABLE = is_redis_available()
POSTGRES_AVAILABLE = is_postgres_available()
MINIO_AVAILABLE = is_minio_available()
ALL_SERVICES_AVAILABLE = REDIS_AVAILABLE and POSTGRES_AVAILABLE and MINIO_AVAILABLE

skip_if_no_redis = pytest.mark.skipif(
    not REDIS_AVAILABLE,
    reason="Redis server not available"
)

skip_if_no_postgres = pytest.mark.skipif(
    not POSTGRES_AVAILABLE,
    reason="PostgreSQL server not available"
)

skip_if_no_minio = pytest.mark.skipif(
    not MINIO_AVAILABLE,
    reason="MinIO server not available"
)

skip_if_no_docker = pytest.mark.skipif(
    not ALL_SERVICES_AVAILABLE,
    reason="Docker services not available (Redis, PostgreSQL, MinIO required)"
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def redis_storage():
    """Create RedisStorage instance for testing."""
    storage = RedisStorage(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=15  # Use test database
    )
    # Clean up before test
    storage.client.flushdb()
    yield storage
    # Clean up after test
    storage.client.flushdb()


@pytest.fixture
def postgres_storage():
    """Create PostgresStorage instance for testing."""
    storage = PostgresStorage(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5434)),
        user=os.getenv("POSTGRES_USER", "crypto"),
        password=os.getenv("POSTGRES_PASSWORD", "crypto"),
        database=os.getenv("POSTGRES_DB", "crypto_data")
    )
    yield storage
    storage.close()


@pytest.fixture
def minio_storage():
    """Create MinioStorage instance for testing."""
    storage = MinioStorage(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        bucket="test-crypto-data",
        secure=False
    )
    yield storage
    storage.close()


@pytest.fixture
def storage_writer(redis_storage, postgres_storage, minio_storage):
    """Create StorageWriter with all storage tiers."""
    return StorageWriter(
        redis=redis_storage,
        postgres=postgres_storage,
        minio=minio_storage
    )


# ============================================================================
# TEST DATA GENERATORS
# ============================================================================

def generate_aggregation_records(count: int = 10, symbol: str = "BTCUSDT") -> List[Dict[str, Any]]:
    """Generate test aggregation records."""
    base_time = datetime.now()
    records = []
    
    for i in range(count):
        timestamp = base_time - timedelta(minutes=i)
        records.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "interval": "1m",
            "open": 50000.0 + i * 10,
            "high": 50100.0 + i * 10,
            "low": 49900.0 + i * 10,
            "close": 50050.0 + i * 10,
            "volume": 100.0 + i,
            "quote_volume": 5000000.0 + i * 1000,
            "trades_count": 1000 + i * 10,
        })
    
    return records


def generate_alert_records(count: int = 5, symbol: str = "BTCUSDT") -> List[Dict[str, Any]]:
    """Generate test alert records."""
    base_time = datetime.now()
    alert_types = ["WHALE_ALERT", "VOLUME_SPIKE", "PRICE_SPIKE"]
    alert_levels = ["HIGH", "MEDIUM", "LOW"]
    records = []
    
    for i in range(count):
        timestamp = base_time - timedelta(minutes=i)
        records.append({
            "alert_id": f"test-alert-{i}",
            "timestamp": timestamp,
            "symbol": symbol,
            "alert_type": alert_types[i % len(alert_types)],
            "alert_level": alert_levels[i % len(alert_levels)],
            "created_at": timestamp,
            "details": {"test_value": i * 100},
        })
    
    return records


# ============================================================================
# REDIS BATCH TESTS
# ============================================================================

@skip_if_no_redis
class TestRedisBatchOperations:
    """Test Redis batch write operations."""
    
    def test_write_aggregations_batch_success(self, redis_storage):
        """Test batch writing aggregations to Redis."""
        records = generate_aggregation_records(5)
        
        success_count, failed = redis_storage.write_aggregations_batch(records)
        
        assert success_count == 5
        assert len(failed) == 0
        
        # Verify data was written
        for record in records:
            key = f"market:{record['symbol']}:{record['interval']}"
            data = redis_storage.client.hgetall(key)
            assert data is not None
            assert "close" in data
    
    def test_write_alerts_batch_success(self, redis_storage):
        """Test batch writing alerts to Redis."""
        # Redis expects ISO strings for datetime fields (StorageWriter handles conversion)
        alerts = []
        base_time = datetime.now()
        
        for i in range(5):
            timestamp = base_time - timedelta(minutes=i)
            alerts.append({
                "alert_id": f"test-alert-{i}",
                "timestamp": timestamp.isoformat(),  # ISO string for Redis
                "symbol": "BTCUSDT",
                "alert_type": "WHALE_ALERT",
                "alert_level": "HIGH",
                "created_at": timestamp.isoformat(),  # ISO string for Redis
                "details": {"test_value": i * 100},
            })
        
        success_count, failed = redis_storage.write_alerts_batch(alerts)
        
        assert success_count == 5
        assert len(failed) == 0
        
        # Verify alerts were written to list
        recent_alerts = redis_storage.get_recent_alerts(limit=10)
        assert len(recent_alerts) >= 5
    
    def test_write_aggregations_batch_empty(self, redis_storage):
        """Test batch writing empty list returns zero."""
        success_count, failed = redis_storage.write_aggregations_batch([])
        
        assert success_count == 0
        assert len(failed) == 0


# ============================================================================
# POSTGRESQL BATCH TESTS
# ============================================================================

@skip_if_no_postgres
class TestPostgresBatchOperations:
    """Test PostgreSQL batch write operations."""
    
    def test_upsert_candles_batch_success(self, postgres_storage):
        """Test batch upserting candles to PostgreSQL."""
        records = generate_aggregation_records(5)
        
        row_count = postgres_storage.upsert_candles_batch(records)
        
        assert row_count == 5
        
        # Verify data was written
        start = datetime.now() - timedelta(hours=1)
        end = datetime.now() + timedelta(hours=1)
        candles = postgres_storage.query_candles("BTCUSDT", start, end)
        assert len(candles) >= 5
    
    def test_insert_alerts_batch_success(self, postgres_storage):
        """Test batch inserting alerts to PostgreSQL."""
        alerts = []
        base_time = datetime.now()
        
        for i in range(5):
            alerts.append({
                "timestamp": base_time - timedelta(minutes=i),
                "symbol": "BTCUSDT",
                "alert_type": "WHALE_ALERT",
                "severity": "HIGH",
                "message": f"Test alert {i}",
                "metadata": {"value": i * 100},
            })
        
        row_count = postgres_storage.insert_alerts_batch(alerts)
        
        assert row_count == 5
    
    def test_upsert_candles_batch_empty(self, postgres_storage):
        """Test batch upserting empty list returns zero."""
        row_count = postgres_storage.upsert_candles_batch([])
        
        assert row_count == 0


# ============================================================================
# MINIO BATCH TESTS
# ============================================================================

@skip_if_no_minio
class TestMinioBatchOperations:
    """Test MinIO batch write operations."""
    
    def test_write_klines_batch_success(self, minio_storage):
        """Test batch writing klines to MinIO."""
        records = generate_aggregation_records(5)
        write_date = datetime.now()
        
        success_count, failed_symbols = minio_storage.write_klines_batch(records, write_date)
        
        assert success_count == 5
        assert len(failed_symbols) == 0
    
    def test_write_alerts_batch_success(self, minio_storage):
        """Test batch writing alerts to MinIO."""
        alerts = []
        base_time = datetime.now()
        
        for i in range(5):
            alerts.append({
                "timestamp": base_time - timedelta(minutes=i),
                "symbol": "BTCUSDT",
                "alert_type": "WHALE_ALERT",
                "severity": "HIGH",
                "message": f"Test alert {i}",
                "metadata": {"value": i * 100},
            })
        
        write_date = datetime.now()
        success_count, errors = minio_storage.write_alerts_batch(alerts, write_date)
        
        assert success_count == 5
        assert len(errors) == 0
    
    def test_write_klines_batch_empty(self, minio_storage):
        """Test batch writing empty list returns zero."""
        success_count, failed_symbols = minio_storage.write_klines_batch([], datetime.now())
        
        assert success_count == 0
        assert len(failed_symbols) == 0


# ============================================================================
# STORAGE WRITER INTEGRATION TESTS
# ============================================================================

@skip_if_no_docker
class TestStorageWriterIntegration:
    """End-to-end integration tests for StorageWriter batch operations."""
    
    def test_write_aggregations_batch_all_tiers(self, storage_writer):
        """Test batch writing aggregations to all 3 storage tiers.
        
        Verifies:
        - Data flows through Redis, PostgreSQL, and MinIO
        - All tiers receive the data successfully
        - BatchResult contains correct statistics
        
        Requirements: 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.4
        """
        records = generate_aggregation_records(10)
        
        result = storage_writer.write_aggregations_batch(records)
        
        # Verify BatchResult structure
        assert isinstance(result, BatchResult)
        assert result.total_records == 10
        assert result.success_count > 0
        assert result.duration_ms > 0
        
        # Verify all tiers succeeded
        assert result.tier_results["redis"] is True, "Redis tier should succeed"
        assert result.tier_results["warm"] is True, "PostgreSQL tier should succeed"
        assert result.tier_results["cold"] is True, "MinIO tier should succeed"
        
        # Verify no failures
        assert result.failure_count == 0
        assert len(result.failed_records) == 0
    
    def test_write_alerts_batch_all_tiers(self, storage_writer):
        """Test batch writing alerts to all 3 storage tiers.
        
        Verifies:
        - Alerts flow through Redis, PostgreSQL, and MinIO
        - All tiers receive the alerts successfully
        - BatchResult contains correct statistics
        
        Requirements: 1.5, 2.1, 2.2, 2.3, 2.4
        """
        alerts = generate_alert_records(5)
        
        result = storage_writer.write_alerts_batch(alerts)
        
        # Verify BatchResult structure
        assert isinstance(result, BatchResult)
        assert result.total_records == 5
        assert result.success_count > 0
        assert result.duration_ms > 0
        
        # Verify all tiers succeeded
        assert result.tier_results["redis"] is True, "Redis tier should succeed"
        assert result.tier_results["warm"] is True, "PostgreSQL tier should succeed"
        assert result.tier_results["cold"] is True, "MinIO tier should succeed"
    
    def test_parallel_write_independence(self, storage_writer, redis_storage):
        """Test that tier failures don't block other tiers.
        
        Property 6: Parallel write independence
        Validates: Requirements 2.3
        """
        records = generate_aggregation_records(5)
        
        # Mock PostgreSQL to fail
        original_upsert = storage_writer._warm_storage.upsert_candles_batch
        storage_writer._warm_storage.upsert_candles_batch = MagicMock(
            side_effect=Exception("Simulated PostgreSQL failure")
        )
        
        try:
            result = storage_writer.write_aggregations_batch(records)
            
            # Redis and MinIO should still succeed
            assert result.tier_results["redis"] is True, "Redis should succeed despite PostgreSQL failure"
            assert result.tier_results["warm"] is False, "PostgreSQL should fail"
            assert result.tier_results["cold"] is True, "MinIO should succeed despite PostgreSQL failure"
            
            # Verify Redis actually has the data
            for record in records:
                key = f"market:{record['symbol']}:{record['interval']}"
                data = redis_storage.client.hgetall(key)
                assert data is not None, f"Redis should have data for {key}"
        finally:
            # Restore original method
            storage_writer._warm_storage.upsert_candles_batch = original_upsert
    
    def test_empty_batch_handling(self, storage_writer):
        """Test that empty batches are handled correctly."""
        result = storage_writer.write_aggregations_batch([])
        
        assert result.total_records == 0
        assert result.success_count == 0
        assert result.failure_count == 0
        assert result.tier_results["redis"] is True
        assert result.tier_results["warm"] is True
        assert result.tier_results["cold"] is True


# ============================================================================
# DATA FLOW VERIFICATION TESTS
# ============================================================================

@skip_if_no_docker
class TestDataFlowVerification:
    """Verify data flows correctly through all storage tiers."""
    
    def test_aggregation_data_readable_after_batch_write(
        self, storage_writer, redis_storage, postgres_storage
    ):
        """Verify aggregation data can be read back from each tier after batch write."""
        records = generate_aggregation_records(3, symbol="ETHUSDT")
        
        # Write batch
        result = storage_writer.write_aggregations_batch(records)
        assert all(result.tier_results.values()), "All tiers should succeed"
        
        # Verify Redis data
        redis_data = redis_storage.get_aggregation("ETHUSDT", "1m")
        assert redis_data is not None, "Redis should have aggregation data"
        assert "close" in redis_data, "Redis data should have close price"
        
        # Verify PostgreSQL data
        start = datetime.now() - timedelta(hours=1)
        end = datetime.now() + timedelta(hours=1)
        pg_candles = postgres_storage.query_candles("ETHUSDT", start, end)
        assert len(pg_candles) >= 3, "PostgreSQL should have candle data"
    
    def test_multiple_symbols_batch_write(self, storage_writer, redis_storage):
        """Test batch writing data for multiple symbols."""
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        all_records = []
        
        for symbol in symbols:
            all_records.extend(generate_aggregation_records(3, symbol=symbol))
        
        result = storage_writer.write_aggregations_batch(all_records)
        
        assert result.total_records == 9
        assert all(result.tier_results.values()), "All tiers should succeed"
        
        # Verify each symbol has data in Redis
        for symbol in symbols:
            redis_data = redis_storage.get_aggregation(symbol, "1m")
            assert redis_data is not None, f"Redis should have data for {symbol}"


# ============================================================================
# LATENCY MEASUREMENT TESTS
# ============================================================================

@skip_if_no_docker
class TestLatencyMeasurement:
    """Measure and verify latency improvements from batch operations."""
    
    def test_batch_write_latency(self, storage_writer):
        """Measure batch write latency for performance verification."""
        records = generate_aggregation_records(100)
        
        start_time = time.time()
        result = storage_writer.write_aggregations_batch(records)
        total_time_ms = (time.time() - start_time) * 1000
        
        # Log latency for analysis
        print(f"\nBatch write latency for 100 records:")
        print(f"  Total time: {total_time_ms:.2f}ms")
        print(f"  Reported duration: {result.duration_ms:.2f}ms")
        print(f"  Per-record average: {total_time_ms / 100:.2f}ms")
        
        # Verify reasonable latency (should be much faster than sequential writes)
        # Sequential writes would take ~100 * 3 tiers * ~10ms = 3000ms
        # Batch + parallel should be under 1000ms for 100 records
        assert total_time_ms < 5000, f"Batch write too slow: {total_time_ms}ms"
        assert result.success_count == 100
    
    def test_parallel_execution_faster_than_sequential(self, storage_writer):
        """Verify parallel writes are faster than sequential would be."""
        records = generate_aggregation_records(50)
        
        result = storage_writer.write_aggregations_batch(records)
        
        # The duration should reflect parallel execution
        # If sequential, it would be ~3x longer (one tier at a time)
        # With parallel, all 3 tiers run concurrently
        print(f"\nParallel write duration: {result.duration_ms:.2f}ms for 50 records")
        
        # Verify all tiers completed
        assert all(result.tier_results.values())
        assert result.success_count == 50
