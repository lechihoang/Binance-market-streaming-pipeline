"""
Tests for Storage Health Check Functions.

Tests the health check functions for 3-tier storage architecture:
- Redis (Hot Path)
- PostgreSQL (Warm Path)
- MinIO (Cold Path)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.storage.health_checks import (
    check_redis_health,
    check_postgres_health,
    check_minio_health,
    check_all_storage_health,
)


class TestRedisHealthCheck:
    """Tests for Redis health check (Hot Path)."""
    
    @patch('src.storage.health_checks.redis')
    def test_redis_health_check_success(self, mock_redis):
        """Test successful Redis health check."""
        # Setup mock
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis.Redis.return_value = mock_client
        
        # Execute
        result = check_redis_health(host='localhost', port=6379)
        
        # Verify
        assert result['service'] == 'redis'
        assert result['tier'] == 'hot'
        assert result['status'] == 'healthy'
        assert result['host'] == 'localhost'
        assert result['port'] == 6379
        assert result['attempt'] == 1
        assert 'timestamp' in result
        
        mock_client.ping.assert_called_once()
        mock_client.close.assert_called_once()
    
    @patch('src.storage.health_checks.redis')
    def test_redis_health_check_retry_on_failure(self, mock_redis):
        """Test Redis health check retries on connection failure."""
        from redis.exceptions import ConnectionError
        
        # Setup mock to fail twice then succeed
        mock_client = Mock()
        mock_client.ping.side_effect = [
            ConnectionError("Connection refused"),
            ConnectionError("Connection refused"),
            True
        ]
        mock_redis.Redis.return_value = mock_client
        mock_redis.exceptions.ConnectionError = ConnectionError
        mock_redis.exceptions.TimeoutError = TimeoutError
        
        # Execute with short retry delay
        result = check_redis_health(
            host='localhost', 
            port=6379, 
            max_retries=3,
            retry_delay=0.01
        )
        
        # Verify
        assert result['status'] == 'healthy'
        assert result['attempt'] == 3
    
    @patch('src.storage.health_checks.redis')
    def test_redis_health_check_fails_after_max_retries(self, mock_redis):
        """Test Redis health check fails after max retries."""
        from redis.exceptions import ConnectionError
        
        # Setup mock to always fail
        mock_client = Mock()
        mock_client.ping.side_effect = ConnectionError("Connection refused")
        mock_redis.Redis.return_value = mock_client
        mock_redis.exceptions.ConnectionError = ConnectionError
        mock_redis.exceptions.TimeoutError = TimeoutError
        
        # Execute and expect failure
        with pytest.raises(Exception) as exc_info:
            check_redis_health(
                host='localhost', 
                port=6379, 
                max_retries=2,
                retry_delay=0.01
            )
        
        assert "Redis health check failed after 2 attempts" in str(exc_info.value)


class TestPostgresHealthCheck:
    """Tests for PostgreSQL health check (Warm Path)."""
    
    @patch('src.storage.health_checks.psycopg2')
    def test_postgres_health_check_success(self, mock_psycopg2):
        """Test successful PostgreSQL health check."""
        # Setup mock
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn
        
        # Execute
        result = check_postgres_health(
            host='localhost',
            port=5432,
            user='crypto',
            password='crypto',
            database='crypto_data'
        )
        
        # Verify
        assert result['service'] == 'postgresql'
        assert result['tier'] == 'warm'
        assert result['status'] == 'healthy'
        assert result['host'] == 'localhost'
        assert result['port'] == 5432
        assert result['database'] == 'crypto_data'
        assert result['attempt'] == 1
        assert 'timestamp' in result
        
        mock_conn.close.assert_called_once()
    
    @patch('src.storage.health_checks.psycopg2')
    def test_postgres_health_check_retry_on_failure(self, mock_psycopg2):
        """Test PostgreSQL health check retries on connection failure."""
        # Setup mock to fail once then succeed
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        
        mock_psycopg2.connect.side_effect = [
            Exception("Connection refused"),
            mock_conn
        ]
        
        # Execute with short retry delay
        result = check_postgres_health(
            host='localhost',
            port=5432,
            max_retries=3,
            retry_delay=0.01
        )
        
        # Verify
        assert result['status'] == 'healthy'
        assert result['attempt'] == 2
    
    @patch('src.storage.health_checks.psycopg2')
    def test_postgres_health_check_fails_after_max_retries(self, mock_psycopg2):
        """Test PostgreSQL health check fails after max retries."""
        # Setup mock to always fail
        mock_psycopg2.connect.side_effect = Exception("Connection refused")
        
        # Execute and expect failure
        with pytest.raises(Exception) as exc_info:
            check_postgres_health(
                host='localhost',
                port=5432,
                max_retries=2,
                retry_delay=0.01
            )
        
        assert "PostgreSQL health check failed after 2 attempts" in str(exc_info.value)


class TestMinioHealthCheck:
    """Tests for MinIO health check (Cold Path)."""
    
    @patch('src.storage.health_checks.Minio')
    def test_minio_health_check_success_bucket_exists(self, mock_minio_class):
        """Test successful MinIO health check when bucket exists."""
        # Setup mock
        mock_client = Mock()
        mock_client.list_buckets.return_value = []
        mock_client.bucket_exists.return_value = True
        mock_minio_class.return_value = mock_client
        
        # Execute
        result = check_minio_health(
            endpoint='localhost:9000',
            access_key='minioadmin',
            secret_key='minioadmin',
            bucket='crypto-data'
        )
        
        # Verify
        assert result['service'] == 'minio'
        assert result['tier'] == 'cold'
        assert result['status'] == 'healthy'
        assert result['endpoint'] == 'localhost:9000'
        assert result['bucket'] == 'crypto-data'
        assert result['attempt'] == 1
        assert 'timestamp' in result
        
        mock_client.list_buckets.assert_called_once()
        mock_client.bucket_exists.assert_called_once_with('crypto-data')
        mock_client.make_bucket.assert_not_called()
    
    @patch('src.storage.health_checks.Minio')
    def test_minio_health_check_creates_bucket_if_not_exists(self, mock_minio_class):
        """Test MinIO health check creates bucket if it doesn't exist."""
        # Setup mock
        mock_client = Mock()
        mock_client.list_buckets.return_value = []
        mock_client.bucket_exists.return_value = False
        mock_minio_class.return_value = mock_client
        
        # Execute
        result = check_minio_health(
            endpoint='localhost:9000',
            bucket='new-bucket'
        )
        
        # Verify
        assert result['status'] == 'healthy'
        mock_client.make_bucket.assert_called_once_with('new-bucket')
    
    @patch('src.storage.health_checks.Minio')
    def test_minio_health_check_retry_on_failure(self, mock_minio_class):
        """Test MinIO health check retries on connection failure."""
        # Setup mock to fail once then succeed
        mock_client = Mock()
        mock_client.list_buckets.side_effect = [
            Exception("Connection refused"),
            []
        ]
        mock_client.bucket_exists.return_value = True
        mock_minio_class.return_value = mock_client
        
        # Execute with short retry delay
        result = check_minio_health(
            endpoint='localhost:9000',
            max_retries=3,
            retry_delay=0.01
        )
        
        # Verify
        assert result['status'] == 'healthy'
        assert result['attempt'] == 2
    
    @patch('src.storage.health_checks.Minio')
    def test_minio_health_check_fails_after_max_retries(self, mock_minio_class):
        """Test MinIO health check fails after max retries."""
        # Setup mock to always fail
        mock_client = Mock()
        mock_client.list_buckets.side_effect = Exception("Connection refused")
        mock_minio_class.return_value = mock_client
        
        # Execute and expect failure
        with pytest.raises(Exception) as exc_info:
            check_minio_health(
                endpoint='localhost:9000',
                max_retries=2,
                retry_delay=0.01
            )
        
        assert "MinIO health check failed after 2 attempts" in str(exc_info.value)


class TestCheckAllStorageHealth:
    """Tests for combined storage health check."""
    
    @patch('src.storage.health_checks.check_minio_health')
    @patch('src.storage.health_checks.check_postgres_health')
    @patch('src.storage.health_checks.check_redis_health')
    def test_check_all_storage_health_success(
        self, 
        mock_redis, 
        mock_postgres, 
        mock_minio
    ):
        """Test successful health check of all storage tiers."""
        # Setup mocks
        mock_redis.return_value = {'service': 'redis', 'status': 'healthy'}
        mock_postgres.return_value = {'service': 'postgresql', 'status': 'healthy'}
        mock_minio.return_value = {'service': 'minio', 'status': 'healthy'}
        
        # Execute
        result = check_all_storage_health()
        
        # Verify
        assert 'redis' in result
        assert 'postgresql' in result
        assert 'minio' in result
        assert result['redis']['status'] == 'healthy'
        assert result['postgresql']['status'] == 'healthy'
        assert result['minio']['status'] == 'healthy'
    
    @patch('src.storage.health_checks.check_minio_health')
    @patch('src.storage.health_checks.check_postgres_health')
    @patch('src.storage.health_checks.check_redis_health')
    def test_check_all_storage_health_with_custom_config(
        self, 
        mock_redis, 
        mock_postgres, 
        mock_minio
    ):
        """Test health check with custom configuration."""
        # Setup mocks
        mock_redis.return_value = {'status': 'healthy'}
        mock_postgres.return_value = {'status': 'healthy'}
        mock_minio.return_value = {'status': 'healthy'}
        
        # Execute with custom config
        result = check_all_storage_health(
            redis_config={'host': 'custom-redis', 'port': 6380},
            postgres_config={'host': 'custom-postgres', 'database': 'custom_db'},
            minio_config={'endpoint': 'custom-minio:9000', 'bucket': 'custom-bucket'}
        )
        
        # Verify configs were passed
        mock_redis.assert_called_once_with(host='custom-redis', port=6380)
        mock_postgres.assert_called_once_with(host='custom-postgres', database='custom_db')
        mock_minio.assert_called_once_with(endpoint='custom-minio:9000', bucket='custom-bucket')
    
    @patch('src.storage.health_checks.check_redis_health')
    def test_check_all_storage_health_fails_on_first_error(self, mock_redis):
        """Test that check_all_storage_health fails fast on first error."""
        # Setup mock to fail
        mock_redis.side_effect = Exception("Redis connection failed")
        
        # Execute and expect failure
        with pytest.raises(Exception) as exc_info:
            check_all_storage_health()
        
        assert "Redis connection failed" in str(exc_info.value)
