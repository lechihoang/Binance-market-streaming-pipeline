"""
Test module for Utils Module.

Contains unit tests and property-based tests for:
- Retry utilities (ExponentialBackoff, retry_operation, @with_retry)
- Configuration utilities (get_env_* functions)
- Logging utilities (setup_logging, get_logger)
- Metrics utilities (record_*, track_latency)
- Graceful shutdown utilities (GracefulShutdown, ShutdownState)

Requirements Coverage:
- Requirement 1: Retry Utilities
- Requirement 2: Configuration Utilities
- Requirement 4: Logging Utilities
- Requirement 5: Prometheus Metrics Utilities
- Requirement 6: Graceful Shutdown Utilities
"""

import asyncio
import logging
import os
import time
from unittest.mock import Mock, patch

import pytest
from hypothesis import given, settings, strategies as st, assume

# Retry utilities
from src.utils.retry import (
    ExponentialBackoff,
    RetryConfig,
    retry_operation,
    retry_operation_async,
    with_retry,
)
# Config utilities
from src.utils.config import (
    get_env_str,
    get_env_int,
    get_env_float,
    get_env_bool,
    get_env_list,
    get_env_optional,
)
# Logging utilities
from src.utils.logging import setup_logging, get_logger
# Metrics utilities
from src.utils.metrics import (
    record_request,
    record_error,
    record_latency,
    track_latency,
    track_latency_decorator,
)
# Shutdown utilities
from src.utils.shutdown import GracefulShutdown, ShutdownState


# ============================================================================
# EXPONENTIAL BACKOFF TESTS
# ============================================================================

class TestExponentialBackoff:
    """Tests for ExponentialBackoff class."""
    
    def test_initial_delay(self):
        """Test that first delay is approximately initial_delay_ms."""
        backoff = ExponentialBackoff(
            initial_delay_ms=1000,
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.0  # No jitter for predictable test
        )
        delay = backoff.next_delay_ms()
        assert delay == 1000
    
    def test_exponential_growth(self):
        """Test that delays grow exponentially."""
        backoff = ExponentialBackoff(
            initial_delay_ms=1000,
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.0
        )
        delay1 = backoff.next_delay_ms()  # 1000
        delay2 = backoff.next_delay_ms()  # 2000
        delay3 = backoff.next_delay_ms()  # 4000
        
        assert delay1 == 1000
        assert delay2 == 2000
        assert delay3 == 4000
    
    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay_ms."""
        backoff = ExponentialBackoff(
            initial_delay_ms=1000,
            max_delay_ms=5000,
            multiplier=2.0,
            jitter_factor=0.0
        )
        # 1000, 2000, 4000, 5000 (capped), 5000 (capped)
        for _ in range(10):
            delay = backoff.next_delay_ms()
        assert delay == 5000
    
    def test_jitter_adds_randomness(self):
        """Test that jitter adds randomness to delays."""
        backoff = ExponentialBackoff(
            initial_delay_ms=1000,
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.1
        )
        delay = backoff.next_delay_ms()
        # With 10% jitter, delay should be between 1000 and 1100
        assert 1000 <= delay <= 1100
    
    def test_reset(self):
        """Test that reset() resets the attempt counter."""
        backoff = ExponentialBackoff(
            initial_delay_ms=1000,
            jitter_factor=0.0
        )
        backoff.next_delay_ms()  # 1000
        backoff.next_delay_ms()  # 2000
        assert backoff.attempt_count == 2
        
        backoff.reset()
        assert backoff.attempt_count == 0
        assert backoff.next_delay_ms() == 1000
    
    def test_invalid_initial_delay(self):
        """Test that invalid initial_delay_ms raises ValueError."""
        with pytest.raises(ValueError):
            ExponentialBackoff(initial_delay_ms=0)
        with pytest.raises(ValueError):
            ExponentialBackoff(initial_delay_ms=-100)
    
    def test_invalid_multiplier(self):
        """Test that multiplier < 1.0 raises ValueError."""
        with pytest.raises(ValueError):
            ExponentialBackoff(multiplier=0.5)
    
    def test_invalid_jitter_factor(self):
        """Test that jitter_factor outside [0, 1] raises ValueError."""
        with pytest.raises(ValueError):
            ExponentialBackoff(jitter_factor=-0.1)
        with pytest.raises(ValueError):
            ExponentialBackoff(jitter_factor=1.5)


# ============================================================================
# RETRY OPERATION TESTS
# ============================================================================

class TestRetryOperation:
    """Tests for retry_operation function."""
    
    def test_success_on_first_attempt(self):
        """Test that successful operation returns immediately."""
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = retry_operation(operation)
        assert result == "success"
        assert call_count == 1
    
    def test_success_after_failures(self):
        """Test that operation succeeds after transient failures."""
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection failed")
            return "success"
        
        config = RetryConfig(
            max_retries=5,
            initial_delay_ms=1,  # Fast for testing
            jitter_factor=0.0
        )
        result = retry_operation(operation, config=config)
        assert result == "success"
        assert call_count == 3
    
    def test_exhaustion_raises_last_exception(self):
        """Test that exhausted retries raise the last exception."""
        def operation():
            raise ValueError("Always fails")
        
        config = RetryConfig(
            max_retries=3,
            initial_delay_ms=1,
            jitter_factor=0.0
        )
        
        with pytest.raises(ValueError) as exc_info:
            retry_operation(operation, config=config)
        assert "Always fails" in str(exc_info.value)
    
    def test_on_retry_callback(self):
        """Test that on_retry callback is called on each retry."""
        retry_calls = []
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("Retry me")
            return "done"
        
        def on_retry(attempt, delay_ms, exception):
            retry_calls.append((attempt, delay_ms, str(exception)))
        
        config = RetryConfig(max_retries=5, initial_delay_ms=1, jitter_factor=0.0)
        retry_operation(operation, config=config, on_retry=on_retry)
        
        assert len(retry_calls) == 2
        assert retry_calls[0][0] == 1  # First retry attempt
        assert retry_calls[1][0] == 2  # Second retry attempt
    
    def test_retryable_exceptions_filter(self):
        """Test that only specified exceptions are retried."""
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Not retryable")
            return "success"
        
        config = RetryConfig(
            max_retries=3,
            retryable_exceptions=(ConnectionError,)  # Only retry ConnectionError
        )
        
        with pytest.raises(ValueError):
            retry_operation(operation, config=config)
        assert call_count == 1  # Should not retry


# ============================================================================
# WITH_RETRY DECORATOR TESTS
# ============================================================================

class TestWithRetryDecorator:
    """Tests for @with_retry decorator."""
    
    def test_decorator_success(self):
        """Test that decorated function works normally on success."""
        @with_retry()
        def my_function(x, y):
            return x + y
        
        result = my_function(2, 3)
        assert result == 5
    
    def test_decorator_retries_on_failure(self):
        """Test that decorated function retries on failure."""
        call_count = 0
        
        @with_retry(config=RetryConfig(max_retries=3, initial_delay_ms=1, jitter_factor=0.0))
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("Flaky")
            return "success"
        
        result = flaky_function()
        assert result == "success"
        assert call_count == 2


# ============================================================================
# ASYNC RETRY TESTS
# ============================================================================

class TestRetryOperationAsync:
    """Tests for retry_operation_async function."""
    
    @pytest.mark.asyncio
    async def test_async_success(self):
        """Test async operation succeeds."""
        async def async_op():
            return "async_success"
        
        result = await retry_operation_async(async_op)
        assert result == "async_success"
    
    @pytest.mark.asyncio
    async def test_async_retry_on_failure(self):
        """Test async operation retries on failure."""
        call_count = 0
        
        async def flaky_async_op():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Async connection failed")
            return "recovered"
        
        config = RetryConfig(max_retries=3, initial_delay_ms=1, jitter_factor=0.0)
        result = await retry_operation_async(flaky_async_op, config=config)
        assert result == "recovered"
        assert call_count == 2


# ============================================================================
# CONFIGURATION UTILITIES TESTS
# ============================================================================

class TestConfigUtilities:
    """Tests for configuration utilities."""
    
    def test_get_env_str_with_value(self):
        """Test get_env_str returns environment variable value."""
        with patch.dict(os.environ, {"TEST_STR": "hello"}):
            result = get_env_str("TEST_STR", "default")
            assert result == "hello"
    
    def test_get_env_str_with_default(self):
        """Test get_env_str returns default when not set."""
        result = get_env_str("NONEXISTENT_VAR", "default_value")
        assert result == "default_value"
    
    def test_get_env_int_with_value(self):
        """Test get_env_int parses integer correctly."""
        with patch.dict(os.environ, {"TEST_INT": "42"}):
            result = get_env_int("TEST_INT", 0)
            assert result == 42
    
    def test_get_env_int_with_default(self):
        """Test get_env_int returns default when not set."""
        result = get_env_int("NONEXISTENT_INT", 100)
        assert result == 100
    
    def test_get_env_float_with_value(self):
        """Test get_env_float parses float correctly."""
        with patch.dict(os.environ, {"TEST_FLOAT": "3.14"}):
            result = get_env_float("TEST_FLOAT", 0.0)
            assert abs(result - 3.14) < 0.001
    
    def test_get_env_float_with_default(self):
        """Test get_env_float returns default when not set."""
        result = get_env_float("NONEXISTENT_FLOAT", 2.71)
        assert abs(result - 2.71) < 0.001
    
    def test_get_env_bool_true_values(self):
        """Test get_env_bool recognizes true values."""
        for true_val in ["true", "TRUE", "True", "1", "yes", "YES"]:
            with patch.dict(os.environ, {"TEST_BOOL": true_val}):
                result = get_env_bool("TEST_BOOL", False)
                assert result is True, f"Failed for value: {true_val}"
    
    def test_get_env_bool_false_values(self):
        """Test get_env_bool recognizes false values."""
        for false_val in ["false", "FALSE", "False", "0", "no", "NO"]:
            with patch.dict(os.environ, {"TEST_BOOL": false_val}):
                result = get_env_bool("TEST_BOOL", True)
                assert result is False, f"Failed for value: {false_val}"
    
    def test_get_env_bool_with_default(self):
        """Test get_env_bool returns default when not set."""
        result = get_env_bool("NONEXISTENT_BOOL", True)
        assert result is True
    
    def test_get_env_list_with_value(self):
        """Test get_env_list parses comma-separated list."""
        with patch.dict(os.environ, {"TEST_LIST": "a, b, c, d"}):
            result = get_env_list("TEST_LIST", [])
            assert result == ["a", "b", "c", "d"]
    
    def test_get_env_list_strips_whitespace(self):
        """Test get_env_list strips whitespace from items."""
        with patch.dict(os.environ, {"TEST_LIST": "  item1  ,  item2  ,  item3  "}):
            result = get_env_list("TEST_LIST", [])
            assert result == ["item1", "item2", "item3"]
    
    def test_get_env_list_with_default(self):
        """Test get_env_list returns default when not set."""
        result = get_env_list("NONEXISTENT_LIST", ["default"])
        assert result == ["default"]
    
    def test_get_env_list_empty_string(self):
        """Test get_env_list returns default for empty string."""
        with patch.dict(os.environ, {"TEST_LIST": ""}):
            result = get_env_list("TEST_LIST", ["default"])
            assert result == ["default"]
    
    def test_get_env_optional_with_value(self):
        """Test get_env_optional returns value when set."""
        with patch.dict(os.environ, {"TEST_OPT": "value"}):
            result = get_env_optional("TEST_OPT")
            assert result == "value"
    
    def test_get_env_optional_returns_none(self):
        """Test get_env_optional returns None when not set."""
        result = get_env_optional("NONEXISTENT_OPT")
        assert result is None


# ============================================================================
# LOGGING UTILITIES TESTS
# ============================================================================

class TestLoggingUtilities:
    """Tests for logging utilities."""
    
    def test_setup_logging_sets_level(self):
        """Test that setup_logging sets the correct log level."""
        setup_logging(level="DEBUG")
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG
    
    def test_get_logger_returns_logger(self):
        """Test that get_logger returns a Logger instance."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"
    
    def test_setup_logging_case_insensitive(self):
        """Test that setup_logging accepts case-insensitive level."""
        setup_logging(level="warning")
        root_logger = logging.getLogger()
        assert root_logger.level == logging.WARNING


# ============================================================================
# METRICS UTILITIES TESTS
# ============================================================================

class TestMetricsUtilities:
    """Tests for metrics utilities."""
    
    def test_record_request_no_error(self):
        """Test that record_request doesn't raise errors."""
        # Should not raise any exceptions
        record_request("test_service", "/test", "GET", "success")
    
    def test_record_error_no_error(self):
        """Test that record_error doesn't raise errors."""
        record_error("test_service", "connection_error", "error")
    
    def test_record_latency_no_error(self):
        """Test that record_latency doesn't raise errors."""
        record_latency("test_service", "query", 0.5)
    
    def test_track_latency_context_manager(self):
        """Test track_latency context manager."""
        with track_latency("test_service", "operation"):
            time.sleep(0.01)  # Small delay
        # Should complete without error
    
    def test_track_latency_decorator(self):
        """Test track_latency_decorator."""
        @track_latency_decorator("test_service", "decorated_op")
        def my_operation():
            return "result"
        
        result = my_operation()
        assert result == "result"


# ============================================================================
# GRACEFUL SHUTDOWN TESTS
# ============================================================================

class TestGracefulShutdown:
    """Tests for GracefulShutdown class."""
    
    def test_initial_state(self):
        """Test initial shutdown state."""
        shutdown = GracefulShutdown(graceful_shutdown_timeout=30)
        assert shutdown.shutdown_requested is False
        assert shutdown.batch_in_progress is False
        assert shutdown.was_graceful is True
    
    def test_request_shutdown(self):
        """Test request_shutdown sets flag."""
        shutdown = GracefulShutdown()
        shutdown.request_shutdown(15)  # SIGTERM
        assert shutdown.shutdown_requested is True
        assert shutdown.shutdown_start_time is not None
    
    def test_mark_batch_start_end(self):
        """Test batch start/end tracking."""
        shutdown = GracefulShutdown()
        
        shutdown.mark_batch_start(1)
        assert shutdown.batch_in_progress is True
        assert shutdown.current_batch_id == 1
        
        shutdown.mark_batch_end(1)
        assert shutdown.batch_in_progress is False
        assert shutdown.current_batch_id is None
    
    def test_should_skip_batch(self):
        """Test should_skip_batch logic."""
        shutdown = GracefulShutdown()
        
        # No shutdown requested - should not skip
        assert shutdown.should_skip_batch() is False
        
        # Shutdown requested, no batch in progress - should skip
        shutdown.request_shutdown(15)
        assert shutdown.should_skip_batch() is True
        
        # Shutdown requested, batch in progress - should not skip
        shutdown.mark_batch_start(1)
        assert shutdown.should_skip_batch() is False
    
    def test_get_state(self):
        """Test get_state returns ShutdownState."""
        shutdown = GracefulShutdown(graceful_shutdown_timeout=60)
        state = shutdown.get_state()
        
        assert isinstance(state, ShutdownState)
        assert state.shutdown_requested is False
        assert state.graceful_shutdown_timeout == 60
    
    def test_wait_for_batch_completion_no_batch(self):
        """Test wait_for_batch_completion when no batch in progress."""
        shutdown = GracefulShutdown()
        shutdown.request_shutdown(15)
        
        result = shutdown.wait_for_batch_completion()
        assert result is True
        assert shutdown.was_graceful is True


# ============================================================================
# SHUTDOWN STATE TESTS
# ============================================================================

class TestShutdownState:
    """Tests for ShutdownState dataclass."""
    
    def test_default_values(self):
        """Test ShutdownState default values."""
        state = ShutdownState()
        assert state.shutdown_requested is False
        assert state.shutdown_start_time is None
        assert state.batch_in_progress is False
        assert state.current_batch_id is None
        assert state.batch_start_time is None
        assert state.graceful_shutdown_timeout == 30
        assert state.was_graceful is True
    
    def test_custom_values(self):
        """Test ShutdownState with custom values."""
        state = ShutdownState(
            shutdown_requested=True,
            graceful_shutdown_timeout=60,
            was_graceful=False
        )
        assert state.shutdown_requested is True
        assert state.graceful_shutdown_timeout == 60
        assert state.was_graceful is False
