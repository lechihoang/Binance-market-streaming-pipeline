"""
Retry utilities with exponential backoff.

Provides:
- ExponentialBackoff: Calculator for backoff delays with jitter
- RetryConfig: Configuration dataclass for retry behavior
- retry_operation(): Function wrapper for retrying callables
- retry_operation_async(): Async version of retry_operation
- @with_retry: Decorator for adding retry logic to functions

Extracted from ticker_consumer/core.py and storage/backends.py to provide
a centralized, reusable retry mechanism across all modules.

Requirements:
- 1.1: Provide ExponentialBackoff with configurable parameters
- 1.2: Provide @with_retry decorator
- 1.3: Provide retry_operation() function
- 1.4: Log attempts and final result
- 1.5: Raise last exception on exhaustion
- 1.6: Support both sync and async operations
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type, TypeVar

from src.utils.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


@dataclass
class RetryConfig:
    """Configuration for retry behavior.
    
    Attributes:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay_ms: Initial delay in milliseconds (default: 1000)
        max_delay_ms: Maximum delay in milliseconds (default: 60000)
        multiplier: Multiplier for exponential backoff (default: 2.0)
        jitter_factor: Random jitter factor 0-1 (default: 0.1)
        retryable_exceptions: Tuple of exception types to retry on (default: all)
    """
    
    max_retries: int = 3
    initial_delay_ms: int = 1000
    max_delay_ms: int = 60000
    multiplier: float = 2.0
    jitter_factor: float = 0.1
    retryable_exceptions: Tuple[Type[Exception], ...] = field(
        default_factory=lambda: (Exception,)
    )


class ExponentialBackoff:
    """
    Exponential backoff calculator with jitter.
    
    Implements exponential backoff for retry mechanisms with configurable
    initial delay, max delay, multiplier, and jitter factor.
    
    The delay formula is:
        delay = min(initial_delay * multiplier^attempt, max_delay) + jitter
    
    Where jitter = delay * jitter_factor * random(0, 1)
    
    Example:
        backoff = ExponentialBackoff(initial_delay_ms=1000, multiplier=2.0)
        delay1 = backoff.next_delay_ms()  # ~1000ms
        delay2 = backoff.next_delay_ms()  # ~2000ms
        delay3 = backoff.next_delay_ms()  # ~4000ms
        backoff.reset()  # Reset to start
    
    Requirement 1.1: Configurable initial_delay, max_delay, multiplier, jitter_factor
    """
    
    def __init__(
        self,
        initial_delay_ms: int = 1000,
        max_delay_ms: int = 60000,
        multiplier: float = 2.0,
        jitter_factor: float = 0.1,
    ):
        """
        Initialize exponential backoff calculator.
        
        Args:
            initial_delay_ms: Initial delay in milliseconds (default: 1000)
            max_delay_ms: Maximum delay in milliseconds (default: 60000)
            multiplier: Multiplier for each attempt (default: 2.0)
            jitter_factor: Random jitter factor 0-1 (default: 0.1)
        """
        if initial_delay_ms <= 0:
            raise ValueError("initial_delay_ms must be positive")
        if max_delay_ms <= 0:
            raise ValueError("max_delay_ms must be positive")
        if multiplier < 1.0:
            raise ValueError("multiplier must be >= 1.0")
        if not 0.0 <= jitter_factor <= 1.0:
            raise ValueError("jitter_factor must be between 0.0 and 1.0")
        
        self.initial_delay_ms = initial_delay_ms
        self.max_delay_ms = max_delay_ms
        self.multiplier = multiplier
        self.jitter_factor = jitter_factor
        self._attempt = 0
    
    def next_delay_ms(self) -> int:
        """
        Calculate next delay with exponential backoff and jitter.
        
        Returns:
            Delay in milliseconds
        """
        # Calculate base delay with exponential backoff
        delay = self.initial_delay_ms * (self.multiplier ** self._attempt)
        
        # Cap at max delay
        delay = min(delay, self.max_delay_ms)
        
        # Add jitter
        jitter = delay * self.jitter_factor * random.random()
        delay = delay + jitter
        
        # Increment attempt counter
        self._attempt += 1
        
        return int(delay)
    
    def reset(self) -> None:
        """Reset attempt counter to zero."""
        self._attempt = 0
    
    @property
    def attempt_count(self) -> int:
        """Get current attempt count."""
        return self._attempt
    
    def __repr__(self) -> str:
        return (
            f"ExponentialBackoff(initial_delay_ms={self.initial_delay_ms}, "
            f"max_delay_ms={self.max_delay_ms}, multiplier={self.multiplier}, "
            f"jitter_factor={self.jitter_factor}, attempt={self._attempt})"
        )



def retry_operation(
    operation: Callable[[], T],
    config: Optional[RetryConfig] = None,
    operation_name: str = "operation",
    on_retry: Optional[Callable[[int, int, Exception], None]] = None,
) -> T:
    """
    Execute operation with retry and exponential backoff.
    
    This function wraps a callable and retries it on failure using
    exponential backoff. It logs each attempt and the final result.
    
    Args:
        operation: Callable to execute (no arguments)
        config: Retry configuration (uses defaults if None)
        operation_name: Name for logging purposes
        on_retry: Optional callback(attempt, delay_ms, exception) before each retry
        
    Returns:
        Result of the operation
        
    Raises:
        Exception: The last exception if all retries are exhausted
        
    Example:
        def connect_to_db():
            return database.connect()
        
        connection = retry_operation(
            connect_to_db,
            config=RetryConfig(max_retries=5),
            operation_name="database_connection"
        )
    
    Requirements:
    - 1.3: Support sync operations with retry logic
    - 1.4: Log attempts and final result
    - 1.5: Raise last exception on exhaustion
    """
    if config is None:
        config = RetryConfig()
    
    backoff = ExponentialBackoff(
        initial_delay_ms=config.initial_delay_ms,
        max_delay_ms=config.max_delay_ms,
        multiplier=config.multiplier,
        jitter_factor=config.jitter_factor,
    )
    
    last_error: Optional[Exception] = None
    
    for attempt in range(config.max_retries):
        try:
            result = operation()
            
            # Log success after retries (Requirement 1.4)
            if attempt > 0:
                logger.info(
                    f"{operation_name} succeeded after {attempt + 1} attempts"
                )
            
            return result
            
        except config.retryable_exceptions as e:
            last_error = e
            
            if attempt < config.max_retries - 1:
                delay_ms = backoff.next_delay_ms()
                
                # Log retry attempt (Requirement 1.4)
                logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}/{config.max_retries}): "
                    f"{e}. Retrying in {delay_ms}ms..."
                )
                
                # Call optional retry callback
                if on_retry:
                    on_retry(attempt + 1, delay_ms, e)
                
                time.sleep(delay_ms / 1000.0)
            else:
                # Log final failure (Requirement 1.4)
                logger.error(
                    f"{operation_name} failed after {config.max_retries} attempts: {e}"
                )
    
    # Raise last exception (Requirement 1.5)
    if last_error is not None:
        raise last_error
    
    # This should never happen, but satisfy type checker
    raise RuntimeError(f"{operation_name} failed with no exception captured")


async def retry_operation_async(
    operation: Callable[[], T],
    config: Optional[RetryConfig] = None,
    operation_name: str = "operation",
    on_retry: Optional[Callable[[int, int, Exception], None]] = None,
) -> T:
    """
    Async version of retry_operation.
    
    Execute an async or sync operation with retry and exponential backoff.
    Uses asyncio.sleep instead of time.sleep for non-blocking delays.
    
    Args:
        operation: Callable to execute (can be sync or async)
        config: Retry configuration (uses defaults if None)
        operation_name: Name for logging purposes
        on_retry: Optional callback(attempt, delay_ms, exception) before each retry
        
    Returns:
        Result of the operation
        
    Raises:
        Exception: The last exception if all retries are exhausted
        
    Example:
        async def fetch_data():
            return await http_client.get("/api/data")
        
        data = await retry_operation_async(
            fetch_data,
            config=RetryConfig(max_retries=3),
            operation_name="api_fetch"
        )
    
    Requirement 1.6: Support async operations
    """
    if config is None:
        config = RetryConfig()
    
    backoff = ExponentialBackoff(
        initial_delay_ms=config.initial_delay_ms,
        max_delay_ms=config.max_delay_ms,
        multiplier=config.multiplier,
        jitter_factor=config.jitter_factor,
    )
    
    last_error: Optional[Exception] = None
    
    for attempt in range(config.max_retries):
        try:
            # Handle both sync and async operations
            result = operation()
            if asyncio.iscoroutine(result):
                result = await result
            
            # Log success after retries
            if attempt > 0:
                logger.info(
                    f"{operation_name} succeeded after {attempt + 1} attempts"
                )
            
            return result
            
        except config.retryable_exceptions as e:
            last_error = e
            
            if attempt < config.max_retries - 1:
                delay_ms = backoff.next_delay_ms()
                
                logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}/{config.max_retries}): "
                    f"{e}. Retrying in {delay_ms}ms..."
                )
                
                if on_retry:
                    on_retry(attempt + 1, delay_ms, e)
                
                # Use asyncio.sleep for non-blocking delay
                await asyncio.sleep(delay_ms / 1000.0)
            else:
                logger.error(
                    f"{operation_name} failed after {config.max_retries} attempts: {e}"
                )
    
    # Raise last exception
    if last_error is not None:
        raise last_error
    
    raise RuntimeError(f"{operation_name} failed with no exception captured")


def with_retry(
    config: Optional[RetryConfig] = None,
    operation_name: Optional[str] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for adding retry logic to functions.
    
    Wraps a function with retry logic using exponential backoff.
    The decorated function will be retried on failure according to
    the provided configuration.
    
    Args:
        config: Retry configuration (uses defaults if None)
        operation_name: Name for logging (uses function name if None)
        
    Returns:
        Decorator function
        
    Example:
        @with_retry(config=RetryConfig(max_retries=5))
        def fetch_data():
            return api.get_data()
        
        # Or with defaults
        @with_retry()
        def connect():
            return database.connect()
    
    Requirement 1.2: Provide @with_retry decorator
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            name = operation_name or func.__name__
            return retry_operation(
                lambda: func(*args, **kwargs),
                config=config,
                operation_name=name,
            )
        return wrapper
    return decorator


def with_retry_async(
    config: Optional[RetryConfig] = None,
    operation_name: Optional[str] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Async decorator for adding retry logic to async functions.
    
    Wraps an async function with retry logic using exponential backoff.
    
    Args:
        config: Retry configuration (uses defaults if None)
        operation_name: Name for logging (uses function name if None)
        
    Returns:
        Decorator function
        
    Example:
        @with_retry_async(config=RetryConfig(max_retries=3))
        async def fetch_data():
            return await api.get_data()
    
    Requirement 1.6: Support async operations
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            name = operation_name or func.__name__
            return await retry_operation_async(
                lambda: func(*args, **kwargs),
                config=config,
                operation_name=name,
            )
        return wrapper
    return decorator
