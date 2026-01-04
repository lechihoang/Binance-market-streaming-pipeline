"""Retry utilities with exponential backoff."""

import asyncio
import random
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type, TypeVar

from utils.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


@dataclass
class RetryConfig:
    max_retries: int = 3
    initial_delay_ms: int = 1000
    max_delay_ms: int = 60000
    multiplier: float = 2.0
    jitter_factor: float = 0.1
    retryable_exceptions: Tuple[Type[Exception], ...] = field(
        default_factory=lambda: (Exception,)
    )


class ExponentialBackoff:
    def __init__(
        self,
        initial_delay_ms: int = 1000,
        max_delay_ms: int = 60000,
        multiplier: float = 2.0,
        jitter_factor: float = 0.1,
    ):
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
        delay = self.initial_delay_ms * (self.multiplier ** self._attempt)
        delay = min(delay, self.max_delay_ms)
        jitter = delay * self.jitter_factor * random.random()
        delay = delay + jitter
        self._attempt += 1
        return int(delay)
    
    def reset(self) -> None:
        self._attempt = 0
    
    @property
    def attempt_count(self) -> int:
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
                
                # Call optional retry callback
                if on_retry:
                    on_retry(attempt + 1, delay_ms, e)
                
                time.sleep(delay_ms / 1000.0)
            else:
                logger.error(
                    f"{operation_name} failed after {config.max_retries} attempts: {e}"
                )
    
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
                
                await asyncio.sleep(delay_ms / 1000.0)
            else:
                logger.error(
                    f"{operation_name} failed after {config.max_retries} attempts: {e}"
                )
    
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{operation_name} failed with no exception captured")


def with_retry(
    config: Optional[RetryConfig] = None,
    operation_name: Optional[str] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
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
