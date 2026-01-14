"""Retry utilities with exponential backoff."""

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from util.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


@dataclass
class RetryConfig:
    """Configuration for retry operations."""

    max_retries: int = 3
    initial_delay_ms: int = 1000
    max_delay_ms: int = 60000
    multiplier: float = 2.0
    jitter_factor: float = 0.1
    retryable_exceptions: tuple[type[Exception], ...] = field(default_factory=lambda: (Exception,))


class ExponentialBackoff:
    """Exponential backoff calculator with jitter."""

    def __init__(
        self,
        initial_delay_ms: int = 1000,
        max_delay_ms: int = 60000,
        multiplier: float = 2.0,
        jitter_factor: float = 0.1,
    ):
        self.initial_delay_ms = initial_delay_ms
        self.max_delay_ms = max_delay_ms
        self.multiplier = multiplier
        self.jitter_factor = jitter_factor
        self._attempt = 0

    def next_delay_ms(self) -> int:
        """Calculate next delay with exponential backoff and jitter."""
        delay = self.initial_delay_ms * (self.multiplier**self._attempt)
        delay = min(delay, self.max_delay_ms)
        jitter = delay * self.jitter_factor * random.random()
        self._attempt += 1
        return int(delay + jitter)

    def reset(self) -> None:
        """Reset attempt counter."""
        self._attempt = 0

    @property
    def attempt_count(self) -> int:
        return self._attempt


def retry_operation(
    operation: Callable[[], T],
    config: RetryConfig | None = None,
    operation_name: str = "operation",
    on_retry: Callable[[int, int, Exception], None] | None = None,
) -> T:
    """Execute operation with retry logic."""
    if config is None:
        config = RetryConfig()

    backoff = ExponentialBackoff(
        initial_delay_ms=config.initial_delay_ms,
        max_delay_ms=config.max_delay_ms,
        multiplier=config.multiplier,
        jitter_factor=config.jitter_factor,
    )

    last_error: Exception | None = None

    for attempt in range(config.max_retries):
        try:
            result = operation()
            if attempt > 0:
                logger.info(f"{operation_name} succeeded after {attempt + 1} attempts")
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
                time.sleep(delay_ms / 1000.0)
            else:
                logger.error(f"{operation_name} failed after {config.max_retries} attempts: {e}")

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{operation_name} failed with no exception captured")
