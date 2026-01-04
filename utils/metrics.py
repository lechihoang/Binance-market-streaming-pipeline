"""Prometheus metrics utilities for production monitoring."""

from contextlib import contextmanager
from functools import wraps
import time
from typing import Optional, Callable, TypeVar, Any

# Try to import prometheus_client, provide stubs if not available
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Provide stub classes when prometheus_client is not installed
    class _StubMetric:
        """Stub metric that does nothing when prometheus_client is not available."""
        def __init__(self, *args, **kwargs):
            pass
        
        def labels(self, **kwargs):
            return self
        
        def inc(self, amount: float = 1) -> None:
            pass
        
        def dec(self, amount: float = 1) -> None:
            pass
        
        def set(self, value: float) -> None:
            pass
        
        def observe(self, amount: float) -> None:
            pass
    
    Counter = _StubMetric
    Gauge = _StubMetric
    Histogram = _StubMetric


T = TypeVar("T")


REQUESTS_TOTAL = Counter(
    'app_requests_total',
    'Total number of requests',
    ['service', 'endpoint', 'method', 'status']
)

ERRORS_TOTAL = Counter(
    'app_errors_total',
    'Total number of errors',
    ['service', 'error_type', 'severity']
)

LATENCY_HISTOGRAM = Histogram(
    'app_latency_seconds',
    'Request latency in seconds',
    ['service', 'operation'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

CONNECTIONS_ACTIVE = Gauge(
    'app_connections_active',
    'Number of active connections',
    ['service', 'backend']
)

MESSAGES_PROCESSED = Counter(
    'app_messages_processed_total',
    'Total messages processed',
    ['service', 'topic', 'status']
)

PROCESSING_LATENCY = Histogram(
    'app_processing_latency_ms',
    'Message processing latency in milliseconds',
    ['service', 'operation'],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000]
)

RETRY_ATTEMPTS = Counter(
    'app_retry_attempts_total',
    'Total retry attempts',
    ['service', 'operation', 'result']
)


def record_request(
    service: str,
    endpoint: str,
    method: str = "GET",
    status: str = "success"
) -> None:
    REQUESTS_TOTAL.labels(
        service=service,
        endpoint=endpoint,
        method=method,
        status=status
    ).inc()


def record_error(
    service: str,
    error_type: str,
    severity: str = "error"
) -> None:
    ERRORS_TOTAL.labels(
        service=service,
        error_type=error_type,
        severity=severity
    ).inc()


def record_latency(
    service: str,
    operation: str,
    latency_seconds: float
) -> None:
    LATENCY_HISTOGRAM.labels(
        service=service,
        operation=operation
    ).observe(latency_seconds)


def record_message_processed(
    service: str,
    topic: str,
    status: str = "success"
) -> None:
    MESSAGES_PROCESSED.labels(
        service=service,
        topic=topic,
        status=status
    ).inc()


def record_retry(
    service: str,
    operation: str,
    result: str
) -> None:
    RETRY_ATTEMPTS.labels(
        service=service,
        operation=operation,
        result=result
    ).inc()


def set_active_connections(
    service: str,
    backend: str,
    count: int
) -> None:
    CONNECTIONS_ACTIVE.labels(
        service=service,
        backend=backend
    ).set(count)


def increment_active_connections(
    service: str,
    backend: str
) -> None:
    CONNECTIONS_ACTIVE.labels(
        service=service,
        backend=backend
    ).inc()


def decrement_active_connections(
    service: str,
    backend: str
) -> None:
    CONNECTIONS_ACTIVE.labels(
        service=service,
        backend=backend
    ).dec()


@contextmanager
def track_latency(service: str, operation: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        latency = time.perf_counter() - start
        record_latency(service, operation, latency)


def track_latency_decorator(
    service: str,
    operation: Optional[str] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation if operation is not None else func.__name__
        
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            with track_latency(service, op_name):
                return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


@contextmanager
def track_processing_latency(service: str, operation: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        latency_ms = (time.perf_counter() - start) * 1000
        PROCESSING_LATENCY.labels(
            service=service,
            operation=operation
        ).observe(latency_ms)


def track_processing_latency_decorator(
    service: str,
    operation: Optional[str] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation if operation is not None else func.__name__
        
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            with track_processing_latency(service, op_name):
                return func(*args, **kwargs)
        
        return wrapper
    
    return decorator
