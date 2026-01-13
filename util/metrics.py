"""Prometheus metrics utilities for production monitoring."""

import time
from contextlib import contextmanager

from prometheus_client import Counter, Histogram


# --- Metrics Definitions ---

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

MESSAGES_PROCESSED = Counter(
    'app_messages_processed_total',
    'Total messages processed',
    ['service', 'topic', 'status']
)

RETRY_ATTEMPTS = Counter(
    'app_retry_attempts_total',
    'Total retry attempts',
    ['service', 'operation', 'result']
)


# --- Helper Functions ---

def record_error(service: str, error_type: str, severity: str = "error") -> None:
    """Record an error occurrence."""
    ERRORS_TOTAL.labels(service=service, error_type=error_type, severity=severity).inc()


def record_latency(service: str, operation: str, latency_seconds: float) -> None:
    """Record operation latency."""
    LATENCY_HISTOGRAM.labels(service=service, operation=operation).observe(latency_seconds)


def record_message_processed(service: str, topic: str, status: str = "success") -> None:
    """Record a processed message."""
    MESSAGES_PROCESSED.labels(service=service, topic=topic, status=status).inc()


def record_retry(service: str, operation: str, result: str) -> None:
    """Record a retry attempt."""
    RETRY_ATTEMPTS.labels(service=service, operation=operation, result=result).inc()


@contextmanager
def track_latency(service: str, operation: str):
    """Context manager to track operation latency."""
    start = time.perf_counter()
    try:
        yield
    finally:
        record_latency(service, operation, time.perf_counter() - start)
