"""Graceful shutdown utilities for long-running services."""

import logging
import time


class GracefulShutdown:
    """Simple graceful shutdown handler for batch processing jobs."""

    def __init__(
        self,
        graceful_shutdown_timeout: int = 15,
        logger: logging.Logger | None = None,
    ):
        self.graceful_shutdown_timeout = graceful_shutdown_timeout
        self.shutdown_requested: bool = False
        self.batch_in_progress: bool = False
        self.current_batch_id: int | None = None
        self.batch_start_time: float | None = None
        self.logger = logger or logging.getLogger(__name__)

    def request_shutdown(self, signal_num: int) -> None:
        """Handle shutdown signal."""
        self.shutdown_requested = True
        signal_names = {2: "SIGINT", 15: "SIGTERM"}
        signal_name = signal_names.get(signal_num, f"Signal {signal_num}")
        self.logger.info(f"Shutdown requested: {signal_name}")

        if self.batch_in_progress and self.current_batch_id is not None:
            elapsed = time.time() - self.batch_start_time if self.batch_start_time else 0
            self.logger.info(f"Batch {self.current_batch_id} in progress, elapsed={elapsed:.1f}s")

    def mark_batch_start(self, batch_id: int) -> None:
        """Mark batch processing as started."""
        self.batch_in_progress = True
        self.current_batch_id = batch_id
        self.batch_start_time = time.time()

    def mark_batch_end(self, batch_id: int) -> None:
        """Mark batch processing as completed."""
        if self.current_batch_id == batch_id:
            elapsed = time.time() - self.batch_start_time if self.batch_start_time else 0
            self.logger.debug(f"Batch {batch_id} completed in {elapsed:.2f}s")

        self.batch_in_progress = False
        self.current_batch_id = None
        self.batch_start_time = None
