"""
Graceful shutdown utilities for long-running services.

Provides:
- GracefulShutdown: Handler for shutdown signals
- ShutdownState: Dataclass for tracking shutdown state
- ShutdownEvent: Dataclass for shutdown event logging

This module was extracted from streaming/graceful_shutdown.py
to provide reusable shutdown handling across all services.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class ShutdownState:
    """Tracks shutdown state for a job."""
    shutdown_requested: bool = False
    shutdown_start_time: Optional[float] = None
    batch_in_progress: bool = False
    current_batch_id: Optional[int] = None
    batch_start_time: Optional[float] = None
    graceful_shutdown_timeout: int = 30
    was_graceful: bool = True


@dataclass
class ShutdownEvent:
    """Represents a shutdown event for logging."""
    signal_type: int
    timestamp: datetime
    batch_id: Optional[int]
    was_graceful: bool
    wait_duration_seconds: float


class GracefulShutdown:
    """
    Class providing graceful shutdown capabilities for long-running services.
    
    Allows in-progress work to complete before termination,
    with configurable timeout and progress logging.
    
    Usage:
        shutdown_handler = GracefulShutdown(timeout=30)
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, lambda s, f: shutdown_handler.request_shutdown(s))
        signal.signal(signal.SIGINT, lambda s, f: shutdown_handler.request_shutdown(s))
        
        # In processing loop
        while not shutdown_handler.should_skip_batch():
            shutdown_handler.mark_batch_start(batch_id)
            try:
                process_batch(batch_id)
            finally:
                shutdown_handler.mark_batch_end(batch_id)
        
        # Wait for completion before exit
        shutdown_handler.wait_for_batch_completion()
    """
    
    def __init__(
        self,
        graceful_shutdown_timeout: int = 30,
        shutdown_progress_interval: int = 5,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize GracefulShutdown.
        
        Args:
            graceful_shutdown_timeout: Maximum seconds to wait for work completion (default: 30)
            shutdown_progress_interval: Seconds between progress log messages (default: 5)
            logger: Optional logger instance. If None, creates a new one.
        """
        # Configuration
        self.graceful_shutdown_timeout = graceful_shutdown_timeout
        self.shutdown_progress_interval = shutdown_progress_interval
        
        # State tracking
        self.shutdown_requested: bool = False
        self.batch_in_progress: bool = False
        self.current_batch_id: Optional[int] = None
        self.batch_start_time: Optional[float] = None
        self.shutdown_start_time: Optional[float] = None
        self.was_graceful: bool = True
        
        # Logger
        self.logger = logger or logging.getLogger(__name__)

    def request_shutdown(self, signal_num: int) -> None:
        """
        Request graceful shutdown, setting flag and logging signal.
        
        Args:
            signal_num: The signal number received (e.g., SIGINT=2, SIGTERM=15)
        """
        timestamp = datetime.now(timezone.utc)
        self.shutdown_requested = True
        self.shutdown_start_time = time.time()
        
        signal_names = {2: "SIGINT", 15: "SIGTERM"}
        signal_name = signal_names.get(signal_num, f"Signal {signal_num}")
        
        self.logger.info(
            f"Shutdown requested: signal={signal_name} ({signal_num}), "
            f"timestamp={timestamp.isoformat()}"
        )
        
        if self.batch_in_progress and self.current_batch_id is not None:
            elapsed = time.time() - self.batch_start_time if self.batch_start_time else 0
            self.logger.info(
                f"Batch {self.current_batch_id} in progress during shutdown, "
                f"elapsed={elapsed:.1f}s"
            )

    def wait_for_batch_completion(self) -> bool:
        """
        Wait for current work to complete within timeout.
        
        Logs progress every shutdown_progress_interval seconds.
        
        Returns:
            True if shutdown was graceful (work completed), False if timeout exceeded
        """
        if not self.shutdown_requested:
            return True
        
        if not self.batch_in_progress:
            self.logger.info("No batch in progress, proceeding with shutdown")
            self.was_graceful = True
            return True
        
        self.logger.info(
            f"Waiting for batch {self.current_batch_id} to complete "
            f"(timeout={self.graceful_shutdown_timeout}s)"
        )
        
        wait_start = time.time()
        last_log_time = wait_start
        
        while self.batch_in_progress:
            elapsed = time.time() - wait_start
            
            # Check timeout
            if elapsed >= self.graceful_shutdown_timeout:
                self.logger.warning(
                    f"Graceful shutdown timeout exceeded ({self.graceful_shutdown_timeout}s), "
                    f"forcing stop"
                )
                self.was_graceful = False
                return False
            
            # Log progress at intervals
            if time.time() - last_log_time >= self.shutdown_progress_interval:
                remaining = self.graceful_shutdown_timeout - elapsed
                batch_elapsed = time.time() - self.batch_start_time if self.batch_start_time else 0
                self.logger.info(
                    f"Shutdown progress: waiting for batch {self.current_batch_id}, "
                    f"batch_elapsed={batch_elapsed:.1f}s, "
                    f"timeout_remaining={remaining:.1f}s"
                )
                last_log_time = time.time()
            
            # Small sleep to avoid busy waiting
            time.sleep(0.1)
        
        total_wait = time.time() - wait_start
        self.logger.info(f"Batch completed after {total_wait:.1f}s wait")
        self.was_graceful = True
        return True

    def mark_batch_start(self, batch_id: int) -> None:
        """
        Mark batch/work as started for tracking.
        
        Args:
            batch_id: The batch/work identifier
        """
        self.batch_in_progress = True
        self.current_batch_id = batch_id
        self.batch_start_time = time.time()
        self.logger.debug(f"Batch {batch_id} started")

    def mark_batch_end(self, batch_id: int) -> None:
        """
        Mark batch/work as completed.
        
        Args:
            batch_id: The batch/work identifier
        """
        if self.current_batch_id == batch_id:
            elapsed = time.time() - self.batch_start_time if self.batch_start_time else 0
            self.logger.debug(f"Batch {batch_id} completed in {elapsed:.2f}s")
        
        self.batch_in_progress = False
        self.current_batch_id = None
        self.batch_start_time = None

    def should_skip_batch(self) -> bool:
        """
        Check if new work should be skipped due to shutdown request.
        
        Returns True if shutdown is requested AND no work is currently in progress.
        This allows in-progress work to complete while preventing new work from starting.
        
        Returns:
            True if work should be skipped, False otherwise
        """
        if self.shutdown_requested and not self.batch_in_progress:
            self.logger.info("Skipping batch due to shutdown request")
            return True
        return False

    def get_state(self) -> ShutdownState:
        """
        Get current shutdown state as a dataclass.
        
        Returns:
            ShutdownState with current values
        """
        return ShutdownState(
            shutdown_requested=self.shutdown_requested,
            shutdown_start_time=self.shutdown_start_time,
            batch_in_progress=self.batch_in_progress,
            current_batch_id=self.current_batch_id,
            batch_start_time=self.batch_start_time,
            graceful_shutdown_timeout=self.graceful_shutdown_timeout,
            was_graceful=self.was_graceful
        )
