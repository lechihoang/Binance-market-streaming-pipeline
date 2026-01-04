"""Graceful shutdown utilities for long-running services."""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class ShutdownState:
    shutdown_requested: bool = False
    shutdown_start_time: Optional[float] = None
    batch_in_progress: bool = False
    current_batch_id: Optional[int] = None
    batch_start_time: Optional[float] = None
    graceful_shutdown_timeout: int = 30
    was_graceful: bool = True


@dataclass
class ShutdownEvent:
    signal_type: int
    timestamp: datetime
    batch_id: Optional[int]
    was_graceful: bool
    wait_duration_seconds: float


class GracefulShutdown:
    def __init__(
        self,
        graceful_shutdown_timeout: int = 15,
        shutdown_progress_interval: int = 5,
        logger: Optional[logging.Logger] = None
    ):
        self.graceful_shutdown_timeout = graceful_shutdown_timeout
        self.shutdown_progress_interval = shutdown_progress_interval
        self.shutdown_requested: bool = False
        self.batch_in_progress: bool = False
        self.current_batch_id: Optional[int] = None
        self.batch_start_time: Optional[float] = None
        self.shutdown_start_time: Optional[float] = None
        self.was_graceful: bool = True
        self.logger = logger or logging.getLogger(__name__)

    def request_shutdown(self, signal_num: int) -> None:
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
        self.batch_in_progress = True
        self.current_batch_id = batch_id
        self.batch_start_time = time.time()
        self.logger.debug(f"Batch {batch_id} started")

    def mark_batch_end(self, batch_id: int) -> None:
        if self.current_batch_id == batch_id:
            elapsed = time.time() - self.batch_start_time if self.batch_start_time else 0
            self.logger.debug(f"Batch {batch_id} completed in {elapsed:.2f}s")
        
        self.batch_in_progress = False
        self.current_batch_id = None
        self.batch_start_time = None

    def should_skip_batch(self) -> bool:
        if self.shutdown_requested and not self.batch_in_progress:
            self.logger.info("Skipping batch due to shutdown request")
            return True
        return False

    def get_state(self) -> ShutdownState:
        return ShutdownState(
            shutdown_requested=self.shutdown_requested,
            shutdown_start_time=self.shutdown_start_time,
            batch_in_progress=self.batch_in_progress,
            current_batch_id=self.current_batch_id,
            batch_start_time=self.batch_start_time,
            graceful_shutdown_timeout=self.graceful_shutdown_timeout,
            was_graceful=self.was_graceful
        )
