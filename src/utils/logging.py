"""
Centralized logging configuration utilities.

Provides consistent logging setup across all modules with support for
structured JSON output and configurable formatting.

This module provides:
- setup_logging(): Configure logging for the application
- get_logger(): Get a configured logger by name
- JSONFormatter: JSON-formatted log output
- StructuredFormatter: Structured text log output with job name

Usage:
------
    from src.utils.logging import setup_logging, get_logger

    # Set up logging at application startup
    setup_logging(level="INFO", json_output=True)

    # Get a logger for your module
    logger = get_logger(__name__)
    logger.info("Application started")

    # Use StructuredFormatter for streaming jobs
    from src.utils.logging import StructuredFormatter
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter("MyJob"))

Requirements Coverage:
---------------------
    - Requirement 4.1: setup_logging() with configurable level and format
    - Requirement 4.2: JSON output option for structured logging
    - Requirement 4.3: Include timestamp, level, logger name, message
    - Requirement 4.4: get_logger() helper for configured loggers
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """
    Custom formatter that outputs log records as JSON.
    
    Includes timestamp, level, logger name, message, and any extra fields.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add location info if available
        if record.pathname and record.lineno:
            log_data["location"] = {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add any extra fields passed to the logger
        if hasattr(record, "extra_fields") and record.extra_fields:
            log_data["extra"] = record.extra_fields
        
        return json.dumps(log_data)


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter for structured logging with job name.
    
    Outputs log records in a structured text format:
    [timestamp] [level] [job_name] [logger_name] message
    
    This formatter is designed for streaming jobs that need consistent,
    parseable log output with job identification.
    
    Args:
        job_name: Name of the job for log identification
    
    Example:
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredFormatter("TechnicalIndicatorsJob"))
        logger.addHandler(handler)
        
        # Output: [2024-01-15T10:30:00.123456] [INFO] [TechnicalIndicatorsJob] [module] Message
    """

    def __init__(self, job_name: str):
        """
        Initialize formatter with job name.
        
        Args:
            job_name: Name of the job for log identification
        """
        super().__init__()
        self.job_name = job_name

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with structured information.
        
        Returns:
            Formatted string: [timestamp] [level] [job_name] [logger_name] message
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        log_parts = [
            f"[{timestamp}]",
            f"[{record.levelname}]",
            f"[{self.job_name}]",
            f"[{record.name}]",
            record.getMessage(),
        ]

        if record.exc_info:
            log_parts.append("\n" + self.formatException(record.exc_info))

        return " ".join(log_parts)


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    json_output: bool = False,
    stream: Optional[Any] = None,
) -> None:
    """
    Configure logging for the application.
    
    Sets up the root logger with the specified configuration. This function
    should be called once at application startup.
    
    Args:
        level: Log level as string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               Case-insensitive. Defaults to "INFO".
        format_string: Custom format string for text output. If None, uses
                      default format with timestamp, level, logger name, message.
        json_output: If True, output logs as JSON for structured logging.
                    Overrides format_string when enabled.
        stream: Output stream for logs. Defaults to sys.stdout.
    
    Example:
        # Basic setup with INFO level
        setup_logging()
        
        # Debug level with JSON output
        setup_logging(level="DEBUG", json_output=True)
        
        # Custom format
        setup_logging(format_string="%(levelname)s - %(message)s")
    
    Requirements:
        - 4.1: Configurable level and format
        - 4.2: JSON output option
        - 4.3: Include timestamp, level, logger name, message
    """
    # Parse log level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Default format includes timestamp, level, logger name, message
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers to avoid duplicates
    root_logger.handlers.clear()
    
    # Create handler
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setLevel(numeric_level)
    
    # Set formatter based on json_output flag
    if json_output:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(format_string)
    
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger by name.
    
    Returns a logger instance that inherits configuration from the root
    logger set up by setup_logging(). If setup_logging() hasn't been called,
    the logger will use Python's default configuration.
    
    Args:
        name: Name for the logger. Typically use __name__ to get a logger
              named after the current module.
    
    Returns:
        A configured logging.Logger instance.
    
    Example:
        logger = get_logger(__name__)
        logger.info("Processing started")
        logger.error("An error occurred", exc_info=True)
    
    Requirements:
        - 4.4: Return configured logger by name
    """
    return logging.getLogger(name)
