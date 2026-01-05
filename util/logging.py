"""Centralized logging configuration utilities."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        if record.pathname and record.lineno:
            log_data["location"] = {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            }
        
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        if hasattr(record, "extra_fields") and record.extra_fields:
            log_data["extra"] = record.extra_fields
        
        return json.dumps(log_data)


class StructuredFormatter(logging.Formatter):
    def __init__(self, job_name: str):
        super().__init__()
        self.job_name = job_name

    def format(self, record: logging.LogRecord) -> str:
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
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.handlers.clear()
    
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setLevel(numeric_level)
    
    if json_output:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(format_string)
    
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
