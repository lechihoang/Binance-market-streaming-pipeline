"""
Job Validators Module

Provides validator classes for validating output from streaming jobs.
Each validator defines required fields, nullable fields, and validation logic.

Also provides validation functions that query Redis for recent data and
validate using the appropriate validator class.

Requirements:
- 2.1, 2.2, 2.3, 2.4, 2.5: Aggregation validation
- 4.1, 4.2, 4.3, 4.4, 4.5: Anomaly validation
- 5.1, 5.2, 5.3, 5.4, 5.5: Storage tier validation
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from src.utils.logging import get_logger

logger = get_logger(__name__)


# Constants for retry logic
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1.0  # Base delay in seconds for exponential backoff


@dataclass
class ValidationError:
    """Represents a validation error for a single record."""
    
    record_identifier: str
    error_type: str
    message: str
    missing_fields: List[str] = field(default_factory=list)
    invalid_values: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of validating a set of records."""
    
    validator_name: str
    is_valid: bool
    record_count: int
    valid_count: int
    invalid_count: int
    errors: List[ValidationError] = field(default_factory=list)
    field_completeness: Dict[str, float] = field(default_factory=dict)
    message: str = ""
    
    def __str__(self) -> str:
        status = "PASSED" if self.is_valid else "FAILED"
        return (
            f"{self.validator_name} validation {status}: "
            f"{self.valid_count}/{self.record_count} records valid"
        )


class AggregationValidator:
    """
    Validator for trade aggregation OHLCV output.
    
    Validates that aggregation records contain all required fields
    and allows nullable fields to be null without failing.
    
    Requirements: 2.1, 2.2, 2.3
    """
    
    # Required fields that must be present and non-null
    REQUIRED_FIELDS: Set[str] = {
        'symbol',
        'window_start',
        'window_end',
        'window_duration',
        'open',
        'high',
        'low',
        'close',
        'volume',
    }
    
    # Nullable fields that may be null without causing validation failure
    NULLABLE_FIELDS: Set[str] = {
        'vwap',
        'price_stddev',
        'quote_volume',
        'trade_count',
        'avg_price',
        'buy_count',
        'sell_count',
        'buy_sell_ratio',
        'large_order_count',
        'price_change_pct',
    }
    
    def __init__(self):
        self.name = "AggregationValidator"
    
    def validate(self, records: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate a list of aggregation records.
        
        Args:
            records: List of aggregation record dictionaries
            
        Returns:
            ValidationResult with validation status and details
        """
        errors: List[ValidationError] = []
        valid_count = 0
        field_presence: Dict[str, int] = {f: 0 for f in self.REQUIRED_FIELDS | self.NULLABLE_FIELDS}
        
        for record in records:
            record_errors = self._validate_record(record)
            if record_errors:
                errors.extend(record_errors)
            else:
                valid_count += 1
            
            # Track field presence for completeness stats
            for field_name in field_presence:
                if field_name in record and record[field_name] is not None:
                    field_presence[field_name] += 1
        
        record_count = len(records)
        invalid_count = record_count - valid_count
        is_valid = invalid_count == 0
        
        # Calculate field completeness percentages
        field_completeness = {}
        if record_count > 0:
            for field_name, count in field_presence.items():
                field_completeness[field_name] = round(count / record_count * 100, 2)
        
        message = self._build_message(is_valid, valid_count, record_count, errors)
        
        return ValidationResult(
            validator_name=self.name,
            is_valid=is_valid,
            record_count=record_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            errors=errors,
            field_completeness=field_completeness,
            message=message,
        )
    
    def _validate_record(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate a single aggregation record."""
        errors = []
        record_id = self._get_record_identifier(record)
        
        # Check for missing required fields
        missing_fields = []
        for field_name in self.REQUIRED_FIELDS:
            if field_name not in record:
                missing_fields.append(field_name)
            elif record[field_name] is None:
                missing_fields.append(field_name)
        
        if missing_fields:
            errors.append(ValidationError(
                record_identifier=record_id,
                error_type="missing_required_fields",
                message=f"Missing required fields: {', '.join(missing_fields)}",
                missing_fields=missing_fields,
            ))
        
        return errors
    
    def _get_record_identifier(self, record: Dict[str, Any]) -> str:
        """Get a human-readable identifier for a record."""
        symbol = record.get('symbol', 'unknown')
        window_start = record.get('window_start', 'unknown')
        return f"{symbol}@{window_start}"
    
    def _build_message(
        self, 
        is_valid: bool, 
        valid_count: int, 
        record_count: int,
        errors: List[ValidationError]
    ) -> str:
        """Build a descriptive message for the validation result."""
        if is_valid:
            return f"Validated {record_count} aggregation records successfully"
        
        # Collect unique missing fields across all errors
        all_missing = set()
        for error in errors:
            all_missing.update(error.missing_fields)
        
        return (
            f"Validation failed: {len(errors)} records invalid. "
            f"Missing fields: {', '.join(sorted(all_missing))}"
        )



class AnomalyValidator:
    """
    Validator for anomaly detection alerts output.
    
    Validates that alert records contain all required fields and that
    alert_type and alert_level have valid enum values.
    
    Requirements: 4.1, 4.2, 4.3
    """
    
    # Required fields that must be present and non-null
    REQUIRED_FIELDS: Set[str] = {
        'alert_id',
        'symbol',
        'alert_type',
        'alert_level',
        'timestamp',
        'created_at',
    }
    
    # Valid alert types
    VALID_ALERT_TYPES: Set[str] = {
        'WHALE_ALERT',
        'VOLUME_SPIKE',
        'PRICE_SPIKE',
        'RSI_EXTREME',
        'BB_BREAKOUT',
        'MACD_CROSSOVER',
    }
    
    # Valid alert levels
    VALID_ALERT_LEVELS: Set[str] = {
        'HIGH',
        'MEDIUM',
        'LOW',
    }
    
    # Optional fields that may be present
    NULLABLE_FIELDS: Set[str] = {
        'details',
    }
    
    def __init__(self):
        self.name = "AnomalyValidator"
    
    def validate(self, records: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate a list of alert records.
        
        Args:
            records: List of alert record dictionaries
            
        Returns:
            ValidationResult with validation status and details
        """
        errors: List[ValidationError] = []
        valid_count = 0
        field_presence: Dict[str, int] = {f: 0 for f in self.REQUIRED_FIELDS | self.NULLABLE_FIELDS}
        
        for record in records:
            record_errors = self._validate_record(record)
            if record_errors:
                errors.extend(record_errors)
            else:
                valid_count += 1
            
            # Track field presence for completeness stats
            for field_name in field_presence:
                if field_name in record and record[field_name] is not None:
                    field_presence[field_name] += 1
        
        record_count = len(records)
        invalid_count = record_count - valid_count
        is_valid = invalid_count == 0
        
        # Calculate field completeness percentages
        field_completeness = {}
        if record_count > 0:
            for field_name, count in field_presence.items():
                field_completeness[field_name] = round(count / record_count * 100, 2)
        
        message = self._build_message(is_valid, valid_count, record_count, errors)
        
        return ValidationResult(
            validator_name=self.name,
            is_valid=is_valid,
            record_count=record_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            errors=errors,
            field_completeness=field_completeness,
            message=message,
        )
    
    def _validate_record(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate a single alert record."""
        errors = []
        record_id = self._get_record_identifier(record)
        
        # Check for missing required fields
        missing_fields = []
        for field_name in self.REQUIRED_FIELDS:
            if field_name not in record:
                missing_fields.append(field_name)
            elif record[field_name] is None:
                missing_fields.append(field_name)
        
        if missing_fields:
            errors.append(ValidationError(
                record_identifier=record_id,
                error_type="missing_required_fields",
                message=f"Missing required fields: {', '.join(missing_fields)}",
                missing_fields=missing_fields,
            ))
        
        # Validate alert_type enum value
        alert_type = record.get('alert_type')
        if alert_type is not None and alert_type not in self.VALID_ALERT_TYPES:
            errors.append(ValidationError(
                record_identifier=record_id,
                error_type="invalid_alert_type",
                message=f"Invalid alert_type: {alert_type}. Must be one of: {', '.join(sorted(self.VALID_ALERT_TYPES))}",
                invalid_values={'alert_type': alert_type},
            ))
        
        # Validate alert_level enum value
        alert_level = record.get('alert_level')
        if alert_level is not None and alert_level not in self.VALID_ALERT_LEVELS:
            errors.append(ValidationError(
                record_identifier=record_id,
                error_type="invalid_alert_level",
                message=f"Invalid alert_level: {alert_level}. Must be one of: {', '.join(sorted(self.VALID_ALERT_LEVELS))}",
                invalid_values={'alert_level': alert_level},
            ))
        
        return errors
    
    def _get_record_identifier(self, record: Dict[str, Any]) -> str:
        """Get a human-readable identifier for a record."""
        alert_id = record.get('alert_id', 'unknown')
        symbol = record.get('symbol', 'unknown')
        return f"{alert_id}:{symbol}"
    
    def _build_message(
        self, 
        is_valid: bool, 
        valid_count: int, 
        record_count: int,
        errors: List[ValidationError]
    ) -> str:
        """Build a descriptive message for the validation result."""
        if is_valid:
            if record_count == 0:
                return "No alerts to validate (empty output is valid)"
            return f"Validated {record_count} alert records successfully"
        
        # Collect unique error types
        error_types = set()
        all_missing = set()
        invalid_values = {}
        
        for error in errors:
            error_types.add(error.error_type)
            all_missing.update(error.missing_fields)
            invalid_values.update(error.invalid_values)
        
        parts = [f"Validation failed: {len(errors)} records invalid."]
        
        if all_missing:
            parts.append(f"Missing fields: {', '.join(sorted(all_missing))}")
        
        if invalid_values:
            invalid_str = ", ".join(f"{k}={v}" for k, v in invalid_values.items())
            parts.append(f"Invalid values: {invalid_str}")
        
        return " ".join(parts)


# =============================================================================
# Validation Functions that Query Redis
# =============================================================================


def _retry_with_backoff(
    func,
    max_retries: int = MAX_RETRIES,
    retry_delay_base: float = RETRY_DELAY_BASE,
) -> Tuple[Any, Optional[Exception]]:
    """
    Execute a function with exponential backoff retry logic.
    
    Args:
        func: Callable to execute
        max_retries: Maximum number of retry attempts
        retry_delay_base: Base delay between retries (exponential backoff)
        
    Returns:
        Tuple of (result, error) - result is None if all retries failed
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            result = func()
            return result, None
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = retry_delay_base * (2 ** attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed: {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"All {max_retries} attempts failed. Last error: {e}"
                )
    
    return None, last_error


def validate_aggregation_output(
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    symbols: Optional[List[str]] = None,
    interval: str = "1m",
    **kwargs,
) -> ValidationResult:
    """
    Validate trade aggregation output by querying Redis.
    
    Queries Redis for recent aggregation data and validates records
    using AggregationValidator. Handles retry logic for storage unavailability.
    
    Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 5.1, 5.4, 5.5
    
    Args:
        redis_storage: RedisStorage instance to query
        symbols: List of symbols to validate (if None, uses default symbols)
        interval: Time interval for aggregation data (default: "1m")
        
    Returns:
        ValidationResult with validation status and details
        
    Raises:
        Exception: If Redis is unavailable after all retries
    """
    import os
    from src.storage.redis import RedisStorage
    
    # Get Redis config from parameters or environment
    redis_host = redis_host or os.getenv('REDIS_HOST', 'redis')
    redis_port = redis_port or int(os.getenv('REDIS_PORT', '6379'))
    redis_storage = RedisStorage(host=redis_host, port=redis_port)
    
    validator = AggregationValidator()
    
    # Default symbols if not provided
    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    
    records: List[Dict[str, Any]] = []
    
    def fetch_aggregations():
        """Fetch aggregation data from Redis."""
        fetched = []
        for symbol in symbols:
            data = redis_storage.get_aggregation(symbol, interval)
            if data:
                # Add symbol and interval to the record for validation
                record = {
                    "symbol": symbol,
                    "window_duration": interval,
                    **data,
                }
                # Map Redis fields to expected validation fields
                if "timestamp" in record:
                    record["window_start"] = record.get("timestamp")
                    record["window_end"] = record.get("timestamp")
                fetched.append(record)
        return fetched
    
    # Retry fetching data from Redis
    result, error = _retry_with_backoff(fetch_aggregations)
    
    if error is not None:
        # Storage unavailable after all retries
        raise Exception(
            f"Failed to query Redis for aggregation data after {MAX_RETRIES} retries: {error}"
        )
    
    records = result or []
    
    # Handle empty data scenario (Requirement 5.5)
    if not records:
        logger.warning(
            "No aggregation data found in Redis. "
            "Job may have processed empty batches."
        )
        return ValidationResult(
            validator_name=validator.name,
            is_valid=True,
            record_count=0,
            valid_count=0,
            invalid_count=0,
            errors=[],
            field_completeness={},
            message="No aggregation data found in Redis (empty batches are valid)",
        )
    
    # Validate records
    validation_result = validator.validate(records)
    
    # Log results (Requirement 2.5)
    if validation_result.is_valid:
        logger.info(
            f"Aggregation validation passed: {validation_result.record_count} records validated. "
            f"Field completeness: {validation_result.field_completeness}"
        )
    else:
        logger.error(f"Aggregation validation failed: {validation_result.message}")
        raise ValueError(validation_result.message)
    
    return validation_result


def validate_anomaly_output(
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    limit: int = 100,
    **kwargs,
) -> ValidationResult:
    """
    Validate anomaly detection output by querying Redis.
    
    Queries Redis for recent alert data and validates records
    using AnomalyValidator. Empty output is valid (no anomalies detected).
    
    Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 5.3, 5.4, 5.5
    
    Args:
        redis_storage: RedisStorage instance to query
        limit: Maximum number of alerts to fetch (default: 100)
        
    Returns:
        ValidationResult with validation status and details
        
    Raises:
        Exception: If Redis is unavailable after all retries
    """
    import os
    from src.storage.redis import RedisStorage
    
    # Get Redis config from parameters or environment
    redis_host = redis_host or os.getenv('REDIS_HOST', 'redis')
    redis_port = redis_port or int(os.getenv('REDIS_PORT', '6379'))
    redis_storage = RedisStorage(host=redis_host, port=redis_port)
    
    validator = AnomalyValidator()
    
    def fetch_alerts():
        """Fetch alert data from Redis."""
        return redis_storage.get_recent_alerts(limit=limit)
    
    # Retry fetching data from Redis
    result, error = _retry_with_backoff(fetch_alerts)
    
    if error is not None:
        # Storage unavailable after all retries
        raise Exception(
            f"Failed to query Redis for alert data after {MAX_RETRIES} retries: {error}"
        )
    
    records = result or []
    
    # Handle empty data scenario (Requirement 4.5, 5.5)
    # Empty output is valid - no anomalies detected
    if not records:
        logger.info(
            "No alerts found in Redis. "
            "This is valid - no anomalies were detected."
        )
        return ValidationResult(
            validator_name=validator.name,
            is_valid=True,
            record_count=0,
            valid_count=0,
            invalid_count=0,
            errors=[],
            field_completeness={},
            message="No alerts to validate (empty output is valid)",
        )
    
    # Validate records
    validation_result = validator.validate(records)
    
    # Log results
    if validation_result.is_valid:
        logger.info(
            f"Anomaly validation passed: {validation_result.record_count} alerts validated. "
            f"Field completeness: {validation_result.field_completeness}"
        )
    else:
        logger.error(f"Anomaly validation failed: {validation_result.message}")
        raise ValueError(validation_result.message)
    
    return validation_result

