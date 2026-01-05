"""Aggregation output validator using Great Expectations 1.0+."""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import great_expectations as gx
from great_expectations import expectations as gxe
import pandas as pd

from storage.postgres import Postgres
from storage.redis import Redis
from util.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    record_count: int
    valid_count: int
    invalid_count: int
    failed_expectations: List[Dict[str, Any]] = field(default_factory=list)
    message: str = ""


def build_aggregation_expectations() -> List[gxe.Expectation]:
    """Build list of expectations for aggregation data."""
    return [
        # Column existence
        gxe.ExpectColumnToExist(column="symbol"),
        gxe.ExpectColumnToExist(column="open"),
        gxe.ExpectColumnToExist(column="high"),
        gxe.ExpectColumnToExist(column="low"),
        gxe.ExpectColumnToExist(column="close"),
        gxe.ExpectColumnToExist(column="volume"),
        # Not null
        gxe.ExpectColumnValuesToNotBeNull(column="symbol"),
        gxe.ExpectColumnValuesToNotBeNull(column="open"),
        gxe.ExpectColumnValuesToNotBeNull(column="high"),
        gxe.ExpectColumnValuesToNotBeNull(column="low"),
        gxe.ExpectColumnValuesToNotBeNull(column="close"),
        gxe.ExpectColumnValuesToNotBeNull(column="volume"),
        # Value ranges
        gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="open", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="high", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="low", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="close", min_value=0),
        # OHLC relationships: high >= low, high >= open, high >= close
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B="low", or_equal=True, ignore_row_if="either_value_is_missing"
        ),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B="open", or_equal=True, ignore_row_if="either_value_is_missing"
        ),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B="close", or_equal=True, ignore_row_if="either_value_is_missing"
        ),
    ]


def fetch_aggregations_from_redis(
    redis_storage: Redis,
    symbols: List[str],
    interval: str = "1m"
) -> List[Dict[str, Any]]:
    records = []
    for symbol in symbols:
        data = redis_storage.get_agg(symbol, interval)
        if data:
            record = {
                "symbol": symbol,
                "window_duration": interval,
                **data,
            }
            if "timestamp" in record:
                record["window_start"] = record.get("timestamp")
                record["window_end"] = record.get("timestamp")
            records.append(record)
    return records


def run_ge_validation(
    records: List[Dict[str, Any]],
    expectations: List[gxe.Expectation]
) -> Any:
    df = pd.DataFrame(records)
    
    context = gx.get_context(mode="ephemeral")
    
    data_source = context.data_sources.add_pandas("aggregation_source")
    data_asset = data_source.add_dataframe_asset(name="aggregation_data")
    
    batch_definition = data_asset.add_batch_definition_whole_dataframe("aggregation_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    suite = gx.ExpectationSuite(name="aggregation_suite")
    for exp in expectations:
        suite.add_expectation(exp)
    
    validation_result = batch.validate(suite)
    
    return validation_result


def extract_failed_records(
    records: List[Dict[str, Any]],
    validation_result: Any
) -> tuple[List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
    failed_indices = set()
    failed_expectations_by_index: Dict[int, List[Dict[str, Any]]] = {}
    
    for result in validation_result.results:
        if not result.success:
            unexpected_indices = result.result.get("unexpected_index_list", [])
            expectation_info = {
                "expectation_type": result.expectation_config.type,
                "kwargs": dict(result.expectation_config.kwargs) if hasattr(result.expectation_config, 'kwargs') else {},
            }
            
            if unexpected_indices:
                for idx in unexpected_indices:
                    failed_indices.add(idx)
                    if idx not in failed_expectations_by_index:
                        failed_expectations_by_index[idx] = []
                    failed_expectations_by_index[idx].append(expectation_info)
            elif not result.success:
                for idx in range(len(records)):
                    failed_indices.add(idx)
                    if idx not in failed_expectations_by_index:
                        failed_expectations_by_index[idx] = []
                    failed_expectations_by_index[idx].append(expectation_info)
    
    invalid_records = [records[i] for i in sorted(failed_indices)]
    failed_expectations = [failed_expectations_by_index.get(i, []) for i in sorted(failed_indices)]
    
    return invalid_records, failed_expectations


def store_invalid_records(
    postgres_storage: Postgres,
    invalid_records: List[Dict[str, Any]],
    failed_expectations: List[List[Dict[str, Any]]]
) -> None:
    if not invalid_records:
        return
    
    try:
        postgres_storage.write_validation_errors(
            source="aggregation",
            records=invalid_records,
            failed=failed_expectations
        )
        logger.warning(
            f"Stored {len(invalid_records)} invalid aggregation records to validation_errors table"
        )
    except Exception as e:
        logger.error(f"Failed to store validation errors: {e}")


def validate_aggregation_records(
    records: List[Dict[str, Any]],
    postgres_storage: Optional[Postgres] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
    """
    Validate aggregation records using Great Expectations.
    
    This function is designed to be called from within Spark jobs to validate
    records before writing them to storage.
    
    Args:
        records: List of aggregation records to validate
        postgres_storage: Optional Postgres connection for quarantine storage.
                         If provided, invalid records will be stored to validation_errors table.
        
    Returns:
        Tuple of (valid_records, invalid_records, failed_expectations)
    """
    if not records:
        return [], [], []
    
    try:
        expectations = build_aggregation_expectations()
        validation_result = run_ge_validation(records, expectations)
        
        if validation_result.success:
            logger.info(f"All {len(records)} aggregation records passed validation")
            return records, [], []
        
        # Extract failed records
        invalid_records, failed_expectations = extract_failed_records(records, validation_result)
        
        # Get valid records by index
        failed_indices = set()
        for i, rec in enumerate(records):
            for inv in invalid_records:
                if rec is inv:
                    failed_indices.add(i)
                    break
        valid_records = [rec for i, rec in enumerate(records) if i not in failed_indices]
        
        # Quarantine invalid records if postgres provided
        if postgres_storage and invalid_records:
            store_invalid_records(postgres_storage, invalid_records, failed_expectations)
        
        logger.info(
            f"Aggregation validation: {len(valid_records)} valid, "
            f"{len(invalid_records)} invalid (quarantined)"
        )
        
        return valid_records, invalid_records, failed_expectations
        
    except Exception as e:
        logger.error(f"Validation failed with error: {e}. Returning all records as valid.")
        return records, [], []


def validate_aggregation_output(
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    postgres_host: Optional[str] = None,
    postgres_port: Optional[int] = None,
    postgres_user: Optional[str] = None,
    postgres_password: Optional[str] = None,
    postgres_db: Optional[str] = None,
    symbols: Optional[List[str]] = None,
    interval: str = "1m",
    **kwargs,
) -> ValidationResult:
    redis_host = redis_host or os.getenv("REDIS_HOST", "redis")
    redis_port = redis_port or int(os.getenv("REDIS_PORT", "6379"))
    postgres_host = postgres_host or os.getenv("POSTGRES_HOST", "postgres-data")
    postgres_port = postgres_port or int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user = postgres_user or os.getenv("POSTGRES_USER", "crypto")
    postgres_password = postgres_password or os.getenv("POSTGRES_PASSWORD", "crypto")
    postgres_db = postgres_db or os.getenv("POSTGRES_DB", "crypto_data")
    
    if symbols is None:
        logger.warning("No symbols provided for validation. Validation now happens in Spark jobs.")
        return ValidationResult(
            is_valid=True,
            record_count=0,
            valid_count=0,
            invalid_count=0,
            message="No symbols provided. Validation now happens in-job via validate_aggregation_records().",
        )
    
    redis_storage = Redis(host=redis_host, port=redis_port)
    postgres_storage = Postgres(
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password,
        database=postgres_db,
    )
    
    try:
        records = fetch_aggregations_from_redis(redis_storage, symbols, interval)
        
        if not records:
            logger.info("No aggregation data found in Redis (empty batches are valid)")
            return ValidationResult(
                is_valid=True,
                record_count=0,
                valid_count=0,
                invalid_count=0,
                message="No aggregation data found in Redis (empty batches are valid)",
            )
        
        expectations = build_aggregation_expectations()
        validation_result = run_ge_validation(records, expectations)
        
        is_valid = validation_result.success
        record_count = len(records)
        
        if is_valid:
            logger.info(f"Aggregation validation passed: {record_count} records validated")
            return ValidationResult(
                is_valid=True,
                record_count=record_count,
                valid_count=record_count,
                invalid_count=0,
                message=f"Validated {record_count} aggregation records successfully",
            )
        
        invalid_records, failed_expectations = extract_failed_records(records, validation_result)
        invalid_count = len(invalid_records)
        valid_count = record_count - invalid_count
        
        store_invalid_records(postgres_storage, invalid_records, failed_expectations)
        
        failed_exp_summary = []
        for result in validation_result.results:
            if not result.success:
                failed_exp_summary.append({
                    "expectation_type": result.expectation_config.type,
                    "kwargs": dict(result.expectation_config.kwargs) if hasattr(result.expectation_config, 'kwargs') else {},
                })
        
        logger.warning(
            f"Aggregation validation: {invalid_count}/{record_count} records invalid. "
            f"Invalid records stored to validation_errors table. DAG continues."
        )
        
        return ValidationResult(
            is_valid=False,
            record_count=record_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            failed_expectations=failed_exp_summary,
            message=f"Validation completed: {invalid_count} invalid records stored to validation_errors",
        )
        
    finally:
        redis_storage.close()
        postgres_storage.close()
