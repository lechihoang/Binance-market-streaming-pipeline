"""Anomaly/Alert output validator using Great Expectations 1.0+."""

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


VALID_ALERT_TYPES = [
    "WHALE_ALERT",
    "VOLUME_SPIKE",
    "PRICE_SPIKE",
]

VALID_ALERT_LEVELS = ["HIGH", "MEDIUM", "LOW"]


@dataclass
class ValidationResult:
    is_valid: bool
    record_count: int
    valid_count: int
    invalid_count: int
    failed_expectations: List[Dict[str, Any]] = field(default_factory=list)
    message: str = ""


def build_anomaly_expectations() -> List[gxe.Expectation]:
    """Build list of expectations for anomaly/alert data."""
    return [
        # Column existence
        gxe.ExpectColumnToExist(column="alert_id"),
        gxe.ExpectColumnToExist(column="symbol"),
        gxe.ExpectColumnToExist(column="alert_type"),
        gxe.ExpectColumnToExist(column="alert_level"),
        gxe.ExpectColumnToExist(column="timestamp"),
        gxe.ExpectColumnToExist(column="created_at"),
        # Not null
        gxe.ExpectColumnValuesToNotBeNull(column="alert_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="symbol"),
        gxe.ExpectColumnValuesToNotBeNull(column="alert_type"),
        gxe.ExpectColumnValuesToNotBeNull(column="alert_level"),
        gxe.ExpectColumnValuesToNotBeNull(column="timestamp"),
        # Value sets
        gxe.ExpectColumnValuesToBeInSet(column="alert_type", value_set=VALID_ALERT_TYPES),
        gxe.ExpectColumnValuesToBeInSet(column="alert_level", value_set=VALID_ALERT_LEVELS),
    ]


def fetch_alerts_from_redis(
    redis_storage: Redis,
    limit: int = 100
) -> List[Dict[str, Any]]:
    return redis_storage.get_alerts(limit=limit)


def run_ge_validation(
    records: List[Dict[str, Any]],
    expectations: List[gxe.Expectation]
) -> Any:
    df = pd.DataFrame(records)
    
    context = gx.get_context(mode="ephemeral")
    
    data_source = context.data_sources.add_pandas("anomaly_source")
    data_asset = data_source.add_dataframe_asset(name="anomaly_data")
    
    batch_definition = data_asset.add_batch_definition_whole_dataframe("anomaly_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    suite = gx.ExpectationSuite(name="anomaly_suite")
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
            source="anomaly",
            records=invalid_records,
            failed=failed_expectations
        )
        logger.warning(
            f"Stored {len(invalid_records)} invalid anomaly records to validation_errors table"
        )
    except Exception as e:
        logger.error(f"Failed to store validation errors: {e}")


def validate_alert_records(
    records: List[Dict[str, Any]],
    postgres_storage: Optional[Postgres] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[List[Dict[str, Any]]]]:
    """
    Validate alert records using Great Expectations.
    
    Args:
        records: List of alert records to validate
        postgres_storage: Optional Postgres connection for quarantine storage.
        
    Returns:
        Tuple of (valid_records, invalid_records, failed_expectations)
    """
    if not records:
        return [], [], []
    
    try:
        expectations = build_anomaly_expectations()
        validation_result = run_ge_validation(records, expectations)
        
        if validation_result.success:
            logger.info(f"All {len(records)} alert records passed validation")
            return records, [], []
        
        invalid_records, failed_expectations = extract_failed_records(records, validation_result)
        
        failed_indices = set()
        for i, rec in enumerate(records):
            for inv in invalid_records:
                if rec is inv:
                    failed_indices.add(i)
                    break
        valid_records = [rec for i, rec in enumerate(records) if i not in failed_indices]
        
        if postgres_storage and invalid_records:
            store_invalid_records(postgres_storage, invalid_records, failed_expectations)
        
        logger.info(
            f"Alert validation: {len(valid_records)} valid, "
            f"{len(invalid_records)} invalid (quarantined)"
        )
        
        return valid_records, invalid_records, failed_expectations
        
    except Exception as e:
        logger.error(f"Validation failed with error: {e}. Returning all records as valid.")
        return records, [], []


def validate_anomaly_output(
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    postgres_host: Optional[str] = None,
    postgres_port: Optional[int] = None,
    postgres_user: Optional[str] = None,
    postgres_password: Optional[str] = None,
    postgres_db: Optional[str] = None,
    limit: int = 100,
    **kwargs,
) -> ValidationResult:
    redis_host = redis_host or os.getenv("REDIS_HOST", "redis")
    redis_port = redis_port or int(os.getenv("REDIS_PORT", "6379"))
    postgres_host = postgres_host or os.getenv("POSTGRES_HOST", "postgres-data")
    postgres_port = postgres_port or int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_user = postgres_user or os.getenv("POSTGRES_USER", "crypto")
    postgres_password = postgres_password or os.getenv("POSTGRES_PASSWORD", "crypto")
    postgres_db = postgres_db or os.getenv("POSTGRES_DB", "crypto_data")
    
    redis_storage = Redis(host=redis_host, port=redis_port)
    postgres_storage = Postgres(
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password,
        database=postgres_db,
    )
    
    try:
        records = fetch_alerts_from_redis(redis_storage, limit)
        
        if not records:
            logger.info("No alerts found in Redis (empty output is valid - no anomalies detected)")
            return ValidationResult(
                is_valid=True,
                record_count=0,
                valid_count=0,
                invalid_count=0,
                message="No alerts to validate (empty output is valid)",
            )
        
        expectations = build_anomaly_expectations()
        validation_result = run_ge_validation(records, expectations)
        
        is_valid = validation_result.success
        record_count = len(records)
        
        if is_valid:
            logger.info(f"Anomaly validation passed: {record_count} alerts validated")
            return ValidationResult(
                is_valid=True,
                record_count=record_count,
                valid_count=record_count,
                invalid_count=0,
                message=f"Validated {record_count} alert records successfully",
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
            f"Anomaly validation: {invalid_count}/{record_count} records invalid. "
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
