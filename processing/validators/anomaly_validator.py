"""Anomaly/Alert output validator using Great Expectations 1.0+."""

from typing import Any, Dict, List, Optional, Tuple

import great_expectations as gx
from great_expectations import expectations as gxe
import pandas as pd

from storage.postgres import Postgres
from util.logging import get_logger

logger = get_logger(__name__)


VALID_ALERT_TYPES = [
    "VOLUME_SPIKE",
    "PRICE_SPIKE",
    "TRADE_COUNT_SPIKE",
    "BUY_SELL_IMBALANCE",
]

VALID_ALERT_LEVELS = ["HIGH", "MEDIUM", "LOW"]


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


def run_ge_validation(
    records: List[Dict[str, Any]],
    expectations: List[gxe.Expectation]
) -> Any:
    """Run Great Expectations validation on records."""
    df = pd.DataFrame(records)
    
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("anomaly_source")
    data_asset = data_source.add_dataframe_asset(name="anomaly_data")
    
    batch_definition = data_asset.add_batch_definition_whole_dataframe("anomaly_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    suite = gx.ExpectationSuite(name="anomaly_suite")
    for exp in expectations:
        suite.add_expectation(exp)
    
    return batch.validate(suite)


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
        result = run_ge_validation(records, build_anomaly_expectations())
        
        if result.success:
            logger.info(f"All {len(records)} alert records passed validation")
            return records, [], []
        
        # Extract failed records
        failed_indices = set()
        failed_by_idx: Dict[int, List[Dict[str, Any]]] = {}
        
        for r in result.results:
            if not r.success:
                indices = r.result.get("unexpected_index_list", [])
                exp_info = {
                    "expectation_type": r.expectation_config.type,
                    "kwargs": dict(r.expectation_config.kwargs) if hasattr(r.expectation_config, 'kwargs') else {},
                }
                for idx in (indices if indices else range(len(records))):
                    failed_indices.add(idx)
                    failed_by_idx.setdefault(idx, []).append(exp_info)
        
        invalid_records = [records[i] for i in sorted(failed_indices)]
        failed_expectations = [failed_by_idx.get(i, []) for i in sorted(failed_indices)]
        valid_records = [r for i, r in enumerate(records) if i not in failed_indices]
        
        # Store invalid records to quarantine
        if postgres_storage and invalid_records:
            try:
                postgres_storage.write_validation_errors(
                    source="anomaly",
                    records=invalid_records,
                    failed=failed_expectations
                )
                logger.warning(f"Quarantined {len(invalid_records)} invalid alert records")
            except Exception as e:
                logger.error(f"Failed to store validation errors: {e}")
        
        logger.info(
            f"Alert validation: {len(valid_records)} valid, "
            f"{len(invalid_records)} invalid (quarantined)"
        )
        
        return valid_records, invalid_records, failed_expectations
        
    except Exception as e:
        logger.error(f"Validation failed with error: {e}. Returning all records as valid.")
        return records, [], []
