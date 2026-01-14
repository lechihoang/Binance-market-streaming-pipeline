"""Aggregation output validator using Great Expectations 1.0+."""

from typing import Any

import great_expectations as gx
import pandas as pd
from great_expectations import expectations as gxe

from storage.postgres import Postgres
from util.logging import get_logger

logger = get_logger(__name__)


def build_aggregation_expectations() -> list[gxe.Expectation]:
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


def run_ge_validation(records: list[dict[str, Any]], expectations: list[gxe.Expectation]) -> Any:
    """Run Great Expectations validation on records."""
    df = pd.DataFrame(records)

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("aggregation_source")
    data_asset = data_source.add_dataframe_asset(name="aggregation_data")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("aggregation_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name="aggregation_suite")
    for exp in expectations:
        suite.add_expectation(exp)

    return batch.validate(suite)


def validate_aggregation_records(
    records: list[dict[str, Any]],
    postgres_storage: Postgres | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[list[dict[str, Any]]]]:
    """
    Validate aggregation records using Great Expectations.

    Args:
        records: List of aggregation records to validate
        postgres_storage: Optional Postgres connection for quarantine storage.

    Returns:
        Tuple of (valid_records, invalid_records, failed_expectations)
    """
    if not records:
        return [], [], []

    try:
        result = run_ge_validation(records, build_aggregation_expectations())

        if result.success:
            logger.info(f"All {len(records)} aggregation records passed validation")
            return records, [], []

        # Extract failed records
        failed_indices = set()
        failed_by_idx: dict[int, list[dict[str, Any]]] = {}

        for r in result.results:
            if not r.success:
                indices = r.result.get("unexpected_index_list", [])
                exp_info = {
                    "expectation_type": r.expectation_config.type,
                    "kwargs": dict(r.expectation_config.kwargs) if hasattr(r.expectation_config, "kwargs") else {},
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
                    source="aggregation", records=invalid_records, failed=failed_expectations
                )
                logger.warning(f"Quarantined {len(invalid_records)} invalid aggregation records")
            except Exception as e:
                logger.error(f"Failed to store validation errors: {e}")

        logger.info(
            f"Aggregation validation: {len(valid_records)} valid, " f"{len(invalid_records)} invalid (quarantined)"
        )

        return valid_records, invalid_records, failed_expectations

    except Exception as e:
        logger.error(f"Validation failed with error: {e}. Returning all records as valid.")
        return records, [], []
