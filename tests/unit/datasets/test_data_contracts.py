from datetime import UTC, datetime

import pytest

from job_plat.pipeline.datasets.data_contracts import (
    DataContractError,
    collect_dataset_quality_metrics,
    validate_dataset_contract,
)


def test_contract_rejects_missing_columns(spark):
    dataframe = spark.createDataFrame([("job-1",)], ["job_id"])

    with pytest.raises(DataContractError, match="missing required columns"):
        validate_dataset_contract(
            dataframe,
            dataset_name="jobs",
            required_columns=("job_id", "title"),
            expected_types={},
            non_null_columns=(),
            unique_keys=(),
        )


def test_contract_enforces_types_nullability_and_uniqueness(spark):
    wrong_type = spark.createDataFrame([(1,)], ["job_id"])
    with pytest.raises(DataContractError, match="incompatible column types"):
        validate_dataset_contract(
            wrong_type,
            dataset_name="jobs",
            required_columns=("job_id",),
            expected_types={"job_id": "string"},
            non_null_columns=(),
            unique_keys=(),
        )

    nullable = spark.createDataFrame(
        [("job-1",), (None,)],
        "job_id string",
    )
    with pytest.raises(DataContractError, match="nullability"):
        validate_dataset_contract(
            nullable,
            dataset_name="jobs",
            required_columns=("job_id",),
            expected_types={"job_id": "string"},
            non_null_columns=("job_id",),
            unique_keys=(),
        )

    duplicates = spark.createDataFrame([("job-1",), ("job-1",)], ["job_id"])
    with pytest.raises(DataContractError, match="uniqueness"):
        validate_dataset_contract(
            duplicates,
            dataset_name="jobs",
            required_columns=("job_id",),
            expected_types={"job_id": "string"},
            non_null_columns=("job_id",),
            unique_keys=("job_id",),
        )


def test_quality_metrics_report_completeness_and_freshness(spark):
    reference = datetime(2025, 3, 2, 12, tzinfo=UTC)
    dataframe = spark.createDataFrame(
        [
            ("job-1", datetime(2025, 3, 2, 10)),
            (None, datetime(2025, 3, 2, 9)),
        ],
        "job_id string, generated_at timestamp",
    )

    metrics = collect_dataset_quality_metrics(
        dataframe,
        non_null_columns=("job_id",),
        freshness_column="generated_at",
        reference_time=reference,
    )

    assert metrics.row_count == 2
    assert metrics.completeness == {"job_id": 0.5}
    assert metrics.freshness_lag_hours == 2.0
