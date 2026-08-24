from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, functions as F


class DataContractError(ValueError):
    pass


@dataclass(frozen=True)
class DatasetQualityMetrics:
    row_count: int
    completeness: dict[str, float]
    freshness_lag_hours: float | None
    maximum_freshness_value: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "row_count": self.row_count,
            "completeness": self.completeness,
            "freshness_lag_hours": self.freshness_lag_hours,
            "maximum_freshness_value": self.maximum_freshness_value,
        }


def validate_dataset_contract(
    dataframe: DataFrame,
    *,
    dataset_name: str,
    required_columns: tuple[str, ...],
    expected_types: dict[str, str],
    non_null_columns: tuple[str, ...],
    unique_keys: tuple[str, ...],
) -> None:
    missing = sorted(set(required_columns) - set(dataframe.columns))
    if missing:
        raise DataContractError(
            f"Dataset {dataset_name} is missing required columns: {missing}"
        )

    actual_types = {
        field.name: field.dataType.simpleString() for field in dataframe.schema
    }
    type_errors = {
        name: {"expected": expected, "actual": actual_types.get(name)}
        for name, expected in expected_types.items()
        if actual_types.get(name) != expected
    }
    if type_errors:
        raise DataContractError(
            f"Dataset {dataset_name} has incompatible column types: {type_errors}"
        )

    if non_null_columns:
        null_counts = dataframe.agg(
            *[
                F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
                for name in non_null_columns
            ]
        ).first()
        violations = {
            name: int(null_counts[name] or 0)
            for name in non_null_columns
            if null_counts is not None and int(null_counts[name] or 0) > 0
        }
        if violations:
            raise DataContractError(
                f"Dataset {dataset_name} violates nullability: {violations}"
            )

    if unique_keys:
        duplicates_exist = (
            dataframe.groupBy(*unique_keys)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
            > 0
        )
        if duplicates_exist:
            raise DataContractError(
                f"Dataset {dataset_name} violates uniqueness key {unique_keys}"
            )


def collect_dataset_quality_metrics(
    dataframe: DataFrame,
    *,
    non_null_columns: tuple[str, ...],
    freshness_column: str | None,
    reference_time: datetime | None,
) -> DatasetQualityMetrics:
    aggregations = [F.count("*").alias("row_count")]
    aggregations.extend(
        F.sum(F.when(F.col(name).isNotNull(), 1).otherwise(0)).alias(
            f"complete__{name}"
        )
        for name in non_null_columns
    )
    if freshness_column is not None:
        aggregations.append(F.max(F.col(freshness_column)).alias("freshness_max"))

    row = dataframe.agg(*aggregations).first()
    row_count = int(row["row_count"]) if row is not None else 0
    completeness = {
        name: (
            round(float(row[f"complete__{name}"]) / row_count, 6)
            if row is not None and row_count
            else 1.0
        )
        for name in non_null_columns
    }

    freshness_value = (
        row["freshness_max"] if row is not None and freshness_column else None
    )
    freshness_text = str(freshness_value) if freshness_value is not None else None
    freshness_lag_hours: float | None = None
    if isinstance(freshness_value, datetime) and reference_time is not None:
        normalized = freshness_value
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=UTC)
        freshness_lag_hours = round(
            max(
                0.0,
                (
                    reference_time.astimezone(UTC) - normalized.astimezone(UTC)
                ).total_seconds()
                / 3600,
            ),
            3,
        )

    return DatasetQualityMetrics(
        row_count=row_count,
        completeness=completeness,
        freshness_lag_hours=freshness_lag_hours,
        maximum_freshness_value=freshness_text,
    )
