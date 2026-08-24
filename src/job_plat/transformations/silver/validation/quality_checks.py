from pyspark.sql import DataFrame
from pyspark.sql.functions import col, unix_timestamp


def check_row_count(df: DataFrame, min_rows: int = 1) -> None:
    row_count = df.count()
    if row_count < min_rows:
        raise ValueError(f"Row count too low: {row_count}")


def check_required_fields(df: DataFrame, fields: list[str]) -> None:
    for field in fields:
        nulls = df.filter(col(field).isNull()).count()
        if nulls > 0:
            raise ValueError(f"Field {field} has {nulls} null values")


def check_uniqueness(df: DataFrame, field: str) -> None:
    total = df.count()
    distinct = df.select(field).distinct().count()

    if total != distinct:
        raise ValueError(f"Duplicate values found in {field}")


def check_freshness(df: DataFrame, max_hours: int = 24) -> None:
    if max_hours <= 0:
        raise ValueError("max_hours must be greater than zero")

    max_seconds = max_hours * 3600
    old_rows = df.filter(
        unix_timestamp() - unix_timestamp(col("scraped_at")) > max_seconds
    ).count()
    if old_rows > 0:
        raise ValueError(f"{old_rows} rows are older than {max_hours} hours")


def run_quality_checks(df: DataFrame) -> None:
    check_row_count(df)
    check_required_fields(df, ["job_title", "description", "company"])
