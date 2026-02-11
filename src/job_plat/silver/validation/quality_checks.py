from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct
from datetime import date

def check_row_count(
    df: DataFrame,
    min_rows: int = 1
) -> None:
    
    row_count = df.count()
    if row_count < min_rows:
        raise ValueError(f"Row count too low: {row_count}")

def check_required_fields(
    df: DataFrame,
    fields: list[str]
) -> None:
    
    for field in fields:
        nulls = df.filter(col(field).isNull()).count()
        if nulls > 0:
            raise ValueError(f"Field {field} has {nulls} null values")


def check_uniqueness(
    df: DataFrame,
    field: str
) -> None:
    
    total = df.count()
    distinct = df.select(field).distinct().count()
    
    if total != distinct:
        raise ValueError(f"Duplicate values found in {field}")


def check_freshness(
    df: DataFrame,
    max_hours: int = 24
) -> None:
    
    max_seconds = max_hours * 3600
    old_hours = df.filter( unix_timestamp() - unix_timestap(col("scraped_at"))  
        > max_seconds
        ).count()
    if old_hours > 0:
        raise ValueError(f"{old_hours} rows are older than {max_hours} hours"
    
def run_quality_checks(df: DataFrame) -> None:
    
    check_row_count(df)
    check_required_fields(
        df,
        ["job_title", "description", "company"]
    )
