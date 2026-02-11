from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_replace,
    to_timestamp,
    lit,
    sha2,
    concat_ws
)
from pyspark.sql.types import StringType
from pathlib import Path

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("clean-jobs-silver")
        .master("local[*]")
        .getOrCreate()
    )

def read_bronze(
    spark: SparkSession,
    input_path: str | Path
) -> DataFrame:
    
    if not Path(input_path).exists():
        raise FileNotFoundError(f"{input_path} not found.")
    return (
        spark.read
        .json(input_path)
    )


def clean_jobs(df: DataFrame) -> DataFrame:
    """
    Clean and normalize Bronze job data into a Silver-layer schema.
    
    Args:
      df (DataFrame): Spark DataFrame of the Bronze layer data.
    
    Returns:
      (DataFrame): Cleaned and normalized Spark DataFrame (Silver candidate).
    """
    return (
        df
        # Drop broken records
        .filter(col("job_title_raw").isNotNull())
        .filter(col("description_raw").isNotNull())
        .filter(trim(col("job_title_raw")) != "")
        .filter(trim(col("description_raw")) != "")
        
        # Normalize text
        .withColumn("job_title", lower(trim(col("job_title_raw"))))
        .withColumn("company", trim(col("company_raw")))
        .withColumn("location", trim(col("location_raw")))
        
        # Clean description text
        .withColumn(
            "description",
            regexp_replace(col("description_raw"), r"\s+", " ")
        )
        
        # Standardize timestamp
        .withColumn(
            "scraped_at",
            to_timestamp(col("scraped_at"), "yyyy-MM-dd'T'HH:mm:ssXXX")
        )
        
        # Unified schema
        .select(
            col("source"),
            col("job_id"),
            col("job_title"),
            col("company"),
            col("location"),
            col("description"),
            col("url"),
            col("scraped_at")
        )
    )

def deduplicate_jobs(df: DataFrame) -> DataFrame:
    """
    Deduplicate job postings using a deterministic hash.

    Args:
        df (DataFrame): Cleaned Spark DataFrame (Silver candidate).

    Returns:
        DataFrame: Deduplicated Spark DataFrame (Silver layer).
    """
    df_with_hash = df.withColumn(
        "job_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("source"), lit("")),
                coalesce(col("job_title"), lit("")),
                coalesce(col("company"), lit("")),
                coalesce(col("location"), lit(""))
            ),
            256
        )
    )
    
    return df_with_hash.dropDuplicates(["job_hash"]).drop("job_hash")
    

def write_silver(
    df: DataFrame,
    output_path: str | Path
) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
