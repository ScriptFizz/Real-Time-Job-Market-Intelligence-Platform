from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    trim,
    lower,
    regexp_replace,
    to_timestamp,
    lit,
    sha2,
    concat_ws,
    row_number,
    coalesce,
    desc
)
from pyspark.sql.types import StringType
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


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
            lower(trim(regexp_replace(col("description_raw"), r"\s+", " ")))
        )
        
        # Standardize timestamp
        .withColumn(
            "ingested_at",
            to_timestamp(col("ingested_at"), "yyyy-MM-dd'T'HH:mm:ssXXX")
        )
        .withColumn(
            "posted_at",
            to_timestamp(col("posted_at_raw"), "yyyy-MM-dd'T'HH:mm:ssXXX")
        )
        # Unified schema
        .select(
            col("source"),
            col("source_job_id").alias("job_id"),
            col("job_title"),
            col("company"),
            col("location"),
            col("description"),
            col("url"),
            col("ingested_at"),
            col("ingestion_date"),
            col("posted_at")
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
    df_valid = df.filter(col("job_id").isNotNull())
    
    # If the same job appears multiple times, keep the most recent ingestion
    window_spec = (
        Window
        .partitionBy("source", "job_id")
        .orderBy(desc("ingested_at"))
    )
    
    df_ranked = df_valid.withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    
    df_dedup = (
        df_ranked
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    return df_dedup



def robust_deduplicate_jobs(df: DataFrame) -> DataFrame:

    fallback_key = sha2(
        concat_ws(
                    "||",
                    lower(coalesce(col("source"), lit(""))),
                    lower(coalesce(col("job_title"), lit(""))),
                    lower(coalesce(col("company"), lit(""))),
                    lower(coalesce(col("location"), lit(""))),
                    lower(coalesce(col("url"), lit("")))
                    
                ),
                256
            )
    
    dedup_key = sha2(
        concat_ws(
            "||",
            lower(coalesce(col("source"), lit(""))),
            coalesce(col("job_id"), fallback_key)
        ),
        256
    )
    
    df_with_key = df.withColumn("dedup_key", dedup_key)

    window_spec = (
        Window
        .partitionBy("dedup_key")
        .orderBy(desc("ingested_at"))
    )

    df_ranked = df_with_key.withColumn(
        "row_num",
        row_number().over(window_spec)
    )

    return (
        df_ranked
        .filter(col("row_num") == 1)
        .drop("row_num", "dedup_key")
    )


