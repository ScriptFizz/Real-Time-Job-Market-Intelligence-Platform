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

# def read_bronze(
    # spark: SparkSession,
    # input_path: str | Path
# ) -> DataFrame:
    
    # if not Path(input_path).exists():
        # raise FileNotFoundError(f"{input_path} not found.")
    # return (
        # spark.read
        # .json(input_path)
    # )


# def normalize_jobs(df: DataFrame) -> DataFrame:
    # df = df.withColumn("source", col("ingestion_metadata.source"))
    # df = df.withColumn(
        # "job_id",
        # when(col("source") == "usajobs", col("payload.PositionID"))
        # .when(col("source") == "adzuna", col("payload.id"))
    # )
    
    # df = df.withColumn(
        # "job_title_raw",
        # when(col("source") == "usajobs", col("payload.PositionTitle"))
        # .when(col("source") == "adzuna", col("payload.title"))
    # )
    
    # df = df.withColumn(
        # "location_raw",
        # when(col("source") == "usajobs", col("payload.PositionLocationDisplay"))
        # .when(col("source") == "adzuna", col("payload.location.display_name"))
    # )
    
    # df = df.withColumn(
        # "description_raw",
        # when(col("source") == "usajobs", col("payload.JobSummary"))
        # .when(col("source") == "adzuna", col("payload.description"))
    # )
    
    # df = df.withColumn(
        # "url",
        # when(col("source") == "usajobs", col("payload.PositionURI"))
        # .when(col("source") == "adzuna", col("payload.redirect_url"))
    # )
    
    # df = df.withColumn(
        # "scraped_at",
        # col("ingestion_metadata.ingestion_ts")
    # )
    
    # return df

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
            "ingested_at",
            to_timestamp(col("ingested_at"), "yyyy-MM-dd'T'HH:mm:ssXXX")
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
            col("ingested_at"),
            col("ingestion_date")
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
    
    # If the same job appears multiple times, keep the most recent post
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
    
    # before_count = df.count()
    # after_count = df_dedup.count()
    
    # logger.info(
        # "silver_deduplication_completed",
        # extra={
            # "before_count": before_count,
            # "after_count": after_count,
            # "dropped": before_count - after_count,
        # },
    # )
    
    return df_dedup



def robust_deduplicate_jobs(df: DataFrame) -> DataFrame:

    fallback_key = sha2(
        concat_ws(
                    "||",
                    lower(coalesce(col("source"), lit(""))),
                    lower(coalesce(col("job_title"), lit(""))),
                    lower(coalesce(col("company"), lit("")),
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


##########

# def robust_deduplicate_jobs(df: DataFrame) -> DataFrame:

    # df_with_fallback_id = df.withColumn(
        # "dedup_key",
        # when(
            # col("job_id").isNotNull(),
            # concat_ws("||", col("source"), col("job_id"))
        # ).otherwise(
            # sha2(
                # concat_ws(
                    # "||",
                    # coalesce(col("source"), lit("")),
                    # coalesce(col("job_title"), lit("")),
                    # coalesce(col("company"), lit("")),
                    # coalesce(col("location"), lit(""))
                # ),
                # 256
            # )
        # )
    # )

    # window_spec = (
        # Window
        # .partitionBy("dedup_key")
        # .orderBy(desc("ingested_at"))
    # )

    # df_ranked = df_with_fallback_id.withColumn(
        # "row_num",
        # row_number().over(window_spec)
    # )

    # return (
        # df_ranked
        # .filter(col("row_num") == 1)
        # .drop("row_num", "dedup_key")
    # )

#########

# def write_silver(
    # df: DataFrame,
    # output_path: str | Path
# ) -> None:
    # (
        # df.write
        # .mode("overwrite")
        # .parquet(output_path)
    # )



# def deduplicate_jobs(df: DataFrame) -> DataFrame:
    # """
    # Deduplicate job postings using a deterministic hash.

    # Args:
        # df (DataFrame): Cleaned Spark DataFrame (Silver candidate).

    # Returns:
        # DataFrame: Deduplicated Spark DataFrame (Silver layer).
    # """
    # df_with_hash = df.withColumn(
        # "job_hash",
        # sha2(
            # concat_ws(
                # "||",
                # coalesce(col("source"), lit("")),
                # coalesce(col("job_title"), lit("")),
                # coalesce(col("company"), lit("")),
                # coalesce(col("location"), lit(""))
            # ),
            # 256
        # )
    # )
    
    # return df_with_hash.dropDuplicates(["job_hash"]).drop("job_hash")
