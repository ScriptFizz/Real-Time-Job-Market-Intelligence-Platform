from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from pyspark.sql.functions import (
    monotonically_increasing_id,
    to_date,
    sha2,
    col,
    length,
    first,
    min
)


def build_dim_jobs(
    job_silver_df: DataFrame
) -> DataFrame:
    return (
        job_silver_df
        .groupBy("job_id")
        .agg(
            first("source").alias("source"),
            first("job_title").alias("job_title"),
            first("company").alias("company"),
            first("location").alias("location"),
            first("ingestion_date").alias("ingestion_date"),
            first("posted_at").alias("posted_at"),
            first(length("description")).alias("description_length")
        )
    )
    
    
def build_dim_skills(
    job_skills_silver_df: DataFrame
) -> DataFrame:

    return (
        job_skills_silver_df
        .groupBy("skills")
        .agg(min("ingestion_date").alias("ingestion_date"))
        .withColumn("skill_id", sha2(col("skills"), 256))
    )

