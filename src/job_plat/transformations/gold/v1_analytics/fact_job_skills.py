from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pathlib import Path



def build_fact_job_skills(
    job_skills_silver_df: DataFrame,
    dim_skills_df: DataFrame,
) -> DataFrame:

    js = job_skills_silver_df.alias("js")
    ds = dim_skills_df.alias("ds")

    return (
        js
        .join(ds, "skills")
        .select(
            col("js.job_id"),
            col("ds.skill_id"),
            col("js.skill_confidence"),
            col("js.processed_at"),
            col("js.ingestion_date"),
            col("js.posted_at")
        )
    )


