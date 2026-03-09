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
            col("js.ingestion_date")
        )
    )



# def build_fact_job_skills(
    # job_skills_silver_df: DataFrame,
    # dim_skills_df: DataFrame,
# ) -> None:

    # return (
        # job_skills_silver_df
        # .join(dim_skills_df, "skills")
        # .select(
            # "job_id",
            # "skill_id",
            # "skill_confidence",
            # "processed_at",
            # "ingestion_date"
        # )
    # )


# def build_fact_job_skills(
    # job_skills_silver_path: str | Path,
    # dim_skills_path: str | Path,
    # output_path: str | Path
# ) -> None:

    # spark = (
        # SparkSession.builder
        # .appName("build-fact-job-skills")
        # .getOrCreate()
    # )
    
    # skills_df = spark.read.parquet(job_skills_silver_path)
    # dim_df = spark.read.parquet(dim_skills_path)
    
    # fact_df = (
        # skills_df
        # .join(dim_df, skills_df.skill == dim_df.skill)
        # .select(
            # "job_id",
            # "skill_id",
            # "skill_confidence",
            # "processed_at"
        # )
    # )
    
    # fact_df.write.mode("overwrite").parquet(output_path)
