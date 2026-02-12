from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from pyspark.sql.functions import (
    monotonically_increasing_id,
    to_date,
    col,
    length
)


def build_dim_jobs(
    job_silver_df: DataFrame
) -> DataFrame:
    return (
        job_silver_df
        .withColumn("scraped_date", to_date(col("scraped_at")))
        .withColumn("description_length", length(col("description")))
        .select(
            "job_id",
            "source",
            col("job_title").alias("job_title_clean"),
            "company",
            "location",
            "scraped_date",
            "description_length"
        )
    )
    
    
def build_dim_skills(
    job_skills_silver_df: DataFrame
) -> DataFrame:

    return (
        job_skills_silver_df
        .select("skill")
        .distinct()
        .withColumn("skill_id", monotonically_increasing_id())
    )
    
    



# def build_dim_skills(
    # job_skills_silver_path: str | Path,
    # output_path: str | Path
# ) -> None:

    # spark = (
        # SparkSession.builder
        # .appName("build-dim-skills")
        # .getOrCreate()
    # )
    
    # df = spark.read.parquet(job_skills_silver_path)
    
    # dim_skills = (
        # df
        # .select("skill")
        # .distinct()
        # .withColumn("skill_id", monotonically_increasing_id())
    # )
    
    # dim_skills.write.mode("overwrite").parquet(output_path)
