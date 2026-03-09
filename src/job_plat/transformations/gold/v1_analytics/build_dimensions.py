from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from pyspark.sql.functions import (
    monotonically_increasing_id,
    to_date,
    sha2,
    col,
    length,
    min
)


def build_dim_jobs(
    job_silver_df: DataFrame
) -> DataFrame:
    return (
        job_silver_df
        #.withColumn("ingested_date", to_date(col("ingested_at")))
        .withColumn("description_length", length(col("description")))
        .select(
            "job_id",
            "source",
            col("job_title").alias("job_title_clean"),
            "company",
            "location",
            "ingestion_date",
            "description_length"
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


###################### 08-03

# def build_dim_skills(
    # job_skills_silver_df: DataFrame
# ) -> DataFrame:

    # return (
        # job_skills_silver_df
        # .select("skills")
        # .distinct()
        # .withColumn("skill_id", sha2(col("skills"), 256))
    # )    
    



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
