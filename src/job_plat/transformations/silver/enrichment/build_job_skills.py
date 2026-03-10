from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lower, split, lit, current_timestamp, explode, element_at, coalesce
)

#from job_plat.utils.helpers import create_spark
from job_plat.transformations.silver.enrichment.spark_ops import (
    extract_skills_udf,
    normalize_skills_udf,
    skill_confidence_udf,
)
from pathlib import Path

def run_job_skills(
    jobs_silver_df: DataFrame
) -> DataFrame:
    """
    Extract skills from Silver job data and write them into the Gold_v1 layer (job_skill_silver).
    
    Args:
        job_silver_path (str | Path): Filepath of Silver job data.
        job_skills_silver_path: Filepath for the Silver job skills data.
        spark (SparkSession): Entry point interface for Spark engine.
    """
    
    df = jobs_silver_df.withColumn(
        "tokens",
        split(lower(col("description")), r"\W+")
    )
    
    df = df.withColumn(
        "skills",
        extract_skills_udf(col("tokens"))
    )
    
    df = df.withColumn(
        "skills_normalized",
        normalize_skills_udf(col("skills"))
    )
    
    df = df.withColumn(
        "skill_confidence",
        skill_confidence_udf(
                col("tokens"), 
                col("skills_normalized")
            )
    )
    
    job_skills_df = (
        df
        .withColumn("skill", explode("skills_normalized"))
        .select(
        "job_id",
        "ingestion_date",
        "posted_at",
        col("skill").alias("skills"),
        coalesce(element_at("skill_confidence", col("skill")), lit(0.0)).alias("skill_confidence"),
    ).withColumn(
        "extraction_method", lit("dictionary_v1")
    ).withColumn(
        "processed_at", current_timestamp()
    ))

    return job_skills_df


######################## 09-03

# def run_job_skills(
    # jobs_silver_df: DataFrame
# ) -> DataFrame:
    # """
    # Extract skills from Silver job data and write them into the Gold_v1 layer (job_skill_silver).
    
    # Args:
        # job_silver_path (str | Path): Filepath of Silver job data.
        # job_skills_silver_path: Filepath for the Silver job skills data.
        # spark (SparkSession): Entry point interface for Spark engine.
    # """
    
    # df = jobs_silver_df.withColumn(
        # "tokens",
        # split(lower(col("description")), r"\W+")
    # )
    
    # df = df.withColumn(
        # "skills",
        # extract_skills_udf(col("tokens"))
    # )
    
    # df = df.withColumn(
        # "skills_normalized",
        # normalize_skills_udf(col("skills"))
    # )
    
    # df = df.withColumn(
        # "skill_confidence",
        # skill_confidence_udf(
                # col("tokens"), 
                # col("skills_normalized")
            # )
    # )
    
    # job_skills_df = (
        # df
        # .withColumn("skill", explode("skills_normalized"))
        # .select(
        # "job_id",
        # "ingestion_date",
        # col("skill").alias("skills"),
        # element_at("skill_confidence", col("skill")).alias("skill_confidence"),
    # ).withColumn(
        # "extraction_method", lit("dictionary_v1")
    # ).withColumn(
        # "processed_at", current_timestamp()
    # ))

    # return job_skills_df

################

# def run_job_skills(
    # jobs_silver_df: DataFrame
# ) -> DataFrame:
    # """
    # Extract skills from Silver job data and write them into the Gold_v1 layer (job_skill_silver).
    
    # Args:
        # job_silver_path (str | Path): Filepath of Silver job data.
        # job_skills_silver_path: Filepath for the Silver job skills data.
        # spark (SparkSession): Entry point interface for Spark engine.
    # """
    
    # df = jobs_silver_df.withColumn(
        # "tokens",
        # split(lower(col("description")), r"\W+")
    # )
    
    # df = df.withColumn(
        # "skills",
        # extract_skills_udf(col("tokens"))
    # )
    
    # df = df.withColumn(
        # "skills_normalized",
        # normalize_skills_udf(col("skills"))
    # )
    
    # df = df.withColumn(
        # "skill_confidence",
        # skill_confidence_udf(
                # col("tokens"), 
                # col("skills_normalized")
            # )
    # )
    
    # job_skills_df = df.select(
        # "job_id",
        # "ingestion_date",
        # explode("skills_normalized").alias("skills"),
        # "skill_confidence"
    # ).withColumn(
        # "extraction_method", lit("dictionary_v1")
    # ).withColumn(
        # "processed_at", current_timestamp()
    # )
    

    # return job_skills_df

#################

# def run_job_skills(
    # job_silver_path: str | Path,
    # job_skills_silver_path: str | Path
    # spark: SparkSession
# ) -> None:
    # """
    # Extract skills from Silver job data and write them into the Gold_v1 layer (job_skill_silver).
    
    # Args:
        # job_silver_path (str | Path): Filepath of Silver job data.
        # job_skills_silver_path: Filepath for the Silver job skills data.
        # spark (SparkSession): Entry point interface for Spark engine.
    # """

        
    # df = spark.read.parquet(job_silver_path)
    
    # df = df.withColumn(
        # "tokens",
        # split(lower(col("description")), r"\W+")
    # )
    
    # df = df.withColumn(
        # "skills",
        # extract_skills_udf(col("tokens"))
    # )
    
    # df = df.withColumn(
        # "skills_normalized",
        # normalize_skills_udf(col("skills"))
    # )
    
    # df = df.withColumn(
        # "skill_confidence",
        # skill_confidence_udf(
                # col("tokens"), 
                # col("skills_normalized")
            # )
    # )
    
    # exploded_df = df.select(
        # "job_id",
        # explode("skills_normalized").alias("skills"),
        # "skill_confidence"
    # ).withColumn(
        # "extraction_method", lit("dictionary_v1")
    # ).withColumn(
        # "processed_at", current_timestamp()
    # )
    

    # exploded_df.write.mode("overwrite").parquet(job_skills_silver_path)

# def run_extract_skills(
    # silver_path: str | Path,
    # gold_path: str | Path
# ) -> None:
    # """
    # Extract skills from Silver job data and write them into a Gold layer.
    
    # Args:
        # silver_path (str | Path): Filepath of Silver job data.
        # gold_path: Filepath for the Gold layer data.
    # """
    # spark = (
        # SparkSession.builder
        # .appName("extract_skills")
        # .getOrCreate()
    # )
        
    # df = spark.read.parquet(silver_path)
    
    # df = df.withColumn(
        # "tokens",
        # split(lower(col("description")), r"\W+")
    # )
    
    # df = df.withColumn(
        # "skills",
        # extract_skills_udf(col("tokens"))
    # )
    
    # df = df.withColumn(
        # "skills_normalized",
        # normalize_skills_udf(col("skills"))
    # )
    
    # df = df.withColumn(
        # "skill_confidence",
        # skill_confidence_udf(
                # col("tokens"), 
                # col("skills_normalized")
            # )
    # )

    # final_df = df.select(
        # "job_id",
        # "skills",
        # "skills_normalized",
        # "skill_confidence"
    # ).withColumn(
        # "extraction_method", lit("dictionary_v1")
    # ).withColumn(
        # "processed_at", current_timestamp()
    # )
    # final_df.write.mode("overwrite").parquet(gold_path)
