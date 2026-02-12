from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, split, lit, current_timestamp, explode
)

from job_plat.utils.helpers import create_spark
from job_plat.processing.spark_ops import (
    extract_skills_udf,
    normalize_skills_udf,
    skill_confidence_udf,
)
from pathlib import Path

def run_extract_skills(
    job_silver_path: str | Path,
    output_path: str | Path
) -> None:
    """
    Extract skills from Silver job data and write them into the Gold_v1 layer (job_skill_silver).
    
    Args:
        job_silver_path (str | Path): Filepath of Silver job data.
        output_path: Filepath for the Silver job skills data.
    """
    spark = create_spark(app_name="silver-extract_skills")

        
    df = spark.read.parquet(silver_path)
    
    df = df.withColumn(
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
    
    exploded_df = df.select(
        "job_id",
        explode("skills_normalized").alias("skills"),
        "skill_confidence"
    ).withColumn(
        "extraction_method", lit("dictionary_v1")
    ).withColumn(
        "processed_at", current_timestamp()
    )
    

    exploded_df.write.mode("overwrite").parquet(gold_path)



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
