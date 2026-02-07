from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, split, lit, current_timestamp
)

from job_plat.processing.spark_ops import (
    extract_skills_udf,
    normalize_skills_udf,
    skill_confidence_udf,
)
from pathlib import Path

def run_extract_skills(
    silver_path: str | Path,
    gold_path: str | Path
) -> None:
    
    spark = (
        SparkSession.builder
        .appName("extract_skills")
        .getOrCreate()
    )
        
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

    final_df = df.select(
        "job_id",
        "skills",
        "skills_normalized",
        "skill_confidence"
    ).withColumn(
        "extraction_method", lit("dictionary_v1")
    ).withColumn(
        "processed_at", current_timestamp()
    )
    final_df.write.mode("overwrite").parquet(gold_path)
