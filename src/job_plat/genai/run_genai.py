from pyspark.sql import SparkSession
import pandas as pd

from job_plat.genai.client import (
    normalize_skills_batch
)

def run_genai(
    gold_v1_path: str,
    lookup_ouput_path: str,
    batch_size: int = 50
):
    spark = SparkSession.builder \
        .appName("genai-skill-normalization") \
        .getOrCreate()
        
    df = spark.read.parquet(gold_v1_path)
    
    # explode and deduplicate
    skills_df = (
        df
        .selectExpr("explode(skills_normalized) as skill")
        .distinct()
    )
    
    skills = [r.skill for r in skills_df.collect()]
    
    mappings = {}
    
    for in range(0, len(skills), batch_size):
        batch = skills[i:i + batch_size]
        result = normalize_skills_batch(batch)
        mappings.update(result)
    
    lookup_df = spark.createDataFrame(
        [(k, v) for k, v in mappings.items()],
        ["raw_skill", "normalized_skill"]
    )
    
    lookup_df.write.mode("overwrite").parquet(
        lookup_ouput_path
    )
