from pyspark.sql import SparkSession

from job_plat.embeddings.normalize import (
    EmbeddingSkillNormalizer
)

def run_normalize(
    gold_v1_path: str,
    lookup_output_path: str
) -> None:
    
    spark = (
        SparkSession.builder
        .appName("embedding-skill-normalization")
        .getOrCreate()
    )
    
    df = spark.read.parquet(gold_v1_path)
    
    # Collect distinct skills
    skills = (
        df
        .selectExpr("explode(skills_normalized) as skill")
        .distinct()
        .rdd
        .map(lambda r:r.skill)
        .collect()
    )
    
    normalizer = EmbeddingSkillNormalizer()
    
    mapping = normalizer.normalize(skills)
    
    lookup_df = spark.createDataFrame(
        [(k, v) for k, v in mapping.items()],
        ["raw_skill", "canonical_skill"]
    )
    
    lookup_df.write.mode("overwrite").parquet(
        lookup_output_path
    )
