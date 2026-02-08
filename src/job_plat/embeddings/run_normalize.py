from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType, FloatType
)

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
    
    records = normalizer.normalize(skills)
    
    schema = StructType([
        StructField("canonical_skill", StringType(), False),
        StructField("embedding", ArrayType(FloatType()), False),
        StructField("aliases", ArrayType(StringType()), False)
    ])
    
    lookup_df = spark.createDataFrame(records, schema=schema)
    
    lookup_df.write.mode("overwrite").parquet(
        lookup_output_path
    )
    
    
    # lookup_df = spark.createDataFrame(
        # [(k, v) for k, v in mapping.items()],
        # ["raw_skill", "canonical_skill"]
    # )
    
    # lookup_df.write.mode("overwrite").parquet(
        # lookup_output_path
    # )
