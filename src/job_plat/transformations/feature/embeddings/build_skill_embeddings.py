from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType, FloatType
)
from pyspark.sql import functions as F
from sentence_transformers import SentenceTransformer
from pathlib import Path
from typing import List

# skill_embeddings: 1 row per (skill_id, model_version)

def build_skill_embeddings(
    dim_skills_df: DataFrame,
    spark: SparkSession,
    model_name: str = "all-MiniLM-L6-v2",
    model_version: str = "v1",
    model_provider: str = "sentence-transformer"
) -> DataFrame:
    """
    Define embedding of normalized skills to store into Gold layer.
    """
    
    # Collect distinct skills
    skills: List[str] = (
        dim_skills_df
        .orderBy("skill_id")
        .select("skills")
        .rdd
        .map(lambda r:r.skills)
        .collect()
    )
    
    model = SentenceTransformer(model_name)
    embeddings = model.encode(skills, show_progress_bar=True)
    
    embedding_dim = embeddings.shape[1] if embeddings.size > 0 else 0
    
    records = [(skill, emb.tolist()) for skill, emb in zip(skills, embeddings)]
    
    embedding_df = spark.createDataFrame(
        records, 
        schema=["skills", "embedding"]
    )
    
    result = (
        dim_skills_df
        .join(embedding_df, "skills")
        .withColumn("embedding_dim", F.lit(embedding_dim))
        .withColumn("model_name", F.lit(model_name))
        .withColumn("model_version", F.lit(model_version))
        .withColumn("model_provider", F.lit(model_provider))
        .withColumn("generated_at", F.current_timestamp())
        .withColumn("is_active", F.lit(True))
        .select(
            "skill_id",
            "skills",
            "embedding",
            "embedding_dim",
            "model_name",
            "model_version",
            "model_provider",
            "generated_at",
            "is_active"
        )
    )
    
    return result
    
