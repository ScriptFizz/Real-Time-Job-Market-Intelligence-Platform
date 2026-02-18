from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ArrayType, FloatType
)

from job_plat.embeddings.gold.v2_intelligence.embedding_skill_normalize import (
    EmbeddingSkillNormalizer
)
from pathlib import Path

def build_skill_embeddings(
    dim_skills_df: DataFrame,
    spark: SparkSession
    model_name: str = "all-MiniLM-L6-v2"
) -> DataFrame:
    """
    Define embedding of normalized skills to store into Gold layer.
    """
    
    # Collect distinct skills
    skills = (
        dim_skills_df
        .select("skill")
        .rdd
        .map(lambda r:r.skill)
        .collect()
    )
    
    model = SentenceTransformer(model_name)
    embeddings = model.encode(skills, show_progress_bar=True)
    
    records = [(skill, emb.tolist()) for skill, emb in zip(skills, embeddings)]
    
    result = spark.sparkSession.createDataFrame(
        records,
        schema=["skill", "embedding"]
    )
    
    return result
    
    normalizer = EmbeddingSkillNormalizer()
    
    records = normalizer.normalize(skills)
    
    schema = StructType([
        StructField("skill", StringType(), False),
        StructField("embedding", ArrayType(FloatType()), False),
        StructField("aliases", ArrayType(StringType()), False)
    ])
    
    skill_lookup_df = spark.createDataFrame(records, schema=schema)
    
    return skill_lookup_df
    
    
# def build_skill_embeddings(
    # spark: SparkSession,
    # dim_skills_df: DataFrame
# ) -> None:
    # """
    # Define embedding of normalized skills to store into Gold layer.
    # """
    
    # # Collect distinct skills
    # skills = (
        # dim_skills_df
        # .select("skill")
        # .distinct()
        # .rdd
        # .map(lambda r:r.skill)
        # .collect()
    # )
    
    # normalizer = EmbeddingSkillNormalizer()
    
    # records = normalizer.normalize(skills)
    
    # schema = StructType([
        # StructField("skill", StringType(), False),
        # StructField("embedding", ArrayType(FloatType()), False),
        # StructField("aliases", ArrayType(StringType()), False)
    # ])
    
    # skill_lookup_df = spark.createDataFrame(records, schema=schema)
    
    # return skill_lookup_df


# def build_skill_embeddings(
    # spark: SparkSession,
    # dim_skills_df: DataFrame
# ) -> None:
    # """
    # Define embedding of normalized skills to store into Gold layer.
    
    # Args:
        # gold_path (str | Path): Filepath of the Gold layer jobs data.
        # lookup_output_path (str | Path): Filepath to store the embedded skills data.
    # """
    # spark = (
        # SparkSession.builder
        # .appName("skill-embeddings")
        # .getOrCreate()
    # )
    
    # dim_df = spark.read.parquet(dim_skills_path)
    
    # # Collect distinct skills
    # skills = (
        # df
        # .select("skill")
        # .distinct()
        # .rdd
        # .map(lambda r:r.skill)
        # .collect()
    # )
    
    # normalizer = EmbeddingSkillNormalizer()
    
    # records = normalizer.normalize(skills)
    
    # schema = StructType([
        # StructField("skill", StringType(), False),
        # StructField("embedding", ArrayType(FloatType()), False),
        # StructField("aliases", ArrayType(StringType()), False)
    # ])
    
    # lookup_df = spark.createDataFrame(records, schema=schema)
    
    # lookup_df.write.mode("overwrite").parquet(
        # lookup_output_path
    # )



# def build_skill_embeddings(
    # dim_skills_path: str | Path,
    # output_path: str | Path
# ) -> None:
    # """
    # Define embedding of normalized skills to store into Gold layer.
    
    # Args:
        # gold_path (str | Path): Filepath of the Gold layer jobs data.
        # lookup_output_path (str | Path): Filepath to store the embedded skills data.
    # """
    # spark = (
        # SparkSession.builder
        # .appName("skill-embeddings")
        # .getOrCreate()
    # )
    
    # dim_df = spark.read.parquet(dim_skills_path)
    
    # # Collect distinct skills
    # skills = (
        # df
        # .select("skill")
        # .distinct()
        # .rdd
        # .map(lambda r:r.skill)
        # .collect()
    # )
    
    # normalizer = EmbeddingSkillNormalizer()
    
    # records = normalizer.normalize(skills)
    
    # schema = StructType([
        # StructField("skill", StringType(), False),
        # StructField("embedding", ArrayType(FloatType()), False),
        # StructField("aliases", ArrayType(StringType()), False)
    # ])
    
    # lookup_df = spark.createDataFrame(records, schema=schema)
    
    # lookup_df.write.mode("overwrite").parquet(
        # lookup_output_path
    # )


# def embed_normalize_skills(
    # gold_path: str | Path,
    # lookup_output_path: str | Path
# ) -> None:
    # """
    # Define embedding of normalized skills to store into Gold layer.
    
    # Args:
        # gold_path (str | Path): Filepath of the Gold layer jobs data.
        # lookup_output_path (str | Path): Filepath to store the embedded skills data.
    # """
    # spark = (
        # SparkSession.builder
        # .appName("embedding-skill-normalization")
        # .getOrCreate()
    # )
    
    # df = spark.read.parquet(gold_path)
    
    # # Collect distinct skills
    # skills = (
        # df
        # .selectExpr("explode(skills_normalized) as skill")
        # .distinct()
        # .rdd
        # .map(lambda r:r.skill)
        # .collect()
    # )
    
    # normalizer = EmbeddingSkillNormalizer()
    
    # records = normalizer.normalize(skills)
    
    # schema = StructType([
        # StructField("canonical_skill", StringType(), False),
        # StructField("embedding", ArrayType(FloatType()), False),
        # StructField("aliases", ArrayType(StringType()), False)
    # ])
    
    # lookup_df = spark.createDataFrame(records, schema=schema)
    
    # lookup_df.write.mode("overwrite").parquet(
        # lookup_output_path
    # )
