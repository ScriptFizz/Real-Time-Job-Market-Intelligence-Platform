from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType
from functools import reduce

def build_job_embeddins(
    gold_v1_path: str,
    skill_lookup_path: str,
    output_path: str
) -> None:

    spark = (
        SparkSession.builder
        .appName("build-job-embeddings")
        .getOrCreate()
    )
    
    jobs_df = spark.read.parquet(gold_v1_path)
    skills_df = spark.read.parquet(skill_lookup_path)
    
    # Explode job skills
    exploded = (
        jobs_df
        .select(
            "job_id",
            F.explode("skills_normalized").alias("canonical_skill")
        )
    )
    
    # Left join to skill lookup
    joined = (
        exploded
        .join(skills_df, on="canonical_skill", how="left")
        .filter(F.col("embedding").isNotNull())
    )
    
    # Aggregate embeddings per job
    job_embeddings = (
        joined
        .groupBy("job_id")
        .agg(
            F.expr("""
                aggregate(
                    collect_list(embedding),
                    cast(array() as array<float>),
                    (acc, x) ->
                        transform(
                            sequence(0, size(x) - 1),
                            i -> coalesce(acc[i], 0.0) + x[i]
                        )
                )
            """).alias("embedding_sum"),
            F.count("*").alias("skill_count")
        )
        .withColumn(
            "job_embedding",
            F.expr(""" 
                transform(embedding_sum, x -> x / skill_count)
            """)
        )
        .select("job_id", "job_embedding")
    )
    
    job_embedding_normalized = (
        job_embeddings
        .withColumn(
            "norm",
            F.expr("""
                sqrt(aggregate(job_embedding, 0D, (acc, y) -> acc + y*y))
            """)
        )
        .withColumn(
            "job_embedding_normalized",
            F.expr("""
                transform(
                    job_embedding,
                    x -> CASE
                        WHEN norm > 0 THEN x / norm
                        ELSE x
                    END
                )
            """)
        )
        .select("job_id", "job_embedding", "job_embedding_normalized")
    )
    
    job_embedding_normalized.write.mode("overwrite").parquet(output_path)
