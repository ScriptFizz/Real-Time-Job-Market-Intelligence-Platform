from datetime import datetime

from pyspark.sql import DataFrame, functions as F

# job_embeddings: 1 row per (job_id, model_version)


def build_job_embeddings(
    fact_job_skill_df: DataFrame,
    skill_embeddings_df: DataFrame,
    *,
    generated_at: datetime,
    model_version: str = "v1",
    aggregation_method: str = "weighted_mean",
) -> DataFrame:
    """
    Build job embeddings using skill embeddings.
    Supports weighted mean aggregation.
    """

    # Only active embedding
    active_embedding = (
        skill_embeddings_df.filter(F.col("is_active"))
        .filter(F.col("model_version") == model_version)
        .select("skill_id", "embedding", "embedding_dim")
    )

    # Join fact -> skill embeddings
    joined = fact_job_skill_df.join(active_embedding, "skill_id").select(
        "job_id", "skill_confidence", "embedding", "embedding_dim"
    )

    # Weighted aggregation
    aggregated = (
        joined.groupBy("job_id")
        .agg(
            F.first("embedding_dim").alias("embedding_dim"),
            F.count("*").alias("skill_count"),
            F.sum("skill_confidence").alias("weight_sum"),
            F.expr("""
                aggregate(
                    collect_list(
                        transform(embedding, x -> x * skill_confidence)
                    ),
                    array_repeat(CAST(0.0 AS DOUBLE), first(embedding_dim)),
                    (acc, x) -> zip_with(acc, x, (a, b) -> a + b)
                )
            """).alias("weighted_sum"),
        )
        .withColumn("embedding", F.expr("transform(weighted_sum, x -> x / weight_sum)"))
    )

    # L2 normalization
    result = (
        aggregated.withColumn(
            "norm", F.sqrt(F.expr("aggregate(embedding, 0D, (acc, x) -> acc + x * x)"))
        )
        .withColumn(
            "embedding_normalized",
            F.expr("""
                transform(
                    embedding,
                    x -> CASE WHEN norm > 0 THEN x / norm ELSE x END
                )
            """),
        )
        .withColumn("model_version", F.lit(model_version))
        .withColumn("aggregation_method", F.lit(aggregation_method))
        .withColumn("generated_at", F.lit(generated_at))
        .select(
            "job_id",
            "model_version",
            "aggregation_method",
            "embedding",
            "embedding_normalized",
            "embedding_dim",
            "skill_count",
            "generated_at",
        )
    )

    return result
