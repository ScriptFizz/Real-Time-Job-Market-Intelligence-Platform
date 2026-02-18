from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType
from functools import reduce

def build_job_embeddins(
    fact_job_skills_df: DataFrame,
    dim_skills_df: DataFrame,
    skill_embeddings_df: DataFrame
) -> DataFrame:
    
    
    # Join fact -> skill -> embedding
    job_skill_embeddings = (
        fact_job_skills_df
        .join(dim_skills_df, "skill_id")
        .join(skill_embeddings_df,
            dim_skills_df.skill = skill_embeddings_df.canonical_skill)
        .select(
            "job_id",
            "skill_confidence",
            "embedding"
        )
        .filter(F.col("embedding").isNotNull())
    )
    
    # Weighted sum of embeddings
    aggregated = (
        job_skill_embeddings
        .groupBy("job_id")
        .agg(
            F.expr("""
                aggregate(
                    collect_list(
                        transform(embedding, x -> x * skill_confidence)
                    ),
                    array_repeat(0.0, size(embedding)),
                    (acc, x) -> zip_with(acc, x, (a, b) -> a + b)
                )
            """).alias("weighted_sum"),
            F.sum("skill_confidence").alias("weight_sum")
        )
        .withColumn(
            "job_embedding",
            F.expr("transform(weighted_sum, x -> x / weight_sum)")
        )
    )
    
    # L2 normalize
    result = (
        aggregated
        .withColumn(
            "norm",
            F.sqrt(
                F.expr("aggregate(job_embedding, 0D, (acc, x) -> acc + x * x)")
            )
        )
        .withColumn(
            "job_embedding_normalized",
            F.expr("""
                transform(
                    job_embedding,
                    x -> CASE WHEN norm > 0 THEN x / norm ELSE x END
                )
            """)
        )
        .withColumn("generated_at", F.current_timestamp())
        .select(
            "job_id",
            "job_embedding",
            "job_embedding_normalized",
            "generated_at"
        )
    )
    
    return result




# def build_job_embeddins(
    # gold_v1_path: str,
    # skill_lookup_path: str,
    # output_path: str
# ) -> None:

    # spark = (
        # SparkSession.builder
        # .appName("build-job-embeddings")
        # .getOrCreate()
    # )
    
    # jobs_df = spark.read.parquet(gold_v1_path)
    # skills_df = spark.read.parquet(skill_lookup_path)
    
    # # Explode job skills
    # exploded = (
        # jobs_df
        # .select(
            # "job_id",
            # F.explode("skills_normalized").alias("canonical_skill")
        # )
    # )
    
    # # Left join to skill lookup
    # joined = (
        # exploded
        # .join(skills_df, on="canonical_skill", how="left")
        # .filter(F.col("embedding").isNotNull())
    # )
    
    # # Aggregate embeddings per job
    # job_embeddings = (
        # joined
        # .groupBy("job_id")
        # .agg(
            # F.expr("""
                # aggregate(
                    # collect_list(embedding),
                    # cast(array() as array<float>),
                    # (acc, x) ->
                        # transform(
                            # sequence(0, size(x) - 1),
                            # i -> coalesce(acc[i], 0.0) + x[i]
                        # )
                # )
            # """).alias("embedding_sum"),
            # F.count("*").alias("skill_count")
        # )
        # .withColumn(
            # "job_embedding",
            # F.expr(""" 
                # transform(embedding_sum, x -> x / skill_count)
            # """)
        # )
        # .select("job_id", "job_embedding")
    # )
    
    # job_embedding_normalized = (
        # job_embeddings
        # .withColumn(
            # "norm",
            # F.expr("""
                # sqrt(aggregate(job_embedding, 0D, (acc, y) -> acc + y*y))
            # """)
        # )
        # .withColumn(
            # "job_embedding_normalized",
            # F.expr("""
                # transform(
                    # job_embedding,
                    # x -> CASE
                        # WHEN norm > 0 THEN x / norm
                        # ELSE x
                    # END
                # )
            # """)
        # ).withColumn(
        # "generated_at", F.current_timestamp()
        # )
        # .select("job_id", "job_embedding", "job_embedding_normalized", "generated_at")
    # )
    
    # job_embedding_normalized.write.mode("overwrite").parquet(output_path)



# def build_job_embeddins(
    # gold_v1_path: str,
    # skill_lookup_path: str,
    # output_path: str
# ) -> None:

    # spark = (
        # SparkSession.builder
        # .appName("build-job-embeddings")
        # .getOrCreate()
    # )
    
    # jobs_df = spark.read.parquet(gold_v1_path)
    # skills_df = spark.read.parquet(skill_lookup_path)
    
    # # Explode job skills
    # exploded = (
        # jobs_df
        # .select(
            # "job_id",
            # F.explode("skills_normalized").alias("canonical_skill")
        # )
    # )
    
    # # Left join to skill lookup
    # joined = (
        # exploded
        # .join(skills_df, on="canonical_skill", how="left")
        # .filter(F.col("embedding").isNotNull())
    # )
    
    # # Aggregate embeddings per job
    # job_embeddings = (
        # joined
        # .groupBy("job_id")
        # .agg(
            # F.expr("""
                # aggregate(
                    # collect_list(embedding),
                    # cast(array() as array<float>),
                    # (acc, x) ->
                        # transform(
                            # sequence(0, size(x) - 1),
                            # i -> coalesce(acc[i], 0.0) + x[i]
                        # )
                # )
            # """).alias("embedding_sum"),
            # F.count("*").alias("skill_count")
        # )
        # .withColumn(
            # "job_embedding",
            # F.expr(""" 
                # transform(embedding_sum, x -> x / skill_count)
            # """)
        # )
        # .select("job_id", "job_embedding")
    # )
    
    # job_embedding_normalized = (
        # job_embeddings
        # .withColumn(
            # "norm",
            # F.expr("""
                # sqrt(aggregate(job_embedding, 0D, (acc, y) -> acc + y*y))
            # """)
        # )
        # .withColumn(
            # "job_embedding_normalized",
            # F.expr("""
                # transform(
                    # job_embedding,
                    # x -> CASE
                        # WHEN norm > 0 THEN x / norm
                        # ELSE x
                    # END
                # )
            # """)
        # ).withColumn(
        # "generated_at", F.current_timestamp()
        # )
        # .select("job_id", "job_embedding", "job_embedding_normalized", "generated_at")
    # )
    
    # job_embedding_normalized.write.mode("overwrite").parquet(output_path)
