from pyspark.sql import SparkSession
from pathlib import Path
from job_plat.transformations.gold.v1_analytics.build_dimensions import build_dim_jobs, build_dim_skills
from job_plat.utils.helpers import path_exists
from job_plat.context.contexts import SilverContext, GoldV1Context

def run_gold_v1(
    silver_ctx: SilverContext,
    gold_v1_ctx: GoldV1Context
) -> None:
    
    job_silver_path = silver_ctx.jobs_path
    job_skills_silver_path = silver_ctx.job_skills_path
    dim_jobs_path = gold_v1_ctx.dim_jobs_path
    dim_skills_path = gold_v1_ctx.dim_skills_path
    fact_job_skill_path = gold_v1_ctx.fact_job_skill_path
    spark = gold_v1_ctx.spark
    
    MISSING_DATA = False
    missing_data_message = []
    
    if not path_exists(spark = spark, path = job_silver_path):
        MISSING_DATA = True
        missing_data_message.append(f"Silver job data not found in {job_silver_path}")
    if not path_exists(spark = spark, path = job_skills_silver_path):
        MISSING_DATA = True
        missing_data_message.append(f"Silver job sills data not found in {job_silver_path}")
    
    if MISSING_DATA:
        raise FileNotFoundError("\n".join(missing_data_message))
        
    job_silver_df = spark.read.parquet(job_silver_path)
    job_skills_silver_df = spark.read.parquet(job_skills_silver_path)
    
    dim_jobs_df = build_dim_jobs(
        job_silver_df = job_silver_df
        )
    
    dim_skills_df = build_dim_skills(
        job_skills_silver_df = job_skills_silver_df
    )
    
    fact_job_skill_df = build_fact_job_skills(
        job_skills_silver_df = job_skills_silver_df,
        dim_skills_df = dim_skills_df
    )
    
    dim_jobs_df.write.mode("overwrite").parquet(dim_jobs_path)
    dim_skills_df.write.mode("overwrite").parquet(dim_skills_path)
    fact_job_skill_df.write.mode("overwrite").parquet(fact_job_skill_path)
    



# def run_gold_v1(
    # job_silver_path: Path,
    # job_skills_silver_path: Path,
    # dim_jobs_path: Path,
    # dim_skills_path: Path,
    # fact_job_skill_path: Path,
    # spark: SparkSession
# ) -> None:
    
    # MISSING_DATA = False
    # missing_data_message = []
    
    # if not path_exists(spark = spark, path = job_silver_path):
        # MISSING_DATA = True
        # missing_data_message.append(f"Silver job data not found in {job_silver_path}")
    # if not path_exists(spark = spark, path = job_skills_silver_path):
        # MISSING_DATA = True
        # missing_data_message.append(f"Silver job sills data not found in {job_silver_path}")
    
    # if MISSING_DATA:
        # raise FileNotFoundError("\n".join(missing_data_message))
        
    # job_silver_df = spark.read.parquet(job_silver_path)
    # job_skills_silver_df = spark.read.parquet(job_skills_silver_path)
    
    # dim_jobs_df = build_dim_jobs(
        # job_silver_df = job_silver_df
        # )
    
    # dim_skills_df = build_dim_skills(
        # job_skills_silver_df = job_skills_silver_df
    # )
    
    # fact_job_skill_df = build_fact_job_skills(
        # job_skills_silver_df = job_skills_silver_df,
        # dim_skills_df = dim_skills_df
    # )
    
    # dim_jobs_df.write.mode("overwrite").parquet(dim_jobs_path)
    # dim_skills_df.write.mode("overwrite").parquet(dim_skills_path)
    # fact_job_skill_df.write.mode("overwrite").parquet(fact_job_skill_path)
