from datetime import date
from pathlib import Path
from job_plat.config.context import PipelineContext
from job_plat.pipelines.stages import GoldV1Stage


def gold_v1_pipeline(
    ctx: PipelineContext
) -> None:
    
    stage = GoldV1Stage(
        silver_ctx = ctx.silver,
        gold_v1_ctx = ctx.gold_v1
    )
    
    stage.execute()
    
 
 
# def gold_v1_pipeline(
    # ctx: PipelineContext
# ) -> None:
    
    # silver_ctx = ctx.silver
    # gold_v1_ctx = ctx.gold_v1
    
    
    
    # run_gold_v1(
        # silver_ctx = silver_ctx,
        # gold_v1_ctx = gold_v1_ctx
    # )
 
 # def gold_v1_pipeline(
    # ctx: PipelineContext
# ) -> None:
    
    # silver_ctx = ctx.silver
    # gold_ctx = ctx.gold_v1
    
    # job_silver_path = silver_ctx.jobs_path
    # job_skills_silver_path = silver_ctx.job_skills_path
    # dim_jobs_path = gold_ctx.dim_jobs_path
    # dim_skills_path = gold_ctx.dim_skills_path
    # fact_job_skill_path = gold_ctx.fact_job_skill_path
    # spark = ctx.spark
    
    # run_gold_v1(
        # job_silver_path = job_silver_path,
        # job_skills_silver_path = job_skills_silver_path,
        # dim_jobs_path = dim_jobs_path,
        # dim_skills_path = dim_skills_path,
        # fact_job_skill_path = fact_job_skill_path,
        # spark = spark
    # )
            
    
    
