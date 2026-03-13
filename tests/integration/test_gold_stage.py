import pytest
from job_plat.pipeline.stages.data.gold_stage import GoldStage
from job_plat.pipeline.datasets.dataset_definitions import GoldDimJobs, GoldDimSkills, GoldFactJobSkills



def test_gold_stage_runs(
    spark,
    dataset_registry,
    partition_manager,
    silver_jobs_data,
    silver_job_skills_data,
    silver_ctx,
    gold_ctx
):
    
    stage = GoldStage(
        gold_ctx=gold_ctx,
        silver_ctx=silver_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager
    )
    
    stage.execute()
    
    gold_dim_jobs = dataset_registry.get(GoldDimJobs)
    gold_dim_jobs_df = spark.read.parquet(str(gold_dim_jobs.path))
    
    gold_dim_skills = dataset_registry.get(GoldDimSkills)
    gold_dim_skills_df = spark.read.parquet(str(gold_dim_skills.path))
    
    gold_fact_job_skills = dataset_registry.get(GoldFactJobSkills)
    gold_fact_job_skills_df = spark.read.parquet(str(gold_fact_job_skills.path))
    
    assert gold_dim_jobs_df.count() > 0
    assert gold_dim_skills_df.count() > 0
    assert gold_fact_job_skills_df.count() > 0
