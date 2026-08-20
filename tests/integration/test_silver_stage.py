from job_plat.pipeline.stages.data.silver_stage import SilverStage
from job_plat.pipeline.datasets.dataset_definitions import SilverJobs, SilverJobSkills, BronzeJobs


def test_silver_stage_runs(
    spark,
    dataset_registry,
    partition_manager,
    bronze_jobs_data,
    silver_ctx
):
    
    
    ds = dataset_registry.get(BronzeJobs)
    
    stage = SilverStage(
        silver_ctx=silver_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager
    )
    
    assert dataset_registry.get(BronzeJobs).list_partitions()
    stage.execute()
    
    silver_jobs = dataset_registry.get(SilverJobs)
    silver_jobs_df = spark.read.parquet(str(silver_jobs.path))
    
    silver_job_skills = dataset_registry.get(SilverJobSkills)
    silver_job_skills_df = spark.read.parquet(str(silver_job_skills.path))
    
    assert silver_jobs_df.count() > 0
    assert silver_job_skills_df.count() > 0
    assert "job_title" in silver_jobs_df.columns
    assert "skills" in silver_job_skills_df.columns
    assert len(silver_jobs.list_partitions()) == 1
