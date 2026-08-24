from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.state_store import LocalStateStore
from job_plat.pipeline.datasets.dataset_definitions import (
    GoldDimJobs,
    GoldDimSkills,
    GoldFactJobSkills,
)
from job_plat.pipeline.stages.data.gold_stage import GoldStage


def test_gold_stage_runs(
    spark,
    dataset_registry,
    partition_manager,
    silver_jobs_data,
    silver_job_skills_data,
    gold_ctx,
    tmp_path,
):
    del silver_jobs_data  # Fixture materializes the Silver jobs input dataset.
    del silver_job_skills_data  # Fixture materializes the Silver skills input dataset.

    stage = GoldStage(
        gold_ctx=gold_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager,
    )
    stage.execute()

    gold_dim_jobs = dataset_registry.get(GoldDimJobs)
    gold_dim_skills = dataset_registry.get(GoldDimSkills)
    gold_fact_job_skills = dataset_registry.get(GoldFactJobSkills)

    gold_dim_jobs_df = spark.read.format("delta").load(str(gold_dim_jobs.path))
    gold_dim_skills_df = spark.read.format("delta").load(
        str(gold_dim_skills.path)
    )
    gold_fact_job_skills_df = spark.read.format("delta").load(
        str(gold_fact_job_skills.path)
    )

    first_jobs_count = gold_dim_jobs_df.count()
    first_skills_count = gold_dim_skills_df.count()
    first_fact_count = gold_fact_job_skills_df.count()

    assert first_jobs_count > 0
    assert first_skills_count > 0
    assert first_fact_count > 0

    retry_metadata = tmp_path / "gold-retry-metadata"
    retry_partition_manager = PartitionManager(LocalStateStore(str(retry_metadata)))

    retry_stage = GoldStage(
        gold_ctx=gold_ctx,
        datasets=dataset_registry,
        partition_manager=retry_partition_manager,
    )
    retry_stage.execute()

    retried_jobs_df = spark.read.format("delta").load(str(gold_dim_jobs.path))
    retried_skills_df = spark.read.format("delta").load(
        str(gold_dim_skills.path)
    )
    retried_fact_df = spark.read.format("delta").load(
        str(gold_fact_job_skills.path)
    )

    assert retried_jobs_df.count() == first_jobs_count
    assert retried_skills_df.count() == first_skills_count
    assert retried_fact_df.count() == first_fact_count
    assert retried_jobs_df.select("job_id").distinct().count() == first_jobs_count
    assert retried_skills_df.select("skill_id").distinct().count() == first_skills_count
