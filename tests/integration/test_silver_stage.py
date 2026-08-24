import pytest

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.processing_ledger import ProcessingLedger
from job_plat.pipeline.datasets.dataset_definitions import (
    BronzeJobs,
    SilverJobs,
    SilverJobSkills,
)
from job_plat.pipeline.stages.data.silver_stage import SilverStage


def test_silver_stage_runs(
    spark,
    dataset_registry,
    partition_manager,
    bronze_jobs_data,
    silver_ctx,
    tmp_path,
):
    del bronze_jobs_data  # Fixture materializes the Bronze input dataset.

    stage = SilverStage(
        silver_ctx=silver_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager,
    )

    assert dataset_registry.get(BronzeJobs).list_partitions()
    stage.execute()

    silver_jobs = dataset_registry.get(SilverJobs)
    silver_jobs_df = spark.read.format("delta").load(str(silver_jobs.path))

    silver_job_skills = dataset_registry.get(SilverJobSkills)
    silver_job_skills_df = spark.read.format("delta").load(
        str(silver_job_skills.path)
    )

    assert silver_jobs_df.count() > 0
    assert silver_job_skills_df.count() > 0
    assert "job_title" in silver_jobs_df.columns
    assert "skills" in silver_job_skills_df.columns
    assert len(silver_jobs.list_partitions()) == 1

    first_jobs_count = silver_jobs_df.count()
    first_skills_count = silver_job_skills_df.count()

    retry_metadata = tmp_path / "retry-metadata"
    retry_partition_manager = PartitionManager(
        ProcessingLedger(spark, str(retry_metadata))
    )

    retry_stage = SilverStage(
        silver_ctx=silver_ctx,
        datasets=dataset_registry,
        partition_manager=retry_partition_manager,
    )
    retry_stage.execute()

    retried_jobs_df = spark.read.format("delta").load(str(silver_jobs.path))
    retried_skills_df = spark.read.format("delta").load(
        str(silver_job_skills.path)
    )

    assert retried_jobs_df.count() == first_jobs_count
    assert retried_skills_df.count() == first_skills_count


def test_silver_stage_recovers_from_partial_write_and_acknowledgement_failure(
    spark,
    dataset_registry,
    partition_manager,
    bronze_jobs_data,
    silver_ctx,
    monkeypatch,
):
    del bronze_jobs_data

    silver_jobs = dataset_registry.get(SilverJobs)
    silver_job_skills = dataset_registry.get(SilverJobSkills)
    original_skills_write = silver_job_skills.write

    def fail_second_output(*_args, **_kwargs):
        raise RuntimeError("injected second-output failure")

    monkeypatch.setattr(silver_job_skills, "write", fail_second_output)
    first_attempt = SilverStage(
        silver_ctx=silver_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager,
    )

    with pytest.raises(RuntimeError, match="second-output failure"):
        first_attempt.execute()

    assert silver_jobs.read_all(spark).count() > 0
    assert partition_manager.get_processed("silver") == set()

    monkeypatch.setattr(silver_job_skills, "write", original_skills_write)
    original_mark_processed = partition_manager.mark_processed

    def fail_acknowledgement(*_args, **_kwargs):
        raise RuntimeError("injected acknowledgement failure")

    monkeypatch.setattr(
        partition_manager,
        "mark_processed",
        fail_acknowledgement,
    )
    second_attempt = SilverStage(
        silver_ctx=silver_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager,
    )

    with pytest.raises(RuntimeError, match="acknowledgement failure"):
        second_attempt.execute()

    jobs_after_failed_ack = silver_jobs.read_all(spark).count()
    skills_after_failed_ack = silver_job_skills.read_all(spark).count()
    assert partition_manager.get_processed("silver") == set()

    monkeypatch.setattr(
        partition_manager,
        "mark_processed",
        original_mark_processed,
    )
    final_attempt = SilverStage(
        silver_ctx=silver_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager,
    )
    final_attempt.execute()

    assert silver_jobs.read_all(spark).count() == jobs_after_failed_ack
    assert silver_job_skills.read_all(spark).count() == skills_after_failed_ack
    assert partition_manager.get_processed("silver")
