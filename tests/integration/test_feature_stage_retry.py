from datetime import UTC, date, datetime

import numpy as np

from job_plat.context.contexts import FeatureContext
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.processing_ledger import ProcessingLedger
from job_plat.pipeline.datasets.dataset_definitions import (
    FeatureJobEmbeddings,
    FeatureSkillEmbeddings,
    GoldDimJobs,
    GoldDimSkills,
    GoldFactJobSkills,
)
from job_plat.pipeline.stages.ml.feature_stage import FeatureStage


class FakeEncoder:
    def encode(self, skills, batch_size=128, show_progress_bar=True):
        del batch_size
        del show_progress_bar
        return np.ones((len(skills), 4))


def test_feature_stage_retry_is_idempotent(
    spark,
    dataset_registry,
    partition_manager,
    monkeypatch,
    tmp_path,
):
    execution_date = datetime(2025, 3, 3, tzinfo=UTC)
    ingestion_date = date(2025, 3, 2)
    posted_at = datetime(2025, 3, 2, 12, tzinfo=UTC)

    dim_jobs_df = spark.createDataFrame(
        [
            ("job-1", posted_at, ingestion_date),
            ("job-2", posted_at, ingestion_date),
        ],
        ["job_id", "posted_at", "ingestion_date"],
    )

    dim_skills_df = spark.createDataFrame(
        [
            ("skill-python", "python", ingestion_date),
            ("skill-spark", "spark", ingestion_date),
        ],
        ["skill_id", "skills", "ingestion_date"],
    )

    fact_job_skills_df = spark.createDataFrame(
        [
            (
                "job-1",
                "skill-python",
                0.9,
                posted_at,
                ingestion_date,
            ),
            (
                "job-1",
                "skill-spark",
                0.8,
                posted_at,
                ingestion_date,
            ),
            (
                "job-2",
                "skill-python",
                0.7,
                posted_at,
                ingestion_date,
            ),
        ],
        [
            "job_id",
            "skill_id",
            "skill_confidence",
            "posted_at",
            "ingestion_date",
        ],
    )

    dataset_registry.get(GoldDimJobs).write(
        dim_jobs_df,
        mode="overwrite",
    )
    dataset_registry.get(GoldDimSkills).write(
        dim_skills_df,
        mode="overwrite",
    )
    dataset_registry.get(GoldFactJobSkills).write(
        fact_job_skills_df,
        mode="overwrite",
    )

    model_load_count = 0

    def load_model(_model_name):
        nonlocal model_load_count
        model_load_count += 1
        return FakeEncoder()

    monkeypatch.setattr(
        "job_plat.transformations.feature.embeddings."
        "build_skill_embeddings.SentenceTransformer",
        load_model,
    )

    feature_ctx = FeatureContext(
        spark=spark,
        execution_date=execution_date,
        window_days=7,
    )

    stage = FeatureStage(
        feature_ctx=feature_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager,
    )
    stage.execute()

    skill_embeddings = dataset_registry.get(FeatureSkillEmbeddings)
    job_embeddings = dataset_registry.get(FeatureJobEmbeddings)

    first_skill_embeddings = skill_embeddings.read_all(spark)
    first_job_embeddings = job_embeddings.read_all(spark)

    first_skill_count = first_skill_embeddings.count()
    first_job_count = first_job_embeddings.count()

    assert first_skill_count == 2
    assert first_job_count == 2

    retry_metadata = tmp_path / "feature-retry-metadata"

    retry_stage = FeatureStage(
        feature_ctx=feature_ctx,
        datasets=dataset_registry,
        partition_manager=PartitionManager(
            ProcessingLedger(spark, str(retry_metadata))
        ),
    )
    retry_stage.execute()

    assert model_load_count == 1

    retried_skill_embeddings = skill_embeddings.read_all(spark)
    retried_job_embeddings = job_embeddings.read_all(spark)

    assert retried_skill_embeddings.count() == first_skill_count
    assert retried_job_embeddings.count() == first_job_count

    assert (
        retried_skill_embeddings.select(
            "skill_id",
            "model_version",
        )
        .distinct()
        .count()
        == first_skill_count
    )

    assert (
        retried_job_embeddings.select(
            "job_id",
            "model_version",
        )
        .distinct()
        .count()
        == first_job_count
    )

    assert retried_skill_embeddings.select("generated_at").distinct().count() == 1
    assert retried_job_embeddings.select("generated_at").distinct().count() == 1
