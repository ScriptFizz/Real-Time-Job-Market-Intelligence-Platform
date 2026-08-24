from datetime import UTC, datetime

from job_plat.context.contexts import MLContext
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.processing_ledger import ProcessingLedger
from job_plat.pipeline.datasets.dataset_definitions import (
    FeatureJobEmbeddings,
    FeatureSkillEmbeddings,
    MLJobCentroids,
    MLJobClusterMetadata,
    MLJobClusters,
    MLJobMembership,
)
from job_plat.pipeline.stages.ml.ml_stage import MLStage
from job_plat.transformations.ml.clusters.build_job_clusters import (
    build_job_clusters,
)


def test_ml_stage_retry_is_idempotent(
    spark,
    dataset_registry,
    partition_manager,
    monkeypatch,
    tmp_path,
):
    training_ts = datetime(2025, 3, 3, tzinfo=UTC)

    skill_embeddings_df = spark.createDataFrame(
        [
            (
                "skill-python",
                "v1",
                training_ts,
            ),
            (
                "skill-spark",
                "v1",
                training_ts,
            ),
        ],
        [
            "skill_id",
            "model_version",
            "generated_at",
        ],
    )

    job_embeddings_df = spark.createDataFrame(
        [
            (
                "job-1",
                [1.0, 0.0],
                2,
                "v1",
                training_ts,
            ),
            (
                "job-2",
                [0.9, 0.1],
                2,
                "v1",
                training_ts,
            ),
            (
                "job-3",
                [0.0, 1.0],
                2,
                "v1",
                training_ts,
            ),
            (
                "job-4",
                [0.1, 0.9],
                2,
                "v1",
                training_ts,
            ),
        ],
        [
            "job_id",
            "embedding_normalized",
            "embedding_dim",
            "model_version",
            "generated_at",
        ],
    )

    dataset_registry.get(FeatureSkillEmbeddings).write(
        skill_embeddings_df,
    )
    dataset_registry.get(FeatureJobEmbeddings).write(
        job_embeddings_df,
    )

    def build_two_cluster_model(
        *,
        spark,
        job_embeddings_df,
        training_ts,
    ):
        return build_job_clusters(
            spark=spark,
            job_embeddings_df=job_embeddings_df,
            training_ts=training_ts,
            k_values=(2,),
        )

    monkeypatch.setattr(
        "job_plat.pipeline.stages.ml.ml_stage.build_job_clusters",
        build_two_cluster_model,
    )

    ml_ctx = MLContext(
        spark=spark,
        execution_date=training_ts,
        min_clusters=2,
        min_silhouette=-1.0,
    )

    stage = MLStage(
        ml_ctx=ml_ctx,
        datasets=dataset_registry,
        partition_manager=partition_manager,
    )
    stage.execute()

    output_datasets = {
        "membership": dataset_registry.get(MLJobMembership),
        "clusters": dataset_registry.get(MLJobClusters),
        "centroids": dataset_registry.get(MLJobCentroids),
        "metadata": dataset_registry.get(MLJobClusterMetadata),
    }

    first_counts = {
        name: dataset.read_all(spark).count()
        for name, dataset in output_datasets.items()
    }

    assert first_counts == {
        "membership": 4,
        "clusters": 2,
        "centroids": 2,
        "metadata": 1,
    }

    first_model_ids = {
        row.model_id
        for dataset in output_datasets.values()
        for row in dataset.read_all(spark).select("model_id").distinct().collect()
    }

    assert len(first_model_ids) == 1

    retry_metadata = tmp_path / "ml-retry-metadata"

    retry_stage = MLStage(
        ml_ctx=ml_ctx,
        datasets=dataset_registry,
        partition_manager=PartitionManager(
            ProcessingLedger(spark, str(retry_metadata))
        ),
    )
    retry_stage.execute()

    retried_counts = {
        name: dataset.read_all(spark).count()
        for name, dataset in output_datasets.items()
    }

    assert retried_counts == first_counts

    retried_model_ids = {
        row.model_id
        for dataset in output_datasets.values()
        for row in dataset.read_all(spark).select("model_id").distinct().collect()
    }

    assert retried_model_ids == first_model_ids

    membership = output_datasets["membership"].read_all(spark)
    clusters = output_datasets["clusters"].read_all(spark)
    centroids = output_datasets["centroids"].read_all(spark)
    metadata = output_datasets["metadata"].read_all(spark)

    assert (
        membership.select("model_id", "job_id").distinct().count()
        == first_counts["membership"]
    )
    assert (
        clusters.select("model_id", "cluster_id").distinct().count()
        == first_counts["clusters"]
    )
    assert (
        centroids.select("model_id", "cluster_id").distinct().count()
        == first_counts["centroids"]
    )
    assert metadata.select("model_id").distinct().count() == first_counts["metadata"]
