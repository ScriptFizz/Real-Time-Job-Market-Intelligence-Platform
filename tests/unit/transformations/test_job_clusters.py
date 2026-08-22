import json
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame

from job_plat.transformations.ml.clusters.build_job_clusters import (
    build_job_clusters,
    find_optimal_fit,
)


def test_find_optimal_fir_rejects_empty_candidates():
    dataframe = MagicMock()

    with pytest.raises(
        ValueError,
        match="At least one candidate k value",
    ):
        find_optimal_fit(dataframe, [])


def test_build_job_clusters_produces_expected_output(spark):
    embeddings = spark.createDataFrame(
        [
            ("job-1", [1.0, 0.0], 2),
            ("job-2", [0.9, 0.1], 2),
            ("job-3", [0.0, 1.0], 2),
            ("job-4", [0.1, 0.9], 2),
        ],
        [
            "job_id",
            "embedding_normalized",
            "embedding_dim",
        ],
    )

    membership, clusters, centroids, metadata = build_job_clusters(
        spark=spark,
        job_embeddings_df=embeddings,
        k_values=(2,),
    )

    assert membership.count() == 4
    assert clusters.count() == 2
    assert centroids.count() == 2

    membership_columns = set(membership.columns)
    assert {
        "job_id",
        "cluster_id",
        "distance_to_centroid",
        "model_id",
        "model_version",
        "assigned_at",
    } <= membership_columns

    metadata_row = metadata.first()

    assert metadata_row is not None
    assert metadata_row.training_size == 4
    assert metadata_row.silhouette_score is not None


def test_build_job_clusters_rejects_k_larger_than_training_set(spark):
    embeddings = spark.createDataFrame(
        [
            ("job-1", [1.0, 0.0], 2),
            ("job-2", [0.0, 1.0], 2),
        ],
        [
            "job_id",
            "embedding_normalized",
            "embedding_dim",
        ],
    )

    with pytest.raises(
        ValueError,
        match="No valid k values",
    ):
        build_job_clusters(
            spark=spark,
            job_embeddings_df=embeddings,
            k_values=(3,),
        )


def test_build_job_clusters_ignores_k_larger_than_training_set(spark):
    embeddings = spark.createDataFrame(
        [
            ("job-1", [1.0, 0.0], 2),
            ("job-2", [0.9, 0.1], 2),
            ("job-3", [0.0, 1.0], 2),
            ("job-4", [0.1, 0.9], 2),
        ],
        [
            "job_id",
            "embedding_normalized",
            "embedding_dim",
        ],
    )

    _, clusters, _, metadata = build_job_clusters(
        spark=spark,
        job_embeddings_df=embeddings,
        k_values=(2, 100),
    )

    assert clusters.count() == 2

    metadata_row = metadata.first()
    assert metadata_row is not None

    hyperparameters = json.loads(metadata_row.hyperparameters)

    assert hyperparameters["k"] == 2


def test_build_job_clusters_unpersists_training_data_on_failure(
    spark,
    monkeypatch,
):
    embeddings = spark.createDataFrame(
        [
            ("job-1", [1.0, 0.0], 2),
            ("job-2", [0.0, 1.0], 2),
        ],
        [
            "job_id",
            "embedding_normalized",
            "embedding_dim",
        ],
    )

    unpersisted: list[DataFrame] = []
    original_unpersist = DataFrame.unpersist

    def track_unpersist(
        dataframe: DataFrame,
        blocking: bool = False,
    ) -> DataFrame:
        unpersisted.append(dataframe)
        return original_unpersist(
            dataframe,
            blocking=blocking,
        )

    monkeypatch.setattr(
        DataFrame,
        "unpersist",
        track_unpersist,
    )

    with pytest.raises(
        ValueError,
        match="No valid k values",
    ):
        build_job_clusters(
            spark=spark,
            job_embeddings_df=embeddings,
            k_values=(3,),
        )

    assert len(unpersisted) == 1
