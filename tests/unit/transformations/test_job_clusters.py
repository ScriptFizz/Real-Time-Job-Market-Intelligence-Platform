import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from pyspark.ml.clustering import KMeansModel
from pyspark.sql import DataFrame

from job_plat.transformations.ml.clusters.build_job_clusters import (
    build_job_clusters,
    build_training_data_fingerprint,
    build_training_run_id,
    find_optimal_fit,
    resolve_active_model_id,
)


def test_find_optimal_fir_rejects_empty_candidates():
    dataframe = MagicMock()

    with pytest.raises(
        ValueError,
        match="At least one candidate k value",
    ):
        find_optimal_fit(dataframe, [])


def test_build_job_clusters_produces_expected_output(spark, tmp_path):
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
        training_ts=datetime(2025, 3, 2, tzinfo=UTC),
        artifact_root=str(tmp_path / "models"),
        promote_model=True,
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
    assert metadata_row.training_data_fingerprint
    assert metadata_row.lifecycle_status == "promoted"
    assert metadata_row.promoted_at is not None
    KMeansModel.load(metadata_row.artifact_uri)

    model_ids = {
        row.model_id
        for dataframe in (membership, clusters, centroids, metadata)
        for row in dataframe.select("model_id").distinct().collect()
    }
    assert len(model_ids) == 1

    expected_model_id = build_training_run_id(
        model_version="v1",
        training_ts=datetime(2025, 3, 2, tzinfo=UTC),
        k_values=(2,),
        seed=42,
        training_data_fingerprint=build_training_data_fingerprint(embeddings),
    )

    assert model_ids == {expected_model_id}


def test_training_fingerprint_changes_when_training_data_changes(spark):
    first = spark.createDataFrame(
        [("job-1", [1.0, 0.0], 2)],
        ["job_id", "embedding_normalized", "embedding_dim"],
    )
    second = spark.createDataFrame(
        [("job-1", [0.0, 1.0], 2)],
        ["job_id", "embedding_normalized", "embedding_dim"],
    )

    assert build_training_data_fingerprint(first) != build_training_data_fingerprint(
        second
    )


def test_active_model_is_latest_promoted_not_latest_candidate(spark):
    metadata = spark.createDataFrame(
        [
            ("promoted-old", "promoted", datetime(2025, 3, 1, tzinfo=UTC)),
            ("candidate", "candidate", None),
            ("promoted-new", "promoted", datetime(2025, 3, 2, tzinfo=UTC)),
        ],
        "model_id string, lifecycle_status string, promoted_at timestamp",
    )

    assert resolve_active_model_id(metadata) == "promoted-new"


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
            training_ts=datetime(2025, 3, 2, tzinfo=UTC),
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
        training_ts=datetime(2025, 3, 2, tzinfo=UTC),
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
            training_ts=datetime(2025, 3, 2, tzinfo=UTC),
        )

    assert len(unpersisted) == 1


def test_training_run_id_is_deterministic():
    training_ts = datetime(2025, 3, 2, tzinfo=UTC)

    first = build_training_run_id(
        model_version="v1",
        training_ts=training_ts,
        k_values=(2, 3),
        seed=42,
    )
    second = build_training_run_id(
        model_version="v1",
        training_ts=training_ts,
        k_values=(3, 2),
        seed=42,
    )

    assert first == second


def test_training_run_id_changes_for_new_execution():
    first = build_training_run_id(
        model_version="v1",
        training_ts=datetime(2025, 3, 2, tzinfo=UTC),
        k_values=(2,),
        seed=42,
    )
    second = build_training_run_id(
        model_version="v1",
        training_ts=datetime(2025, 3, 3, tzinfo=UTC),
        k_values=(2,),
        seed=42,
    )

    assert first != second


def test_training_run_id_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="timezone-aware"):
        build_training_run_id(
            model_version="v1",
            training_ts=datetime(2025, 3, 2),
            k_values=(2,),
            seed=42,
        )
