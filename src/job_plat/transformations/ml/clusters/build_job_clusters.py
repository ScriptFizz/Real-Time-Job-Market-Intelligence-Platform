import json
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime
from hashlib import sha256

import numpy as np
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import array_to_vector, vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    expr,
    lit,
    max as spark_max,
    min as spark_min,
    sha2,
    sum as spark_sum,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from job_plat.storage.paths import join_storage_path

MODEL_NAME = "job_clustering"


def build_training_run_id(
    *,
    model_version: str,
    training_ts: datetime,
    k_values: Iterable[int],
    seed: int,
    training_data_fingerprint: str = "",
) -> str:
    if training_ts.tzinfo is None:
        raise ValueError("training_ts must be timezone-aware")

    identity = json.dumps(
        {
            "model_name": MODEL_NAME,
            "model_version": model_version,
            "training_ts": training_ts.astimezone(UTC).isoformat(),
            "k_values": sorted(k_values),
            "seed": seed,
            "training_data_fingerprint": training_data_fingerprint,
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    return str(uuid.uuid5(uuid.NAMESPACE_URL, identity))


def build_training_data_fingerprint(training_df: DataFrame) -> str:
    """Build a compact, deterministic fingerprint without collecting training rows."""
    row_hashes = training_df.select(
        sha2(
            concat_ws(
                "|",
                col("job_id").cast("string"),
                col("embedding_normalized").cast("string"),
                col("embedding_dim").cast("string"),
            ),
            256,
        ).alias("row_hash")
    )
    summary = row_hashes.agg(
        count("*").alias("row_count"),
        spark_min("row_hash").alias("minimum_hash"),
        spark_max("row_hash").alias("maximum_hash"),
        spark_sum(expr("pmod(xxhash64(row_hash), 1000000007)")).alias("hash_sum"),
    ).first()
    if summary is None or summary.row_count == 0:
        raise ValueError("Cannot fingerprint an empty training dataset")

    canonical_summary = json.dumps(
        {
            "row_count": summary.row_count,
            "minimum_hash": summary.minimum_hash,
            "maximum_hash": summary.maximum_hash,
            "hash_sum": summary.hash_sum,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(canonical_summary.encode("utf-8")).hexdigest()


def resolve_active_model_id(metadata_df: DataFrame) -> str | None:
    """Return the newest explicitly promoted model; candidates are never active."""
    active = (
        metadata_df.filter(col("lifecycle_status") == "promoted")
        .orderBy(col("promoted_at").desc(), col("model_id").desc())
        .select("model_id")
        .first()
    )
    return str(active.model_id) if active is not None else None


def find_optimal_fit(
    df: DataFrame,
    k_values: Iterable[int],
    *,
    seed: int = 42,
) -> tuple[int, float, KMeansModel, DataFrame]:
    candidates = tuple(k_values)

    if not candidates:
        raise ValueError("At least one candidate k value is required")

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster_id",
        metricName="silhouette",
        distanceMeasure="cosine",
    )

    best_k: int | None = None
    best_score = float("-inf")
    best_model: KMeansModel | None = None
    best_predictions: DataFrame | None = None

    for k in candidates:
        kmeans = KMeans(
            k=k, seed=seed, featuresCol="features", predictionCol="cluster_id"
        )

        model = kmeans.fit(df)
        predictions = model.transform(df)

        score = evaluator.evaluate(predictions)

        if score > best_score:
            best_score = score
            best_k = k
            best_model = model
            best_predictions = predictions

    if best_k is None or best_model is None or best_predictions is None:
        raise ValueError("No valid clustering model could be selected")

    return best_k, best_score, best_model, best_predictions


# -----------------------------------


def build_job_clusters(
    spark: SparkSession,
    job_embeddings_df: DataFrame,
    *,
    training_ts: datetime,
    model_version: str = "v1",
    k_values: Iterable[int] = (10, 15, 20, 25, 30),
    seed: int = 42,
    artifact_root: str | None = None,
    promote_model: bool = False,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    candidate_k_values = tuple(k_values)

    # Filter valid embeddings
    training_df = (
        job_embeddings_df.filter(col("embedding_normalized").isNotNull())
        .filter(~expr("exists(embedding_normalized, x -> isnan(x) OR x IS NULL)"))
        .filter(col("embedding_dim") > 0)
        .withColumn(
            "features",
            array_to_vector(col("embedding_normalized")),
        )
        .cache()
    )

    try:
        training_size = training_df.count()

        if training_size == 0:
            raise ValueError("No embeddings available for clustering.")

        training_data_fingerprint = build_training_data_fingerprint(training_df)
        model_id = build_training_run_id(
            model_version=model_version,
            training_ts=training_ts,
            k_values=candidate_k_values,
            seed=seed,
            training_data_fingerprint=training_data_fingerprint,
        )

        valid_k_values = tuple(k for k in candidate_k_values if 2 <= k <= training_size)

        if not valid_k_values:
            raise ValueError(
                f"No valid k values for clustering: training_size={training_size}"
            )

        k, silhouette_score, model, predictions = find_optimal_fit(
            df=training_df,
            k_values=valid_k_values,
            seed=seed,
        )

        artifact_uri = ""
        if artifact_root is not None:
            artifact_uri = join_storage_path(artifact_root, model_id)
            model.write().overwrite().save(artifact_uri)

        # Compute distance to centroid (cosine-style for normalized embeddings)
        centroids = model.clusterCenters()

        def cosine_distance(vec, cluster_id):
            centroid = centroids[cluster_id]
            return float(1 - float(np.dot(vec, centroid)))

        cosine_distance_udf = udf(cosine_distance, DoubleType())

        predictions = predictions.withColumn(
            "distance_to_centroid",
            cosine_distance_udf(vector_to_array(col("features")), col("cluster_id")),
        )

        # Membership table
        membership_df = (
            predictions.select("job_id", "cluster_id", "distance_to_centroid")
            .withColumn("model_id", lit(model_id))
            .withColumn("model_version", lit(model_version))
            .withColumn("assigned_at", lit(training_ts))
        )

        # Cluster statistics
        cluster_df = (
            membership_df.groupBy("cluster_id")
            .agg(
                count("job_id").alias("cluster_size"),
                avg("distance_to_centroid").alias("avg_distance_to_centroid"),
            )
            .withColumn("model_id", lit(model_id))
            .withColumn("model_version", lit(model_version))
            .withColumn("created_at", lit(training_ts))
        )

        # Store centroids
        centroids_data = [
            (
                idx,
                model_id,
                model_version,
                centroid.tolist(),
                len(centroid),
                training_ts,
            )
            for idx, centroid in enumerate(centroids)
        ]

        centroids_schema = StructType(
            [
                StructField("cluster_id", IntegerType(), False),
                StructField("model_id", StringType(), False),
                StructField("model_version", StringType(), False),
                StructField("centroid_vector", ArrayType(DoubleType()), False),
                StructField("embedding_dim", IntegerType(), False),
                StructField("created_at", TimestampType(), False),
            ]
        )

        centroids_df = spark.createDataFrame(centroids_data, schema=centroids_schema)

        # Metadata table
        metadata_schema = StructType(
            [
                StructField("model_id", StringType(), False),
                StructField("model_name", StringType(), False),
                StructField("model_version", StringType(), False),
                StructField("algorithm", StringType(), False),
                StructField("hyperparameters", StringType(), False),
                StructField("training_size", IntegerType(), False),
                StructField("training_data_fingerprint", StringType(), False),
                StructField("artifact_uri", StringType(), False),
                StructField("lifecycle_status", StringType(), False),
                StructField("promoted_at", TimestampType(), True),
                StructField("silhouette_score", DoubleType(), False),
                StructField("created_at", TimestampType(), False),
            ]
        )
        metadata_df = spark.createDataFrame(
            [
                (
                    model_id,
                    MODEL_NAME,
                    model_version,
                    "spark_ml_kmeans",
                    json.dumps({"k": k, "seed": seed}, sort_keys=True),
                    training_size,
                    training_data_fingerprint,
                    artifact_uri,
                    "promoted" if promote_model else "candidate",
                    training_ts if promote_model else None,
                    float(silhouette_score),
                    training_ts,
                )
            ],
            metadata_schema,
        )

        return membership_df, cluster_df, centroids_df, metadata_df

    finally:
        training_df.unpersist()
