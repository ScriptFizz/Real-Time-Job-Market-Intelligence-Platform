from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, avg, lit
from typing import Tuple
from datetime import datetime
import uuid

import numpy as np
from numpy.linalg import norm

import pandas as pd

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

def build_job_clusters(
    spark: SparkSession,
    job_embeddings_df: DataFrame,
    n_clusters: int,
    model_version: str
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    
    training_ts = datetime.utcnow()
    latest_embeddings_df = job_embeddings_df.filter(
        col("job_embedding_normalized").isNotNull()
    )
    
    data = (
        latest_embeddings_df
        .select("job_id", "job_embedding_normalized")
        .collect()
    )
    
    if len(data) == 0:
        raise ValueError("No embeddings available for clustering.")
        
    job_ids = [row["job_id"] for row in data]
    vectors = np.array([row["job_embedding_normalized"] for row in data])
    
    if len(vectors) < n_clusters:
        n_clusters = len(vectors)
        
    kmeans = KMeans(
        n_clusters = n_clusters,
        random_state = 42,
        n_init = "auto"
    )
    
    labels = kmeans.fit_predict(vectors)
    centroids = kmeans.cluster_centers_
    
    # Compute distances from centroid
    distances = norm(
        vectors - centroids[labels],
        axis=1
    )
    
    model_id = str(uuid.uuid4())
    
    membership_pd = pd.DataFrame({
        "job_id": job_ids,
        "cluster_id": labels,
        "distance_to_centroid": distances,
        "model_id": model_id,
        "model_version": model_version,
        "assigned_at": training_ts
    })
    
    membership_df = spark.createDataFrame(membership_pd)
    
    cluster_df = (
        membership_df
        .groupBy("cluster_id")
        .agg(
            count("job_id").alias("cluster_size"),
            avg("distance_to_centroid").alias("avg_distance_to_centroid")
        )
        .withColumn("model_id", lit(model_id))
        .withColumn("model_version", lit(model_version))
        .withColumn("created_at", lit(training_ts)
    )
    
    sil_score = silhouette_score(vectors, labels)
    
    
    metadata = {
        "model_id": model_id,
        "model_name": "job_clustering",
        "model_version": model_version,
        "algorithm": "sklearn_kmeans",
        "hyperparameters": json.dumps({
            "no_clusters": n_clusters,
            "random_state": 42
        }),
        "training_size": len(vectors),
        "created_at": training_ts,
        "silhouette_score": float(sil_score)
    }
    
    metadata_df = spark.createDataFrame([metadata])
    
    return membership_df, clusters_df
