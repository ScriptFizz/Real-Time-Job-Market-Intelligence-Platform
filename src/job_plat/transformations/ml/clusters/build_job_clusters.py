from typing import Tuple, Iterable
from datetime import datetime
import uuid
import json
import numpy as np

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, avg, lit, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.functions import vector_to_array, array_to_vector
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def find_optimal_fit(
    df: DataFrame, 
    k_values: Iterable[int] #=(10,15,20,25,30)
    ) -> Tuple[int, float, KMeansModel, DataFrame]:

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster_id",
        metricName="silhouette",
        distanceMeasure="cosine"
    )

    best_k = None
    best_score = -1
    best_model = None
    best_predictions = None

    for k in k_values:
        kmeans = KMeans(
            k=k,
            seed=42,
            featuresCol="features",
            predictionCol="cluster_id"
        )

        model = kmeans.fit(df)
        predictions = model.transform(df)

        score = evaluator.evaluate(predictions)

        if score > best_score:
            best_score = score
            best_k = k
            best_model = model
            best_predictions = predictions

    return best_k, best_score, best_model, best_predictions

#-----------------------------------

def build_job_clusters(
    spark: SparkSession,
    job_embeddings_df: DataFrame,
    model_version: str = "v1",
    k_values: Iterable[int] =(10,15,20,25,30)
) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    
    training_ts = datetime.utcnow()
    model_id = str(uuid.uuid4())
    
    # Filter valid embeddings
    df = job_embeddings_df.filter(
        col("embedding_normalized").isNotNull()
    ).filter(
        ~expr("exists(embedding_normalized, x -> isnan(x) OR x IS NULL)")
    )
    
    df = df.filter(col("embedding_dim") > 0)
    
    if df.rdd.isEmpty():
        raise ValueError("No embeddings available for clustering.")
    
    df = df.withColumn("features", array_to_vector("embedding_normalized"))
    
    df = df.cache()
    training_size = df.count()
    
    k, silhouette_score, model, predictions = find_optimal_fit(df=df, k_values=k_values)
    
    # Compute distance to centroid (cosine-style for normalized embeddings)
    centroids = model.clusterCenters()
    
    def cosine_distance(vec, cluster_id):
        centroid = centroids[cluster_id]
        return float( 1 - float(np.dot(vec, centroid)))
    
    cosine_distance_udf = udf(cosine_distance, DoubleType())
    
    predictions = predictions.withColumn(
        "distance_to_centroid",
        cosine_distance_udf(
            vector_to_array("features"),
            col("cluster_id")
        )
    )
        
    # Membership table
    membership_df = (
        predictions.select(
            "job_id",
            "cluster_id",
            "distance_to_centroid"
        )
        .withColumn("model_id", lit(model_id))
        .withColumn("model_version", lit(model_version))
        .withColumn("assigned_at", lit(training_ts))
    )
    
    # Cluster statistics
    cluster_df = (
        membership_df
        .groupBy("cluster_id")
        .agg(
            count("job_id").alias("cluster_size"),
            avg("distance_to_centroid").alias("avg_distance_to_centroid")
        )
        .withColumn("model_id", lit(model_id))
        .withColumn("model_version", lit(model_version))
        .withColumn("created_at", lit(training_ts))
    )
    
    # Store centroids
    centroids_data = [(
        idx,
        model_id,
        model_version,
        centroid.tolist(),
        len(centroid),
        training_ts
    )
    for idx, centroid in enumerate(centroids)
    ]
    
    centroids_schema = StructType([
        StructField("cluster_id", IntegerType(), False),
        StructField("model_id", StringType(), False),
        StructField("model_version", StringType(), False),
        StructField("centroid_vector", ArrayType(DoubleType()), False),
        StructField("embedding_dim", IntegerType(), False),
        StructField("created_at", TimestampType(), False),
    ])
    
    centroids_df = spark.createDataFrame(
        centroids_data,
        schema=centroids_schema
    )
    
    # # Silhouette score
    # evaluator = ClusteringEvaluator(
        # featuresCol="features",
        # predictionCol="cluster_id",
        # metricName="silhouette",
        # distanceMeasure="cosine"
    # )
    
    # silhouette_score = evaluator.evaluate(predictions)
    
    # Metadata table
    metadata_df = spark.createDataFrame([{
        "model_id": model_id,
        "model_name": "job_clustering",
        "model_version": model_version,
        "algorithm": "spark_ml_kmeans",
        "hyperparameters": json.dumps({
            "k": k,
            "seed": 42
        }),
        "training_size": training_size,
        "silhouette_score": float(silhouette_score),
        "created_at": training_ts
    }])
    
    return membership_df, cluster_df, centroids_df, metadata_df



############################# 09-03

# def build_job_clusters(
    # spark: SparkSession,
    # job_embeddings_df: DataFrame,
    # n_clusters: int = 20,
    # model_version: str = "v1"
# ) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    
    # training_ts = datetime.utcnow()
    # model_id = str(uuid.uuid4())
    
    # # Filter valid embeddings
    # df = job_embeddings_df.filter(
        # col("embedding_normalized").isNotNull()
    # ).filter(
        # ~expr("exists(embedding_normalized, x -> isnan(x) OR x IS NULL)")
    # )
    
    # df = df.filter(col("embedding_dim") > 0)
    
    # if df.rdd.isEmpty():
        # raise ValueError("No embeddings available for clustering.")
    
    # # Convert ARRAY<FLOAT> -> VectorUDT
    # # to_vector_udf = udf(lambda x: Vectors.dense(x), VectorUDT())
    
    # # df = df.withColumn(
        # # "features",
        # # to_vector_udf(col("embedding_normalized"))
    # # )
    
    # df = df.withColumn("features", array_to_vector("embedding_normalized"))
    # ############################
    
    
    # # Train Spark KMeans
    # kmeans = KMeans(
        # k = n_clusters,
        # seed = 42,
        # featuresCol = "features",
        # predictionCol = "cluster_id"
    # )
    
    # model = kmeans.fit(df)
    # predictions = model.transform(df)
    
    # # Compute distance to centroid (cosine-style for normalized embeddings)
    # centroids = model.clusterCenters()
    
    # def cosine_distance(vec, cluster_id):
        # centroid = centroids[cluster_id]
        # return float( 1 - float(np.dot(vec, centroid)))
    
    # cosine_distance_udf = udf(cosine_distance, DoubleType())
    
    # predictions = predictions.withColumn(
        # "distance_to_centroid",
        # cosine_distance_udf(
            # vector_to_array("features"),
            # col("cluster_id")
        # )
    # )
        
    # # Membership table
    # membership_df = (
        # predictions.select(
            # "job_id",
            # "cluster_id",
            # "distance_to_centroid"
        # )
        # .withColumn("model_id", lit(model_id))
        # .withColumn("model_version", lit(model_version))
        # .withColumn("assigned_at", lit(training_ts))
    # )
    
    # # Cluster statistics
    # cluster_df = (
        # membership_df
        # .groupBy("cluster_id")
        # .agg(
            # count("job_id").alias("cluster_size"),
            # avg("distance_to_centroid").alias("avg_distance_to_centroid")
        # )
        # .withColumn("model_id", lit(model_id))
        # .withColumn("model_version", lit(model_version))
        # .withColumn("created_at", lit(training_ts))
    # )
    
    # # Store centroids
    # centroids_data = [(
        # idx,
        # model_id,
        # model_version,
        # centroid.tolist(),
        # len(centroid),
        # training_ts
    # )
    # for idx, centroid in enumerate(centroids)
    # ]
    
    # centroids_schema = StructType([
        # StructField("cluster_id", IntegerType(), False),
        # StructField("model_id", StringType(), False),
        # StructField("model_version", StringType(), False),
        # StructField("centroid_vector", ArrayType(DoubleType()), False),
        # StructField("embedding_dim", IntegerType(), False),
        # StructField("created_at", TimestampType(), False),
    # ])
    
    # centroids_df = spark.createDataFrame(
        # centroids_data,
        # schema=centroids_schema
    # )
    
    # # Silhouette score
    # evaluator = ClusteringEvaluator(
        # featuresCol="features",
        # predictionCol="cluster_id",
        # metricName="silhouette",
        # distanceMeasure="cosine"
    # )
    
    # silhouette_score = evaluator.evaluate(predictions)
    
    # # Metadata table
    # metadata_df = spark.createDataFrame([{
        # "model_id": model_id,
        # "model_name": "job_clustering",
        # "model_version": model_version,
        # "algorithm": "spark_ml_kmeans",
        # "hyperparameters": json.dumps({
            # "k": n_clusters,
            # "seed": 42
        # }),
        # "training_size": df.count(),
        # "silhouette_score": float(silhouette_score),
        # "created_at": training_ts
    # }])
    
    # return membership_df, cluster_df, centroids_df, metadata_df
    
##################################################################
    
# def build_job_clusters(
    # spark: SparkSession,
    # job_embeddings_df: DataFrame,
    # n_clusters: int,
    # model_version: str
# ) -> Tuple[DataFrame, DataFrame, DataFrame]:
    
    # training_ts = datetime.utcnow()
    # latest_embeddings_df = job_embeddings_df.filter(
        # col("job_embedding_normalized").isNotNull()
    # )
    
    # data = (
        # latest_embeddings_df
        # .select("job_id", "job_embedding_normalized")
        # .collect()
    # )
    
    # if len(data) == 0:
        # raise ValueError("No embeddings available for clustering.")
        
    # job_ids = [row["job_id"] for row in data]
    # vectors = np.array([row["job_embedding_normalized"] for row in data])
    
    # if len(vectors) < n_clusters:
        # n_clusters = len(vectors)
        
    # kmeans = KMeans(
        # n_clusters = n_clusters,
        # random_state = 42,
        # n_init = "auto"
    # )
    
    # labels = kmeans.fit_predict(vectors)
    # centroids = kmeans.cluster_centers_
    
    # # Compute distances from centroid
    # distances = norm(
        # vectors - centroids[labels],
        # axis=1
    # )
    
    # model_id = str(uuid.uuid4())
    
    # membership_pd = pd.DataFrame({
        # "job_id": job_ids,
        # "cluster_id": labels,
        # "distance_to_centroid": distances,
        # "model_id": model_id,
        # "model_version": model_version,
        # "assigned_at": training_ts
    # })
    
    # membership_df = spark.createDataFrame(membership_pd)
    
    # cluster_df = (
        # membership_df
        # .groupBy("cluster_id")
        # .agg(
            # count("job_id").alias("cluster_size"),
            # avg("distance_to_centroid").alias("avg_distance_to_centroid")
        # )
        # .withColumn("model_id", lit(model_id))
        # .withColumn("model_version", lit(model_version))
        # .withColumn("created_at", lit(training_ts)
    # )
    
    # sil_score = silhouette_score(vectors, labels)
    
    
    # metadata = {
        # "model_id": model_id,
        # "model_name": "job_clustering",
        # "model_version": model_version,
        # "algorithm": "sklearn_kmeans",
        # "hyperparameters": json.dumps({
            # "no_clusters": n_clusters,
            # "random_state": 42
        # }),
        # "training_size": len(vectors),
        # "created_at": training_ts,
        # "silhouette_score": float(sil_score)
    # }
    
    # metadata_df = spark.createDataFrame([metadata])
    
    # return membership_df, clusters_df, metadata_df
    

