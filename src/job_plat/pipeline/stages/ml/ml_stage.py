from job_plat.pipeline.core.base_stage import BaseStage
from job_plat.context.contexts import FeatureContext, MLContext
from job_plat.transformations.ml.clusters.build_job_clusters import build_job_clusters
from job_plat.storage.storages import Storage
from pyspark.sql import DataFrame
from job_plat.ingestion.metadata import StageExecutionContext
from pyspark.sql.functions import countDistinct, avg
from job_plat.schemas.output_schemas import MLOutputs
from job_plat.pipeline.datasets.dataset_definitions import FeatureJobEmbeddings, FeatureSkillEmbeddings
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.partitioning.partition_manager import PartitionManager


class MLStage(BaseStage):
    
    STAGE_NAME="gold_v2"
    INPUT_MAP = {"job_embeddings_df": FeatureJobEmbeddings, "skill_embeddings_df": FeatureSkillEmbeddings}
    OUTPUT_TYPE = MLOutputs
    
    def __init__(
        self, 
        #feature_ctx: FeatureContext, 
        ml_ctx: MLContext,
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,):
        super().__init__(spark=ml_ctx.spark, datasets=datasets, partition_manager=partition_manager)
        #self.feature_ctx = feature_ctx
        self.ml_ctx = ml_ctx
        
    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage=self.STAGE_NAME,
            pipeline_version="1.0.0"
        )
        return run_context
    
    def transform(
        self, 
        job_embeddings_df: DataFrame,
        skill_embeddings_df: DataFrame,
        ) -> MLOutputs:
        
        
        self.logger.info("building_clusters_data")
        job_membership_df, job_clusters_df, job_centroids_df, job_metadata_df = build_job_clusters(spark=self.ml_ctx.spark, job_embeddings_df=job_embeddings_df)
        
        return MLOutputs(
            job_clusters=job_clusters_df,
            job_membership=job_membership_df,
            job_centroids=job_centroids_df,
            job_cluster_metadata=job_metadata_df
        )

    def compute_metrics(self, outputs: MLOutputs) -> dict:
        
        if not outputs:
            return {}

        job_clusters_df = outputs.job_clusters
        job_membership_df = outputs.job_membership
        job_metadata_df = outputs.job_cluster_metadata
        
        # Cache 
        job_membership_df.cache()
        job_clusters_df.cache()
        
        # Cluster metrics
        cluster_stats = (
            job_membership_df
            .groupBy()
            .agg(
                countDistinct("cluster_id").alias("num_clusters"),
                avg("distance_to_centroid").alias("avg_distance")
            )
            .first()
        )
        
        # Silhouette score from metadata
        silhouette_row = job_metadata_df.select("silhouette_score").first()
        silhouette = silhouette_row["silhouette_score"] if silhouette_row else None
        
        job_membership_df.unpersist()
        job_clusters_df.unpersist()
        
        return {
            "num_clusters": cluster_stats["num_clusters"],
            "avg_distance_to_centroid": cluster_stats["avg_distance"],
            "silhouette_score": silhouette,
            }
    
    def evaluate_metrics(self, metrics: dict) -> None:
        
        if metrics["num_clusters"] < self.ml_ctx.min_clusters:
            self.logger.warning(
                "model_issue",
                extra={
                    "issue": "too_few_clusters",
                    "num_clusters": metrics["num_clusters"],
                },
            )
        
        if metrics["silhouette_score"] is not None and metrics["silhouette_score"] < self.ml_ctx.min_silhouette:
            self.logger.warning(
                "model_quality_degraded",
                extra={
                    "silhouette_score": metrics["silhouette_score"],
                    "threshold": self.ml_ctx.min_silhouette,
                },
            )


