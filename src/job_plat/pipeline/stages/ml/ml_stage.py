from pyspark.sql.functions import avg, countDistinct

from job_plat.context.contexts import MLContext, StageExecutionContext
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.core.base_stage import (
    BaseStage,
    Metrics,
    StageInputs,
)
from job_plat.pipeline.datasets.dataset_definitions import (
    FeatureJobEmbeddings,
    FeatureSkillEmbeddings,
)
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.schemas.output_schemas import MLOutputs
from job_plat.transformations.ml.clusters.build_job_clusters import build_job_clusters
from job_plat.utils.helpers import StageSkip


class MLStage(BaseStage):
    STAGE_NAME = "gold_v2"
    INPUT_MAP = {
        "job_embeddings_df": FeatureJobEmbeddings,
        "skill_embeddings_df": FeatureSkillEmbeddings,
    }
    OUTPUT_TYPE = MLOutputs

    def __init__(
        self,
        ml_ctx: MLContext,
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,
    ):
        super().__init__(
            datasets=datasets, partition_manager=partition_manager, ctx=ml_ctx
        )

    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage=self.STAGE_NAME, pipeline_version="1.0.0"
        )
        return run_context

    def transform(self, inputs: StageInputs) -> MLOutputs:
        job_embeddings_df = inputs.get("job_embeddings_df")
        skill_embeddings_df = inputs.get("skill_embeddings_df")

        if job_embeddings_df is None or skill_embeddings_df is None:
            raise StageSkip("no embeddings available for clustering")

        self.logger.info("building_clusters_data")
        job_membership_df, job_clusters_df, job_centroids_df, job_metadata_df = (
            build_job_clusters(spark=self.spark, job_embeddings_df=job_embeddings_df)
        )

        return MLOutputs(
            job_clusters=job_clusters_df,
            job_membership=job_membership_df,
            job_centroids=job_centroids_df,
            job_cluster_metadata=job_metadata_df,
        )

    def compute_metrics(self, outputs: MLOutputs) -> Metrics:
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
            job_membership_df.groupBy()
            .agg(
                countDistinct("cluster_id").alias("num_clusters"),
                avg("distance_to_centroid").alias("avg_distance"),
            )
            .first()
        )

        if cluster_stats is None:
            raise RuntimeError("ML cluster aggregation returned no result")

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

    def evaluate_metrics(self, metrics: Metrics) -> None:
        if metrics["num_clusters"] < self.ctx.min_clusters:
            self.logger.warning(
                "model_issue",
                extra={
                    "issue": "too_few_clusters",
                    "num_clusters": metrics["num_clusters"],
                },
            )

        if (
            metrics["silhouette_score"] is not None
            and metrics["silhouette_score"] < self.ctx.min_silhouette
        ):
            self.logger.warning(
                "model_quality_degraded",
                extra={
                    "silhouette_score": metrics["silhouette_score"],
                    "threshold": self.ctx.min_silhouette,
                },
            )
