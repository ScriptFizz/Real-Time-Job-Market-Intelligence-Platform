from pyspark.sql.functions import col, count, sum, when

from job_plat.context.contexts import SilverContext, StageExecutionContext
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.core.base_stage import (
    BaseStage,
    Metrics,
    StageInputs,
)
from job_plat.pipeline.datasets.dataset_definitions import BronzeJobs
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.schemas.output_schemas import SilverOutputs
from job_plat.transformations.silver.cleaning.clean_jobs import (
    clean_jobs,
    deduplicate_jobs,
)
from job_plat.transformations.silver.enrichment.build_job_skills import run_job_skills
from job_plat.utils.helpers import StageSkip


class SilverStage(BaseStage[SilverContext, SilverOutputs]):
    STAGE_NAME = "silver"
    INPUT_MAP = {"job_bronze_df": BronzeJobs}
    OUTPUT_TYPE = SilverOutputs

    def __init__(
        self,
        silver_ctx: SilverContext,
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,
    ):
        super().__init__(
            datasets=datasets, partition_manager=partition_manager, ctx=silver_ctx
        )

        self._metrics: Metrics = {}

    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage=self.STAGE_NAME, pipeline_version="1.0.0"
        )
        return run_context

    def transform(self, inputs: StageInputs) -> SilverOutputs:
        job_bronze_df = inputs.get("job_bronze_df")

        # Skip transform if no new partitions
        if job_bronze_df is None:
            raise StageSkip("no new partitions")

        df_normalized = job_bronze_df.select(
            col("run_id"),
            col("ingestion_date"),
            col("payload.*"),
            col("ingestion_metadata.started_at").alias("ingested_at"),
        )

        quality_metrics = df_normalized.agg(
            count("*").alias("total"),
            sum(when(col("job_title_raw").isNull(), 1).otherwise(0)).alias(
                "null_titles"
            ),
            sum(when(col("description_raw").isNull(), 1).otherwise(0)).alias(
                "null_descriptions"
            ),
        ).first()

        if quality_metrics is None:
            raise RuntimeError("Silver quality aggregation returned no result")

        self._metrics = {
            "total": quality_metrics["total"],
            "null_titles": quality_metrics["null_titles"],
            "null_descriptions": quality_metrics["null_descriptions"],
        }

        self.logger.info("building_df_clean")
        df_clean = clean_jobs(df=df_normalized)

        self.logger.info("building_jobs_silver")
        jobs_silver_df = deduplicate_jobs(df_clean)

        self.logger.info("building_job_skills_silver")
        job_skills_silver_df = run_job_skills(jobs_silver_df=jobs_silver_df)

        return SilverOutputs(
            silver_jobs=jobs_silver_df,
            silver_job_skills=job_skills_silver_df,
        )

    def compute_metrics(self, _outputs: SilverOutputs) -> Metrics:
        return self._metrics

    def evaluate_metrics(self, metrics: Metrics) -> None:
        if metrics["null_titles"] > 0:
            self.logger.warning(
                "data_quality_issue", extra={"issue": "null_titles_detected"}
            )

        if metrics["null_descriptions"] > 0:
            self.logger.warning(
                "data_quality_issue", extra={"issue": "null_descriptions_detected"}
            )
