from job_plat.context.contexts import FeatureContext, StageExecutionContext
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.core.base_stage import (
    BaseStage,
    StageInputs,
)
from job_plat.pipeline.core.read_strategy import TimeWindowReadStrategy
from job_plat.pipeline.datasets.dataset_definitions import (
    FeatureJobEmbeddings,
    FeatureSkillEmbeddings,
    GoldDimJobs,
    GoldDimSkills,
    GoldFactJobSkills,
)
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from job_plat.schemas.output_schemas import FeatureOutputs
from job_plat.transformations.feature.embeddings.build_job_embeddings import (
    build_job_embeddings,
)
from job_plat.transformations.feature.embeddings.build_skill_embeddings import (
    build_skill_embeddings,
)
from job_plat.utils.helpers import StageSkip


class FeatureStage(BaseStage[FeatureContext, FeatureOutputs]):
    STAGE_NAME = "feature"
    ALLOW_EMPTY_OUTPUTS = True
    INPUT_MAP = {
        "dim_jobs_df": GoldDimJobs,
        "dim_skills_df": GoldDimSkills,
        "fact_job_skill_df": GoldFactJobSkills,
    }
    OUTPUT_TYPE = FeatureOutputs

    def __init__(
        self,
        # gold_ctx: GoldContext,
        feature_ctx: FeatureContext,
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,
    ):
        super().__init__(
            datasets=datasets, partition_manager=partition_manager, ctx=feature_ctx
        )
        self.READ_STRATEGY = TimeWindowReadStrategy(window_days=self.ctx.window_days)

    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage=self.STAGE_NAME, pipeline_version="1.0.0"
        )
        return run_context

    def transform(self, inputs: StageInputs) -> FeatureOutputs:
        dim_jobs_df = inputs.get("dim_jobs_df")
        dim_skills_df = inputs.get("dim_skills_df")
        fact_job_skill_df = inputs.get("fact_job_skill_df")

        if dim_jobs_df is None or dim_skills_df is None or fact_job_skill_df is None:
            raise StageSkip("no input data available for feature generation")

        execution_date = self.ctx.execution_date

        if execution_date is None:
            raise ValueError(
                "FeatureStage requires execution_date for deterministic output identity"
            )

        self.logger.info("building_skill_embeddings")
        skill_dataset = self.datasets.get(FeatureSkillEmbeddings)
        existing_skill_embeddings = (
            skill_dataset.read_all(self.spark)
            if skill_dataset.storage.exists(skill_dataset.path)
            else None
        )
        skill_embeddings_df = build_skill_embeddings(
            dim_skills_df=dim_skills_df,
            spark=self.spark,
            generated_at=execution_date,
            existing_embeddings_df=existing_skill_embeddings,
            model_name=self.ctx.embedding_model_name,
            model_version=self.ctx.embedding_model_version,
            model_provider=self.ctx.embedding_model_provider,
            batch_size=self.ctx.embedding_batch_size,
            max_driver_skills=self.ctx.max_driver_skills,
        )

        available_skill_embeddings = skill_embeddings_df
        if existing_skill_embeddings is not None:
            available_skill_embeddings = existing_skill_embeddings.unionByName(
                skill_embeddings_df
            )

        self.logger.info("building_job_embeddings")
        job_dataset = self.datasets.get(FeatureJobEmbeddings)
        existing_job_embeddings = (
            job_dataset.read_all(self.spark)
            if job_dataset.storage.exists(job_dataset.path)
            else None
        )
        job_embeddings_df = build_job_embeddings(
            fact_job_skill_df=fact_job_skill_df,
            skill_embeddings_df=available_skill_embeddings,
            generated_at=execution_date,
            existing_embeddings_df=existing_job_embeddings,
            model_version=self.ctx.embedding_model_version,
            aggregation_method=self.ctx.job_embedding_aggregation,
        )

        return FeatureOutputs(
            skill_embeddings=skill_embeddings_df,
            job_embeddings=job_embeddings_df,
        )

    def compute_metrics(self, outputs: FeatureOutputs) -> dict:
        if not outputs:
            return {}

        skill_embeddings_df = outputs.skill_embeddings
        job_embeddings_df = outputs.job_embeddings

        # Counts
        skills_embedded = skill_embeddings_df.count()
        jobs_embedded = job_embeddings_df.count()

        return {
            "skills_embedded": skills_embedded,
            "jobs_embedded": jobs_embedded,
        }

    def evaluate_metrics(self, metrics: dict) -> None:
        if metrics["jobs_embedded"] == 0:
            self.logger.info("no_new_job_embeddings")
