from datetime import date

from job_plat.pipelines.pipeline_stages import (
    bronze_pipeline,
    silver_pipeline,
    gold_v1_pipeline,
    gold_v2_pipeline
)

from job_plat.config.context.contexts import (
    BronzeContext,
    SilverContext,
    GoldV1Context,
    GoldV2Context,
    PipelineContext
    )

from job_plat.config.context.build_pipeline_context import build_pipeline_context


def full_pipeline() -> None:
    
    data_date = date.today()
    pipeline_ctx = build_pipeline_context(data_date = data_date)
    
    bronze_pipeline(ctx=pipeline_ctx)
    silver_pipeline(ctx=pipeline_ctx)
    gold_v1_pipeline(ctx=pipeline_ctx)
    gold_v2_pipeline(ctx=pipeline_ctx)
