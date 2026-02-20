from datetime import date
from typing import Dict

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
from job_plat.utils.storage import get_storage



def full_pipeline(config: Dict) -> None:
    
    data_date = date.today()
    storage = get_storage(config=config)
    pipeline_ctx = build_pipeline_context(data_date = data_date)
    
    bronze_pipeline(ctx=pipeline_ctx, storage=storage)
    silver_pipeline(ctx=pipeline_ctx, storage=storage)
    gold_v1_pipeline(ctx=pipeline_ctx, storage=storage)
    gold_v2_pipeline(ctx=pipeline_ctx, storage=storage)
