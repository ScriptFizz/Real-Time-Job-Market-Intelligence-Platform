from datetime import date
from pathlib import Path
from job_plat.config.context import GoldV1Context, GoldV2Context, PipelineContext
from job_plat.pipelines.stages.gold_v2_stage import GoldV2Stage

def gold_v2_pipeline(
    ctx: PipelineContext
) -> None:
    stage = GoldV2Stage(
        gold_v1_ctx = ctx.gold_v1,
        gold_v2_ctx = ctx.gold_v2
    )
    
    stage.execute()
