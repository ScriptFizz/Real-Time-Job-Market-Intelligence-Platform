from datetime import date
from job_plat.ingestion.scrape_indeed import scrape_indeed
from job_plat.utils.io import write_jsonl
from pathlib import Path
from job_plat.config.context import BronzeContext, PipelineContext
from job_plat.pipelines.stages import BronzeStage


def bronze_pipeline(ctx: PipelineContext) -> None:
    stage = BronzeStage(
        bronze_ctx = ctx.bronze
    )
    stage.produce()
