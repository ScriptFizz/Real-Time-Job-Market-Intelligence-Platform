from job_plat.pipelines.context.contexts import (
    BronzeContext, 
    SilverContext,
    GoldV1Context,
    GoldV2Context,
    PipelineContext
    )
from job_plat.pipelines.stages.bronze_stage import BronzeStage
from job_plat.pipelines.stages.silver_stage import SilverStage
from job_plat.pipelines.stages.gold_v1_stage import GoldV1Stage
from job_plat.pipelines.stages.gold_v2_stage import GoldV2Stage
from job_plat.utils.storage import Storage
from job_plat.bronze.ingestion.connectors import JobConnector
from typing import List


## BRONZE PIPELINE ##

def run_bronze_pipeline(
    ctx: BronzeContext,
    storage: Storage,
    connector: JobConnector
    ) -> None:
    stage = BronzeStage(
        bronze_ctx = ctx,
        storage = storage,
        connector = connector
    )
    stage.execute()


## SILVER PIPELINE ##

def run_silver_pipeline(
    ctx: PipelineContext,
    storage: Storage
) -> None:
    stage = SilverStage(
        silver_ctx = ctx.silver,
        bronze_ctx = ctx.bronze,
        storage = storage
    )
    
    stage.execute()


## GOLD V1 PIPELINE ##

def run_gold_v1_pipeline(
    ctx: PipelineContext,
    storage: Storage
) -> None:
    
    stage = GoldV1Stage(
        silver_ctx = ctx.silver,
        gold_v1_ctx = ctx.gold_v1,
        storage = storage
    )
    
    stage.execute()
    

## GOLD V2 PIPELINE ##

def run_gold_v2_pipeline(
    ctx: PipelineContext,
    storage: Storage
) -> None:
    stage = GoldV2Stage(
        gold_v1_ctx = ctx.gold_v1,
        gold_v2_ctx = ctx.gold_v2,
        storage = storage
    )
    
    stage.execute()

### FULL PIPELINE ###


def run_full_pipeline(
    ctx: PipelineContext,
    storage: Storage,
    connectors: List[JobConnector],
) -> None:
    
    for connector in connectors:
        run_bronze_pipeline(
            ctx=bronze_ctx,
            storage=storage,
            connector=connector
        )
        
    silver_pipeline(ctx=pipeline_ctx, storage=storage)
    gold_v1_pipeline(ctx=pipeline_ctx, storage=storage)
    gold_v2_pipeline(ctx=pipeline_ctx, storage=storage)

##############

