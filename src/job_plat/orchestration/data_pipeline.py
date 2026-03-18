from job_plat.context.contexts import (
    BronzeContext, 
    SilverContext,
    GoldContext,
    DataPipelineContext
    )
from job_plat.pipeline.stages.data.bronze_stage import BronzeStage
from job_plat.pipeline.stages.data.silver_stage import SilverStage
from job_plat.pipeline.stages.data.gold_stage import GoldStage
from job_plat.storage.storages import Storage
from job_plat.ingestion.connectors import JobConnector
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
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
    ctx: DataPipelineContext,
    datasets: DatasetRegistry,
    partition_manager: PartitionManager
) -> None:
    stage = SilverStage(
        silver_ctx = ctx.silver,
        datasets = datasets,
        partition_manager = partition_manager
    )
    
    stage.execute()


## GOLD PIPELINE ##

def run_gold_pipeline(
    ctx: DataPipelineContext,
    datasets: DatasetRegistry,
    partition_manager: PartitionManager
) -> None:
    
    stage = GoldStage(
        gold_ctx = ctx.gold,
        datasets = datasets,
        partition_manager = partition_manager
    )
    
    stage.execute()
    


### DATA PIPELINE ###


def run_data_pipeline(
    ctx: DataPipelineContext,
    storage: Storage,
    datasets: DatasetRegistry,
    partition_manager: PartitionManager,
    connectors: List[JobConnector],
) -> None:
    
    for connector in connectors:
        run_bronze_pipeline(
            ctx=ctx.bronze,
            storage=storage,
            connector=connector
        )
        
    run_silver_pipeline(ctx=ctx, datasets=datasets, partition_manager=partition_manager)
    run_gold_pipeline(ctx=ctx, datasets=datasets, partition_manager=partition_manager)
