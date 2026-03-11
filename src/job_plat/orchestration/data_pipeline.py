from job_plat.context.contexts import (
    BronzeContext, 
    SilverContext,
    GoldContext,
    DataPipelineContext,
    # FeatureContext,
    # MLContext,
    # MLPipelineContext
    )
from job_plat.pipeline.stages.data.bronze_stage import BronzeStage
from job_plat.pipeline.stages.data.silver_stage import SilverStage
from job_plat.pipeline.stages.data.gold_stage import GoldStage
# from job_plat.pipeline.stages.ml.feature_stage import FeatureStage
# from job_plat.pipeline.stages.ml.ml_stage import MLStage
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
        bronze_ctx = ctx.bronze,
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
        silver_ctx = ctx.silver,
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

##############


# ## FEATURE PIPELINE ##

# def run_feature_pipeline(
    # ctx: MLPipelineContext,
    # datasets: DatasetRegistry,
    # partition_manager: PartitionManager
# ) -> None:
    # stage = FeatureStage(
        # gold_ctx = ctx.gold,
        # feature_ctx = ctx.feature,
        # datasets = datasets,
        # partition_manager = partition_manager
    # )
    
    # stage.execute()


# ## ML PIPELINE ##

# def run_ml_pipeline(
    # ctx: MLPipelineContext,
    # datasets: DatasetRegistry,
    # partition_manager: PartitionManager
# ) -> None:
    # stage = MLStage(
        # feature_ctx = ctx.feature,
        # ml_ctx = ctx.ml,
        # datasets = datasets,
        # partition_manager = partition_manager
    # )
    
    # stage.execute()
