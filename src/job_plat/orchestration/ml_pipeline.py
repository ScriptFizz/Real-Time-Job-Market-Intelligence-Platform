from job_plat.context.contexts import (
    FeatureContext,
    MLContext,
    MLPipelineContext
    )
from job_plat.pipeline.stages.ml.feature_stage import FeatureStage
from job_plat.pipeline.stages.ml.ml_stage import MLStage
from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.pipeline.datasets.dataset_registry import DatasetRegistry
from typing import List


## FEATURE PIPELINE ##

def run_feature_pipeline(
    ctx: MLPipelineContext,
    datasets: DatasetRegistry,
    partition_manager: PartitionManager
) -> None:
    stage = FeatureStage(
        #gold_ctx = ctx.gold,
        feature_ctx = ctx.feature,
        datasets = datasets,
        partition_manager = partition_manager
    )
    
    stage.execute()


## ML PIPELINE ##

def run_ml_pipeline(
    ctx: MLPipelineContext,
    datasets: DatasetRegistry,
    partition_manager: PartitionManager
) -> None:
    stage = MLStage(
        #feature_ctx = ctx.feature,
        ml_ctx = ctx.ml,
        datasets = datasets,
        partition_manager = partition_manager
    )
    
    stage.execute()

### ML PIPELINE ###


def run_full_ml_pipeline(
    ctx: MLPipelineContext,
    datasets: DatasetRegistry,
    partition_manager: PartitionManager
) -> None:
    
    run_feature_pipeline(ctx=ctx, datasets=datasets, partition_manager=partition_manager)
    run_ml_pipeline(ctx=ctx, datasets=datasets, partition_manager=partition_manager)

##############

