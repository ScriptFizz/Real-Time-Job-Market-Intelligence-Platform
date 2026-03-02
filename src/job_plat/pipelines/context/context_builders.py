from job_plat.utils.io import load_params
from job_plat.utils.helpers import create_spark
from job_plat.pipelines.context.contexts import (
    BronzeContext, 
    SilverContext, 
    GoldV1Context, 
    GoldV2Context,
    PipelineContext
    )
from datetime import date
from pyspark.sql import SparkSession
from typing import Dict
from job_plat.config.env_config import EnvironmentConfig

def build_pipeline_context(
    data_date: date,
    config: EnvironmentConfig
) -> PipelineContext:

    
    spark = create_spark(spark_config=env_config.spark)
    
    bronze_ctx = BronzeContext(
        data_date = data_date,
        base_path = config.paths.bronze
    )
    
    silver_ctx = SilverContext(
        data_date = data_date,
        base_path = config.paths.silver,
        spark = spark
    )
    
    gold_v1_ctx = GoldV1Context(
        data_date = data_date,
        base_path = config.paths.gold_v1,
        spark = spark
    )
    
    gold_v2_ctx = GoldV2Context(
        data_date = data_date,
        base_path = config.paths.gold_v2,
        spark = spark
    )
    
    return PipelineContext(
        data_date = data_date,
        env = config.env,
        spark = spark,
        bronze = bronze_ctx,
        silver = silver_ctx,
        gold_v1 = gold_v1_ctx,
        gold_v2 = gold_v2_ctx
        
    )
    
def build_bronze_context(
    pipeline_ctx: PipelineContext,
    query: str | None,
    location: str | None,
) -> BronzeContext:
    
    final_query = query or pipeline_ctx.config["bronze"]["query"]
    final_location = location or pipeline_ctx.config["bronze"]["location"]
    
    missing = []
    if not final_query:
        missing.append("Query must not be empty")
    if not final_location:
        missing.append("Location must not be empty")
    if missing:
        raise ValueError(", ".join(missing))
    
    return BronzeContext(
        data_date = pipeline_ctx.data_date,
        base_path = pipeline_ctx.bronze.base_path,
        query = final_query,
        location = final_location
    )
    
    
    
    # def build_pipeline_context(
    # data_date: date,
    # config_path: str = "settings.yaml"
# ) -> PipelineContext:

    # params = load_params(config_path = config_path)
    
    # spark = create_spark()
    
    # bronze_ctx = BronzeContext(
        # data_date = data_date,
        # base_path = params["path"]["bronze"]
    # )
    
    # silver_ctx = SilverContext(
        # data_date = data_date,
        # base_path = params["path"]["silver"],
        # spark = spark
    # )
    
    # gold_v1_ctx = GoldV1Context(
        # data_date = data_date,
        # base_path = params["path"]["gold_v1"],
        # spark = spark
    # )
    
    # gold_v2_ctx = GoldV2Context(
        # data_date = data_date,
        # base_path = params["path"]["gold_v2"],
        # spark = spark
    # )
    
    # return PipelineContext(
        # data_date = data_date,
        # env = params["env"],
        # spark = spark,
        # bronze = bronze_ctx,
        # silver = silver_ctx,
        # gold_v1 = gold_v1_ctx,
        # gold_v2 = gold_v2_ctx
        
    # )
