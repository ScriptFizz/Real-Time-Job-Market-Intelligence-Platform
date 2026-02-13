from dataclasses import dataclass
from pathlib import Path
from datetime import date
from pyspark.sql import SparkSession
from job_plat.config.context import (
    BronzeContext, 
    SilverContext, 
    GoldV1Context, 
    GoldV2Context
    )

@dataclass
class PipelineContext:
    data_date: date
    env: str
    spark: SparkSession
    bronze: BronzeContext
    silver: SilverContext
    gold_v1: GoldV1Context
    gold_v2: GoldV2Context
    
    
