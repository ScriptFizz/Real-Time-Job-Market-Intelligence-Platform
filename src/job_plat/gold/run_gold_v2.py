from pyspark.sql import SparkSession, DataFrame
from pathlib import Path

def run_gold_v1(
    
) -> None:
    
    # INSERT CODE


    cluster_df.write.mode("overwrite")...
    membership_df.write.mode("overwrite")...
    metadata_df.write.mode("append")...
