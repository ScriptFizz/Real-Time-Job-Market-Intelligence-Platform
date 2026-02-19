from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from urllib.parse import urlencode


def create_spark(
    app_name: str,
    master_url: str = "local[*]"
) -> SparkSession:
    """
    Create a SparkSession with a specific application name and master URL.
    
    Args:
        app_name (str): Name of the application to create.
        master_url(str): Master URL of the application to create.
        
    Returns:
        (SparkSession): Entry point to programming Spark.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .getOrCreate()
    )


def union_all(dfs: list[DataFrame]) -> DataFrame:
    """
    Union a list of Spark DataFrames by column name.
    Assumes schemas are aligned.
    
    Args:
        dfs (list[DataFrame]): List of schema-aligned Spark DataFrame to join.
    
    Returns:
        DataFrame: Spark DataFrame of the combined dataframes list.
    """
    
    if not dfs:
        raise ValuerError("No DataFrames to union")
    
    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dfs,
    )

def path_exists(spark: SparkSession, path: str | Path) -> bool:
    
    try:
        spark.read.parquet(path).limit(1).collect()
        return True
    except Exception:
        return False


def build_indeed_url(
    query str,
    location: str
    ) -> str:
    """
    
    """
    missing = []
    if not query:
        missing.append("Missing job role")
    if not location:
        missing.append("Missing location")
    
    if missing:
        raise ValueError(f"Incomplete data: {', '.join(missing)}")
    
    params = {
        "q": query,
        "l": location
    }
    return f"https://www.indeed.com/jobs?{urlencode(params)}"
