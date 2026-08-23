from datetime import datetime
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from job_plat.config.env_config import SparkConfig


class StageSkip(Exception):
    pass


def parse_date(d: str | None):
    return datetime.strptime(d, "%Y-%m-%d").date() if d else None


def create_spark(spark_config: SparkConfig) -> SparkSession:
    """
    Create a SparkSession with a specific application name and master URL.

    Args:


    Returns:
        (SparkSession): Entry point to programming Spark.
    """

    builder = SparkSession.builder.appName(spark_config.app_name).master(
        spark_config.master
    )

    for key, value in spark_config.config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


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
        raise ValueError("No DataFrames to union")

    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dfs,
    )


def path_exists(spark: SparkSession, path: str | Path) -> bool:
    try:
        spark.read.parquet(str(path)).limit(1).collect()
        return True
    except Exception:
        return False


def assert_df_equality(df1, df2):
    assert sorted(df1.collect()) == sorted(df2.collect())
