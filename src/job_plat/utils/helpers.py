from functools import reduce
from pyspark.sql import DataFrame

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
