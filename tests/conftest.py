import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("test")
        .getOrCreate()
    )
    yield spark
    spark.stop()

pytest_plugins = [
    "fixtures.datasets",
    "fixtures.sample_dataframe",
    "fixtures.sample_data",
    "fixtures.partition_manager",
    "fixtures.contexts"
]
