import pytest

from job_plat.config.env_config import SparkConfig
from job_plat.utils.helpers import create_spark


@pytest.fixture(scope="session")
def spark():
    spark = create_spark(
        SparkConfig(
            app_name="test",
            master="local[*]",
        )
    )
    yield spark
    spark.stop()


pytest_plugins = [
    "fixtures.datasets",
    "fixtures.sample_dataframe",
    "fixtures.sample_data",
    "fixtures.partition_manager",
    "fixtures.contexts",
]
