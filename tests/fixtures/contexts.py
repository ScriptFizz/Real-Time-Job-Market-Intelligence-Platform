import pytest
from job_plat.context.contexts import (
    BronzeContext,
    SilverContext,
    GoldContext
)

@pytest.fixture
def bronze_ctx(tmp_path):
    return BronzeContext(root_path=tmp_path)

@pytest.fixture
def silver_ctx(spark):
    return SilverContext(spark=spark)

@pytest.fixture
def gold_ctx(spark):
    return GoldContext(spark=spark, fact_per_job_ratio_threshold=100)


