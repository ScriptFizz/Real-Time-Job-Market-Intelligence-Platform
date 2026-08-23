from datetime import UTC, datetime

import pytest

from job_plat.context.contexts import GoldContext, SilverContext


@pytest.fixture
def execution_date():
    return datetime(2025, 3, 2, tzinfo=UTC)


@pytest.fixture
def silver_ctx(spark, execution_date):
    return SilverContext(
        spark=spark,
        execution_date=execution_date,
    )


@pytest.fixture
def gold_ctx(spark, execution_date):
    return GoldContext(
        spark=spark,
        execution_date=execution_date,
        fact_per_job_ratio_threshold=100,
    )
