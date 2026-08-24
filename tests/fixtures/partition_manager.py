import pytest

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.processing_ledger import ProcessingLedger


@pytest.fixture
def partition_manager(spark, tmp_path):
    return PartitionManager(ProcessingLedger(spark, str(tmp_path)))
