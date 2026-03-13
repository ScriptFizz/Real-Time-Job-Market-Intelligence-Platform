import pytest
from job_plat.partitioning.partition_manager import PartitionManager

@pytest.fixture
def partition_manager(tmp_path):
    return PartitionManager(tmp_path)
