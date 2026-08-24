import pytest

from job_plat.partitioning.partition_manager import PartitionManager
from job_plat.partitioning.state_store import LocalStateStore


@pytest.fixture
def partition_manager(tmp_path):
    return PartitionManager(LocalStateStore(str(tmp_path)))
