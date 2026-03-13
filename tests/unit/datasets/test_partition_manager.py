import pytest
from datetime import date
from job_plat.partitioning.partition_manager import PartitionManager

def test_marks_processed_partitions(tmp_path):
    
    pm = PartitionManager(tmp_path)
    
    pm.mark_processed(
        stage_name="silver_jobs",
        partitions=[date(2025, 3, 1), date(2025, 3, 2)]
    )
    
    processed = pm.get_processed(
        stage_name="silver_jobs"
        )
    
    assert date(2025, 3, 1) in processed


