import pytest

from job_plat.utils.helpers import union_all



def test_union_all_rejects_empty_input():
    with pytest.raises(ValueError, match="No DataFrames to union"):
        union_all([])