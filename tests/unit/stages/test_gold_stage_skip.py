import pytest

from job_plat.pipeline.stages.data.gold_stage import GoldStage 
from job_plat.utils.helpers import StageSkip 



@pytest.mark.parametrize(
    ("jobs", "skills"),
    [
        (None, object()),
        (object(), None),
        (None, None),
    ],
)
def test_gold_transform_skips_when_an_input_is_missing(jobs, skills):
    # Create an uninitialized stage instance to avoid requiring Spark context, datasets and partition manager
    stage = object.__new__(GoldStage)

    with pytest.raises(StageSkip, match="no new partitions"):
        stage.transform(
            job_silver_df=jobs,
            job_skills_silver_df=skills,
        )