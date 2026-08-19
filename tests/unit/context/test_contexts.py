from datetime import timezone

from job_plat.context.contexts import StageExecutionContext



def test_stage_execution_context_generates_unique_run_ids():
    first = StageExecutionContext(stage="silver", pipeline_version="1.0.0")
    second = StageExecutionContext(stage="silver", pipeline_version="1.0.0")

    assert first.run_id != second.run_id

def test_stage_execution_context_uses_aware_utc_timestamp():
    context = StageExecutionContext(
        stage="silver", 
        pipeline_version="1.0.0",
    )

    assert context.started_at.tzinfo is not None
    assert context.started_at.utcoffset() == timezone.utc.utcoffset(
        context.started_at
    )