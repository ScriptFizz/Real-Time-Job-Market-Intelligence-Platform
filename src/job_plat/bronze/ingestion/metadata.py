from dataclasses import dataclass
from datetime import datetime
import uuid
import json
from pathlib import Path


@dataclass(kw_only=True)
class StageExecutionContext:
    stage: str
    pipeline_version: str
    run_id: str = str(uuid.uuid4())
    started_at: datetime = datetime.utcnow()


@dataclass(kw_only=True)
class IngestionRun(StageExecutionContext):
    stage: str = "bronze"
    source: str
    query: str
    location: str


def write_metadata(path: Path, run: IngestionRun, row_count: int):
    
    metadata = {
        "run_id": run.run_id,
        "source": run.source,
        "query": run.query,
        "location": run.location,
        "started_at": run.started_at.isoformat(),
        "pipeline_version": run.pipeline_version,
        "row_count": row_count,
    }
    
    path.mkdir(parents=True, exist_ok=True)
    
    with open(path / "_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)


# @dataclass
# class StageExecutionContext:
    # run_id: str
    # stage: str
    # pipeline_version: str
    # started_at: datetime
    
    # @classmethod
    # def create(cls, stage: str, pipeline_version: str):
        # return cls(
            # run_id = str(uuid.uuid4()),
            # stage = stage,
            # pipeline_version = pipeline_version,
            # started_at = datetime.utcnow()
        # )

# @dataclass
# class IngestionRun(StageExecutionContext):
    # source: str
    # query: str
    # location: str
    
    # @classmethod
    # def create(
        # cls, 
        # source: str, 
        # query: str, 
        # location: str, 
        # pipeline_version: str
        # ):
        
        # base = super().create(
            # stage="bronze",
            # pipeline_version=pipeline_version
        # )
        # return cls(
            # source=source,
            # query=query,
            # location=location,
            # run_id=base.run_id,
            # started_at=base.started_at,
            # stage=base.stage,
            # pipeline_version=base.pipeline_version
        # )
