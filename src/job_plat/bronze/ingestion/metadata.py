from dataclasses import dataclass
from datetime import datetime
import uuid
from pathlib import Path

@dataclass
class IngestionRun:
    source: str
    query: str
    location: str
    ingestion_ts: datetime
    run_id: str
    pipeline_version: str
    
    @classmethod
    def create(cls, source: str, query: str, location: str, version: str):
        return cls(
            source=source,
            query=query,
            location=location,
            ingestion_ts=datetime.utcnow(),
            run_id=str(uuid.uuid4()),
            pipeline_version=version,
        )


# @dataclass
# class IngestionRun:
    # source: str
    # query: str
    # location: str
    # ingestion_ts: datetime
    # run_id: str
    
    # @classmethod
    # def create(cls, source: str, query: str, location: str):
        # return cls(
            # source=source,
            # query=query,
            # location=location,
            # ingestion_ts=datetime.uctnow(),
            # run_id=str(uuid.uuid4()),
        # )


def write_metadata(path: Path, run: IngestionRun, row_count: int):
    
    metadata = {
        "source": run.source,
        "query": run.query,
        "location": run.location,
        "ingestion_ts": run.ingestion_ts.isoformat(),
        "row_count": row_count,
    }
    
    with open(path / "_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
