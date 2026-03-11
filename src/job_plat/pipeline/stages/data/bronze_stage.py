from datetime import date
import time
import logging
import json
from pathlib import Path
from typing import Iterator, Dict
from pyspark.sql import DataFrame
from job_plat.context.contexts import BronzeContext, SilverContext, DataPipelineContext
from job_plat.pipeline.core.base_source_stage import BaseSourceStage
from job_plat.storage.storages import Storage
from job_plat.ingestion.connectors import JobConnector
from job_plat.ingestion.metadata import IngestionRun, write_metadata
from job_plat.ingestion.search_criteria import JobSearchCriteria

class BronzeStage(BaseSourceStage):
    
    def __init__(
        self, 
        bronze_ctx: BronzeContext,
        storage: Storage,
        connector: JobConnector):
            
        super().__init__(storage=storage)
        self.bronze_ctx = bronze_ctx
        self.connector = connector
        
    def validate_config(self) -> None:
        missing = []
        if not self.bronze_ctx.query:
            missing.append("Query must not be empty")
        if not self.bronze_ctx.location:
            missing.append("Location must not be empty")
        if missing:
            raise ValueError(", ".join(missing))
    
    def create_context(self) -> IngestionRun:
        run_context = IngestionRun(
            source=self.connector.name,
            query=self.bronze_ctx.query,
            country=self.bronze_ctx.country,
            location=self.bronze_ctx.location,
            pipeline_version="1.0.0"
        )
        return run_context
    
    def _enrich_with_ingestion_metadata(
        self, 
        records : Iterator[Dict], 
        run:IngestionRun
        ) -> Iterator[Dict]:
        for record in records:
            yield {
                "ingestion_metadata": {
                    "run_id": run.run_id,
                    "source": run.source,
                    "query": run.query,
                    "location": run.location,
                    "started_at": run.started_at.isoformat(),
                    "canonical_schema_version": "1.0.0",
                },
                "raw_payload": record,
                "payload": self.connector.normalize(record).model_dump()
            }
    
    def produce(self, run: IngestionRun, logger: logging.Logger) -> int:
        
        logger.info("bronze_run_started", extra={"source": run.source},)

        criteria = JobSearchCriteria(
            query=run.query,
            country=run.country,
            location=run.location,
        )
        
        raw_stream = self.connector.fetch(
            criteria=criteria,
        )
        
        enriched_stream = self._enrich_with_ingestion_metadata(
            records=raw_stream, 
            run=run,
            )

        base_path = (
            Path(self.bronze_ctx.root_path)
            / "bronze"
            / "jobs"
            / f"ingestion_date={run.started_at.date()}"
            / f"source={run.source}"
            / f"run_id={run.run_id}"
        )
    
        data_path = base_path / "part-000.jsonl"
    
        row_count = self.storage.write_jsonl(
            records=enriched_stream,
            path=data_path,
        )
    
        write_metadata(
            path=base_path, 
            run=run, 
            row_count=row_count
            )
        
        # Save runs metadata in metadata registry
        
        runs_dir = Path(self.bronze_ctx.root_path) /  "_runs"
        
        runs_dir.mkdir(parents=True, exist_ok=True)
        write_metadata(
            path=runs_dir,
            run=run,
            row_count=row_count,
            filename=f"{run.run_id}.json"
        )

        # Log bronze run stats 

        logger.info(
            "bronze_run_completed",
            extra={
                "source": run.source,
                "row_count": row_count,
            },
        )
        return row_count

################################


# class BronzeStage(BaseSourceStage):
    
    # def __init__(
        # self, 
        # bronze_ctx: BronzeContext,
        # storage: Storage,
        # connector: JobConnector):
            
        # super().__init__(storage=storage)
        # self.bronze_ctx = bronze_ctx
        # self.connector = connector
        
    # def validate_config(self) -> None:
        # missing = []
        # if not self.bronze_ctx.query:
            # missing.append("Query must not be empty")
        # if not self.bronze_ctx.location:
            # missing.append("Location must not be empty")
        # if missing:
            # raise ValueError(", ".join(missing))
    
    # def create_context(self) -> IngestionRun:
        # run_context = IngestionRun(
            # source=self.connector.name,
            # query=self.bronze_ctx.query,
            # location=self.bronze_ctx.location,
            # pipeline_version="1.0.0"
        # )
        # return run_context
    
    # def _enrich_with_ingestion_metadata(
        # self, 
        # records : Iterator[Dict], 
        # run:IngestionRun
        # ) -> Iterator[Dict]:
        # for record in records:
            # yield {
                # "ingestion_metadata": {
                    # "run_id": run.run_id,
                    # "source": run.source,
                    # "query": run.query,
                    # "location": run.location,
                    # "started_at": run.started_at.isoformat(),
                    # "canonical_schema_version": "1.0.0",
                # },
                # "raw_payload": record,
                # "payload": self.connector.normalize(record).model_dump()
            # }
    
    # def produce(self, run: IngestionRun, logger: logging.Logger) -> int:
        
        # logger.info("bronze_run_started", extra={"source": run.source},)

        # criteria = JobSearchCriteria(
            # query=run.query,
            # location=run.location,
        # )
        
        # raw_stream = self.connector.fetch(
            # criteria=criteria,
        # )
        
        # enriched_stream = self._enrich_with_ingestion_metadata(
            # records=raw_stream, 
            # run=run,
            # )

        # base_path = (
            # Path(self.bronze_ctx.root_path)
            # / "bronze"
            # / "jobs"
            # / f"ingestion_date={run.started_at.date()}"
            # / f"source={run.source}"
            # / f"run_id={run.run_id}"
        # )
    
        # data_path = base_path / "part-000.jsonl"
    
        # row_count = self.storage.write_jsonl(
            # records=enriched_stream,
            # path=data_path,
        # )
    
        # write_metadata(
            # path=base_path, 
            # run=run, 
            # row_count=row_count
            # )
        
        # # Save runs metadata in metadata registry
        
        # runs_dir = Path(self.bronze_ctx.root_path) /  "_runs"
        
        # runs_dir.mkdir(parents=True, exist_ok=True)
        # write_metadata(
            # path=runs_dir,
            # run=run,
            # row_count=row_count,
            # filename=f"{run.run_id}.json"
        # )

        # # Log bronze run stats 

        # logger.info(
            # "bronze_run_completed",
            # extra={
                # "source": run.source,
                # "row_count": row_count,
            # },
        # )
        # return row_count
############################### 08-03


# class BronzeStage(BaseSourceStage):
    
    # def __init__(
        # self, 
        # bronze_ctx: BronzeContext,
        # storage: Storage,
        # connector: JobConnector):
            
        # super().__init__(storage=storage)
        # self.bronze_ctx = bronze_ctx
        # self.connector = connector
        
    # def validate_config(self) -> None:
        # missing = []
        # if not self.bronze_ctx.query:
            # missing.append("Query must not be empty")
        # if not self.bronze_ctx.location:
            # missing.append("Location must not be empty")
        # if missing:
            # raise ValueError(", ".join(missing))
    
    # def create_context(self) -> IngestionRun:
        # run_context = IngestionRun(
            # source=self.connector.name,
            # query=self.bronze_ctx.query,
            # location=self.bronze_ctx.location,
            # pipeline_version="1.0.0"
        # )
        # return run_context
    
    # def _enrich_with_ingestion_metadata(
        # self, 
        # records : Iterator[Dict], 
        # run:IngestionRun
        # ) -> Iterator[Dict]:
        # for record in records:
            # yield {
                # "ingestion_metadata": {
                    # "run_id": run.run_id,
                    # "source": run.source,
                    # "query": run.query,
                    # "location": run.location,
                    # "started_at": run.started_at.isoformat(),
                    # "canonical_schema_version": "1.0.0",
                # },
                # "raw_payload": record,
                # "payload": self.connector.normalize(record).model_dump()
            # }
    
    # def produce(self, run: IngestionRun, logger: logging.Logger) -> int:
        
        # logger.info("bronze_run_started", extra={"source": run.source},)

        # criteria = JobSearchCriteria(
            # query=run.query,
            # location=run.location,
        # )
        
        # raw_stream = self.connector.fetch(
            # criteria=criteria,
        # )
        
        # enriched_stream = self._enrich_with_ingestion_metadata(
            # records=raw_stream, 
            # run=run,
            # )

        # base_path = (
            # Path(self.bronze_ctx.root_path)
            # / "bronze"
            # / "jobs"
            # / f"ingestion_date={run.started_at.date()}"
            # / f"source={run.source}"
            # / f"run_id={run.run_id}"
        # )
    
        # data_path = base_path / "part-000.jsonl"
    
        # row_count = self.storage.write_jsonl(
            # records=enriched_stream,
            # path=data_path,
        # )
    
        # write_metadata(
            # path=base_path, 
            # run=run, 
            # row_count=row_count
            # )
        
        # # Save runs metadata in metadata registry
        
        # runs_dir = Path(self.bronze_ctx.root_path) /  "_runs"
        
        # runs_dir.mkdir(parents=True, exist_ok=True)
        # write_metadata(
            # path=runs_dir,
            # run=run,
            # row_count=row_count,
            # filename=f"{run.run_id}.json"
        # )

        # # Log bronze run stats 

        # logger.info(
            # "bronze_run_completed",
            # extra={
                # "source": run.source,
                # "row_count": row_count,
            # },
        # )
        # return row_count

##########################
