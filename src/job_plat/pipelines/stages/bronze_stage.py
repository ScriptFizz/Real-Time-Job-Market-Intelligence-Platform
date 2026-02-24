from datetime import date
from pathlib import Path
from pyspark.sql import DataFrame
from job_plat.utils.helpers import build_indeed_url
from job_plat.config.context import BronzeContext, SilverContext, PipelineContext
from job_plat.pipelines.stages import BaseSourceStage
from job_plat.bronze.ingestion.scrape_indeed import scrape_indeed
from job_plat.bronze.ingestion.http_client import HttpClient
from job_plat.utils.storage import Storage
from job_plat.bronze.ingestion.scrapers import JobScraper
from job_plat.bronze.ingestion.metadata import IngestionRun, write_metadata

class BronzeStage(BaseSourceStage):
    
    def __init__(
        self, 
        bronze_ctx: BronzeContext,
        storage: Storage,
        scraper: JobScraper):
            
        super().__init__(storage=storage)
        self.bronze_ctx = bronze_ctx
        self.scraper = scraper
        
    def validate_config(self) -> None:
        missing = []
        if not self.bronze_ctx.query:
            missing.append("Query must not be empty")
        if not self.bronze_ctx.location:
            missing.append("Location must not be empty")
        if missing:
            raise ValueError(", ".join(missing))
    
    def produce(self) -> int:
        
        run = IngestionRun.create(
            source=self.scraper.source_name,
            query = self.bronze_ctx.query,
            location = self.bronze_ctx.location
        )
        # url = self.scraper.build_url(
            # query = self.bronze_ctx.query,
            # location = self.bronze_ctx.location
        # )
        
        # jobs = self.scraper.scrape(url)
        
        jobs = self.scraper.scrape()
        
        # Build partitioned bronze path
        base_path = (
            self.bronze_ctx.base_path
            / f"source={run.source}"
            / f"ingestion_date={run.ingestion_ts.date()}"
            / f"run_id={run.run_id}"
        )
        
        data_path = base_path / "part-000.jsonl"
        
        row_count = self.storage.write_jsonl(
            records=jobs,
            path=data_path,
        )
        
        write_metadata(
            path=data_path, 
            run=run, 
            row_count=row_count
            )
        
        self.logger.info(
            "bronze_run_completed",
            extra={
                "source": run.source,
                "run_id": run.run_id,
                "row_count": row_count,
            }
        )
        return count




# class BronzeStage(BaseSourceStage):
    
    # def __init__(
        # self, 
        # bronze_ctx: BronzeContext,
        # storage: Storage,
        # scraper: JobScraper):
            
        # super().__init__(storage=storage)
        # self.bronze_ctx = bronze_ctx
        # self.scraper = scraper
        
    # def validate_config(self) -> None:
        # missing = []
        # if not self.bronze_ctx.query:
            # missing.append("Query must not be empty")
        # if not self.bronze_ctx.location:
            # missing.append("Location must not be empty")
        # if missing:
            # raise ValueError(", ".join(missing))
    
    # def produce(self) -> int:
        
        # run = IngestionRun.create(
            # source=self.scraper.source_name,
            # query = self.bronze_ctx.query,
            # location = self.bronze_ctx.location
        # )
        # url = self.scraper.build_url(
            # query = self.bronze_ctx.query,
            # location = self.bronze_ctx.location
        # )
        
        # jobs = self.scraper.scrape(url)
        
        # # Build partitioned bronze path
        # base_path = (
            # self.bronze_ctx.base_path
            # / f"source={run.source}"
            # / f"ingestion_date={run.ingestion_ts.date()}"
            # / f"run_id={run.run_id}"
        # )
        
        # data_path = base_path / "part-000.jsonl"
        
        # row_count = self.storage.write_jsonl(
            # records=jobs,
            # path=data_path,
        # )
        
        # write_metadata(
            # path=data_path, 
            # run=run, 
            # row_count=row_count
            # )
        
        # self.logger.info(
            # "bronze_run_completed",
            # extra={
                # "source": run.source,
                # "run_id": run.run_id,
                # "row_count": row_count,
            # }
        # )
        # return count
