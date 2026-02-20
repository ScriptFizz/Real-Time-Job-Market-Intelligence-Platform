from datetime import date
from pathlib import Path
from pyspark.sql import DataFrame
from job_plat.utils.helpers import build_indeed_url
from job_plat.config.context import BronzeContext, SilverContext, PipelineContext
from job_plat.pipelines.stages import BaseSourceStage
from job_plat.bronze.ingestion.scrape_indeed import scrape_indeed
from job_plat.bronze.ingestion.http_client import HttpClient
from job_plat.utils.storage import Storage

class BronzeStage(BaseSourceStage):
    
    def __init__(
        self, 
        bronze_ctx: BronzeContext,
        storage: Storage):
        super().__init__(storage=storage)
        self.bronze_ctx = bronze_ctx
        
    def validate_config(self) -> None:
        missing = []
        if not self.bronze_ctx.query:
            missing.append("Query must not be empty")
        if not self.bronze_ctx.location:
            missing.append("Location must not be empty")
        if missing:
            raise ValueError(", ".join(missing))
    
    def produce(self) -> int:
        url = build_indeed_url(
            query = self.bronze_ctx.query,
            location = self.bronze_ctx.location
        )
        
        output_path = self.bronze_ctx.indeed_jobs_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        client = HttpClient(headers={
        "User-Agent": "Mozilla/5.0 (compatible; JobPlatBot/1.0)"
        })
        
        jobs = scrape_indeed(
            url=url,
            client=client)
        
        count = self.storage.write_jsonl(
            records = jobs,
            path = output_path
        )
        return count
        # count = 0
        # with output_path.open("w", encoding="utf-8") as f:
            # for job in scrape_indeed(url):
                # f.write(json.dumps(job, ensure_ascii=False) + "\n")
                # count += 1
        # return count
