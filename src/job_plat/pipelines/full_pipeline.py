from datetime import date
from typing import Dict

from job_plat.pipelines.pipeline_stages import (
    bronze_pipeline,
    silver_pipeline,
    gold_v1_pipeline,
    gold_v2_pipeline
)

from job_plat.config.context.contexts import (
    BronzeContext,
    SilverContext,
    GoldV1Context,
    GoldV2Context,
    PipelineContext
    )

from job_plat.config.context.build_pipeline_context import build_pipeline_context
#from job_plat.config.browser import DEFAULT_BROWSER_HEADERS
from job_plat.utils.storage import get_storage
#from job_plat.bronze.ingestion.http_client import HttpClient
#from job_plat.bronze.ingestion.sources_builders import build_indeed_source, build_linkedin_source
#from job_plat.bronze.ingestion.scrapers import jobScraper
from job_plat.bronze.ingestion.connectors import JobConnector


def full_pipeline(config: Dict) -> None:
    
    data_date = date.today()
    storage = get_storage(config=config)
    
    pipeline_ctx = build_pipeline_context(data_date = data_date)
    query = pipeline_ctx.bronze.query
    location = pipeline_ctx.bronze.location

        
    bronze_pipeline(ctx=pipeline_ctx, storage=storage, connector=indeed_scraper)
    bronze_pipeline(ctx=pipeline_ctx, storage=storage, connector=linkedin_scraper)
        
        
    silver_pipeline(ctx=pipeline_ctx, storage=storage)
    gold_v1_pipeline(ctx=pipeline_ctx, storage=storage)
    gold_v2_pipeline(ctx=pipeline_ctx, storage=storage)
