from datetime import date
from pathlib import Path
from job_plat.config.context import BronzeContext, SilverContext, PipelineContext
from job_plat.pipelines.stages.silver_stage import SilverStage

def silver_pipeline(
    ctx: PipelineContext
) -> None:
    stage = SilverStage(
        silver_ctx = ctx.silver,
        bronze_ctx = ctx.bronze
    )
    
    stage.execute()
    
    

# def silver_pipeline(
    # ctx: PipelineContext
# ) -> None:
    
    # bronze_ctx  = ctx.bronze
    # silver_ctx = ctx.silver
    
    # job_bronze_paths = bronze_ctx.jobs_path_list
    # job_silver_path = silver_ctx.jobs_path
    # job_skills_silver_path = silver_ctx.job_skills_path
    # spark = ctx.spark
    
    
    # if not job_bronze_path.exists():
        # raise FileNotFoundError(f"Bronze data not found in {job_bronze_path}")
    
    
    # run_clean(
        # job_bronze_paths = job_bronze_paths,
        # job_silver_path = job_silver_path,
        # spark = spark
    # )
    
    # run_job_skills(
        # job_silver_path = job_silver_path,
        # job_skills_silver_path = job_skills_silver_path,
        # spark = spark
    # )
    
    # run_quality_checks()

# def silver_pipeline(
    # data_date: date,
    # bronze_path: str | None = None,
    # silver_path: str | None = None,
    # config_path: str = "settings.yaml"
# ) -> None:
    
    # params = load_params(config_path=config_path)
    # base_bronze = Path(bronze_path or params["path"]["bronze"])
    # base_silver = Path(silver_path or params["path"]["silver"])
    # sources = params["ingestion"]["sources"]
    
    # job_bronze_path = base_bronze / f"{data_date}.jsonl"
    
    # if not job_bronze_path.exists():
        # raise FileNotFoundError(f"Bronze data not found in {job_bronze_path}")
    
    # job_silver_path = base_silver / "jobs" / f"data_date={data_date}"
    # job_skills_silver_path = base_silver / "job_skills" / f"data_date={data_date}"
    
    # run_clean(
        # data_date = data_date,
        # base_bronze_path = base_bronze,
        # job_silver_path = job_silver_path,
        # sources = sources
    # )
    
    # run_job_skills(
        # job_silver_path = job_silver_path,
        # job_skills_silver_path = job_skills_silver_path
    # )
    
    # run_quality_checks()
