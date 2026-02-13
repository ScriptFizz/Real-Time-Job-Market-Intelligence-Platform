from datetime import date
from pathlib import Path
from job_plat.config.context import SilverContext
from job_plat.silver.cleaning.run_clean import run_clean
from job_plat.silver.enrichment.build_job_skills import run_job_skills
from job_plat.silver.validation.quality_checks import run_quality_checks

def silver_pipeline(
    ctx: 
) -> None:
    
    params = load_params(config_path=config_path)
    base_bronze = Path(bronze_path or params["path"]["bronze"])
    base_silver = Path(silver_path or params["path"]["silver"])
    sources = params["ingestion"]["sources"]
    
    job_bronze_path = base_bronze / f"{data_date}.jsonl"
    
    if not job_bronze_path.exists():
        raise FileNotFoundError(f"Bronze data not found in {job_bronze_path}")
    
    job_silver_path = base_silver / "jobs" / f"data_date={data_date}"
    job_skills_silver_path = base_silver / "job_skills" / f"data_date={data_date}"
    
    run_clean(
        data_date = data_date,
        base_bronze_path = base_bronze,
        job_silver_path = job_silver_path,
        sources = sources
    )
    
    run_job_skills(
        job_silver_path = job_silver_path,
        job_skills_silver_path = job_skills_silver_path
    )
    
    run_quality_checks()
    
    
    

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
