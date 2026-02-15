from job_plat.processing.clean_jobs import read_bronze, clean_jobs, deduplicate_jobs
from job_plat.utils.helpers import create_spark, union_all
from pyspark.sql.functions import lit
from datetime import date
from pathlib import Path
from typing import List

def run_clean(
    job_bronze_paths: List[Path],
    job_silver_path: Path,
    spark: SparkSession
) -> None:
    """
    Clean and deduplicate jobs data from the Bronze layer, store it in the Silver layer (jobs_silver).
    
    Args:
      job_bronze_paths (List[Path]): List of filepaths of the Bronze layer.
      job_silver_path (Path): Filepath to store the job table in the Silver layer.
      spark (SparkSession): Entry point interface for Spark engine.
    """
    
    
    for job_bronze_path in job_bronze_paths:
        if not job_bronze_path.exists():
            continue
            
        df = read_bronze(spark, job_bronze_path)
        df_clean = clean_jobs(df=df)
        dfs.append(df_clean)
    
    if not dfs:
        raise RuntimeError("No bronze data found for given date")
        
    silver_df = union_all(dfs)
    silver_df = deduplicate_jobs(silver_df)
        
    
    write_silver(silver_df, job_silver_path)


# def run_clean(
    # data_date: date,
    # base_bronze_path: str | Path,
    # job_silver_path: str | Path,
    # source: List[str] | None = None
# ) -> None:
    # """
    # Clean and deduplicate jobs data from the Bronze layer, store it in the Silver layer (jobs_silver).
    
    # Args:
      # base_bronze_path (str | Path): base filepath of the Bronze layer.
      # silver_path: (str | Path): filepath of the Silver layer.
    # """
    
    # spark = create_spark(
        # app_name = "clean-jobs-silver"
    # )
    
    # sources = sources or ["indeed", "linkedin"]
    # dfs = []
    
    # for source in sources:
        # bronze_path = Path(base_bronze_path) / source / f"{data_date.isoformat()}.jsonl"
        # if not bronze_file.exists():
            # continue
            
        # df = read_bronze(spark, bronze_path)
        # df_clean = clean_jobs(df=df)
        # dfs.append(df_clean)
    
    # if not dfs:
        # raise RuntimeError("No bronze data found for given date")
        
    # silver_df = union_all(dfs)
    # silver_df = deduplicate_jobs(silver_df)
        
    
    # write_silver(silver_df, job_silver_path)
    
    # spark.stop()
