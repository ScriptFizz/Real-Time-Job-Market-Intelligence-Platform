from job_plat.processing.clean_jobs import read_bronze, clean_jobs, deduplicate_jobs
from datetime import date
from pathlib import Path

def run_clean(
    run_date: date,
    bronze_path: str | Path,
    silver_path: str | Path
) -> None:
    """
    Clean and deduplicate jobs data from the Bronze layer, store it in the Silver layer.
    
    Args:
      run_date (date): Execution date of the scrape.
      bronze_path (str | Path): filepath of the Bronze layer.
      silver_path: (str | Path): filepath of the Silver layer.
    """
    spark = create_spark()
    
    df = read_bronze(spark, bronze_path)
    df_clean = clean_jobs(df)
    df_deduped = deduplicate_jobs(df_clean)
    
    write_silver(df_deduped, silver_path)
    
    spark.stop()


def run_clean(
    bronze_path: str | Path,
    silver_path: str | Path
) -> None:
    """
    Clean and deduplicate jobs data from the Bronze layer, store it in the Silver layer.
    
    Args:
      bronze_path (str | Path): filepath of the Bronze layer.
      silver_path: (str | Path): filepath of the Silver layer.
    """
    spark = create_spark()
    
    df = read_bronze(spark, bronze_path)
    df_clean = clean_jobs(df)
    df_deduped = deduplicate_jobs(df_clean)
    
    write_silver(df_deduped, silver_path)
    
    spark.stop()
