from job_plat.processing.clean_jobs import read_bronze, clean_jobs, deduplicate_jobs

def run_clean(run_date: str) -> None:
    
    spark = create_spark()
    
    bronze_path = 
    silver_path = 
    
    df = read_bronze(spark, bronze_path)
    df_clean = clean_jobs(df)
    df_deduped = deduplicate_jobs(df_clean)
    
    write_silver(df_deduped, silver_path)
    
    spark.stop()
