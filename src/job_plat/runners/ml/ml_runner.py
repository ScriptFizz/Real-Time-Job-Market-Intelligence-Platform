from job_plat.utils.runtime import build_runtime
from job_plat.context.context_builders import build_ml_pipeline_context
from job_plat.orchestration.ml_pipeline import run_ml_pipeline

def main(
    env: str,
    execution_date: str,
    config_path: str = "settings.yaml",
) -> None:
    
    (
        env_config,
        execution,
        execution_dt,
        spark,
        datasets,
        partition_manager,
    ) = build_runtime(
        config_path=config_path,
        env=env,
        execution_date=execution_date,
    )
    
    try:
        ctx = build_ml_pipeline_context(
            config = env_config,
            spark=spark,
            execution_date=execution_dt
        )
        
        run_ml_pipeline(
            ctx=ctx,
            datasets=datasets,
            partition_manager=partition_manager
            )
            
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--config", default="settings.yaml")
    
    args = parser.parse_args()
    
    main(
        env=args.env,
        execution_date=args.execution_date,
        config_path=args.config,
    )
