from job_plat.utils.runtime import build_runtime
from job_plat.context.context_builders import build_data_pipeline_context
from job_plat.orchestration.data_pipeline import run_data_pipeline
from job_plat.ingestion.connectors import build_connectors


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
        ctx = build_data_pipeline_context(
            execution = execution,
            config = env_config,
            spark=spark,
            execution_date=execution_dt
        )
        
        connectors = build_connectors(env_config)
        
        run_data_pipeline(
            ctx=ctx,
            storage=datasets.storage,
            datasets=datasets,
            partition_manager=partition_manager,
            connectors=connectors,
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
