from airflow.decorators import dag, task, get_current_context
from datetime import datetime
import subprocess


@dag(schedule="@weekly", start_date=datetime(2024, 1, 1), catchup=False)
def ml_dag():
    
    @task
    def run_features():
        
        context = get_current_context()
        execution_date = context["logical_date"].isoformat()
        subprocess.run(
            [
                "poetry", "run", "python", "-m", "job_plat.cli", "feature",
                "--execution-date", execution_date,
            ],
            check=True
        )
            
    @task
    def run_ml():
        
        context = get_current_context()
        execution_date = context["logical_date"].isoformat()
        subprocess.run(
            [
                "poetry", "run", "python", "-m", "job_plat.cli", "ml",
                "--execution-date", execution_date,
            ],
            check=True
        )
    
    run_features() >> run_ml()

dag = ml_dag()




# @dag(schedule="@weekly", start_date=datetime(2024, 1, 1), catchup=False)
# def ml_dag():
    
    # @task
    # def run_features():
        # env_config, spark, _, datasets, partition_manager, _ = build_common()
        # try:
            # pipeline_ctx = build_ml_pipeline_context(
                # config=env_config,
                # spark=spark
                # )
            # stage = FeatureStage(
                # ctx=pipeline_ctx.feature, 
                # datasets= datasets, 
                # partition_manager=partition_manager
                # )
            # stage.execute()
        # finally:
            # spark.stop()
            
    # @task
    # def run_ml():
        # env_config, spark, _, datasets, partition_manager, _ = build_common()
        # try:
            # pipeline_ctx = build_ml_pipeline_context(
                # config=env_config,
                # spark=spark
                # )
            # stage = MLStage(
                # ctx=pipeline_ctx.ml, 
                # datasets= datasets, 
                # partition_manager=partition_manager
                # )
            # stage.execute()
        # finally:
            # spark.stop()
    
    # run_features() >> run_ml()

# dag = ml_dag()
