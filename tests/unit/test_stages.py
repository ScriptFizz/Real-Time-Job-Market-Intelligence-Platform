# import pytest
# from job_plat.partitioning.partition_manager import PartitionManager

# def test_stage_runs(spark, dataset_registry, tmp_path, bronze_ctx, silver_ctx):
    
    # pm = PartitionManager(tmp_path)
    
    # stage = SilverJobsStage(
        # silver_ctx=silver_ctx,
        # bronze_ctx=bronze_ctx,
        # spark=spark,
        # datasets=dataset_registry,
        # partition_manager=pm
        # )
    
    # stage.run()
    
    # ds = dataset_registry.get(SilverJobs)
    
    # df = spark.read.parquet(str(ds.path))
    
    # assert df.count() > 0
