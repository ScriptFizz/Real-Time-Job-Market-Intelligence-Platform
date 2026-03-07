from job_plat.pipelines.stages.base_stage import BaseStage
from job_plat.pipelines.context.contexts import GoldV1Context, GoldV2Context
from job_plat.gold.v2_intelligence.embeddings.build_skill_embeddings import build_skill_embeddings
from job_plat.gold.v2_intelligence.embeddings.build_job_embeddings import build_job_embeddings
from job_plat.gold.v2_intelligence.clusters.build_job_clusters import build_job_clusters
from job_plat.utils.storage import Storage
from pyspark.sql import DataFrame
from job_plat.bronze.ingestion.metadata import StageExecutionContext

class GoldV2Stage(BaseStage):
    
    STAGE_NAME="gold_v2"
    INPUT_DATASETS = ["gold_v1_dim_jobs", "gold_v1_dim_skills", "gold_v1_fact_job_skills"]
    OUTPUT_DATASETS = ["gold_v2_skill_embeddings", "gold_v2_job_embeddings", "gold_v2_job_clusters", "gold_v2_job_membership", "gold_v2_job_centroids", "gold_v2_job_cluster_metadata"]
    
    def __init__(
        self, 
        gold_v1_ctx: GoldV1Context, 
        gold_v2_ctx: GoldV2Context,
        datasets: DatasetRegistry,
        partition_manager: PartitionManager,):
        super().__init__(spark=gold_v2_ctx.spark, datasets=datasets, partition_manager=partition_manager)
        STAGE_NAME="gold_v2"
        self.gold_v1_ctx = gold_v1_ctx
        self.gold_v2_ctx = gold_v2_ctx
        
    def create_context(self) -> StageExecutionContext:
        run_context = StageExecutionContext(
            stage="gold_v2",
            pipeline_version="1.0.0"
        )
        return run_context
    
    def transform(
        self, 
        inputs: dict,
        ) -> dict:
        
        dim_skills_df = inputs["gold_v1_dim_skills"]["df"]
        fact_job_skill_df = inputs["gold_v1_fact_job_skills"]["df"]
        
        self.logger.info("building_skill_embeddings")
        skill_embeddings_df = build_skill_embeddings(dim_skills_df=dim_skills_df, spark=self.gold_v2_ctx.spark)
        self.logger.info("building_job_embeddings")
        job_embeddings_df = build_job_embeddings(fact_job_skill_df=fact_job_skill_df, skill_embeddings_df=skill_embeddings_df)
        self.logger.info("building_clusters_data")
        job_membership_df, job_clusters_df, job_centroids_df, job_metadata_df = build_job_clusters(spark=self.gold_v2_ctx.spark, job_embeddings_df=job_embeddings_df)
        
        return {
            "gold_v2_skill_embeddings": skill_embeddings_df,
            "gold_v2_job_embeddings": job_embeddings_df,
            "gold_v2_job_clusters": job_clusters_df,
            "gold_v2_job_membership": job_membership_df,
            "gold_v2_job_centroids": job_centroids_df,
            "gold_v2_job_cluster_metadata": job_metadata_df
        }
    

    
    def compute_metrics(self, outputs: dict) -> dict:
        if not outputs:
            return {}
        skill_embeddings_df = outputs.get("gold_v2_skill_embeddings")
        job_embeddings_df = outputs.get("gold_v2_job_embeddings")
        job_clusters_df = outputs.get("gold_v2_job_clusters")
        job_membership_df = outputs.get("gold_v2_job_membership")
        job_metadata_df = outputs.get("gold_v2_job_cluster_metadata")
        
        # Cache 
        job_membership_df.cache()
        job_clusters_df.cache()
        
        # Counts
        skills_embedded = skill_embeddings_df.count()
        jobs_embedded = job_embeddings_df.count()
        
        # Cluster metrics
        cluster_stats = (
            job_membership_df
            .groupBy()
            .agg(
                countDistinct("cluster_id").alias("num_clusters"),
                avg("distance_to_centroid").alias("avg_distance")
            )
            .first()
        )
        
        # Silhouette score from metadata
        silhouette_row = job_metadata_df.select("silhouette_score").first()
        silhouette = silhouette_row["silhouette_score"] if silhouette_row else None
        
        job_membership_df.unpersist()
        job_clusters_df.unpersist()
        
        return {
            "skills_embedded": skills_embedded,
            "jobs_embedded": jobs_embedded,
            "num_clusters": cluster_stats["num_clusters"],
            "avg_distance_to_centroid": cluster_stats["avg_distance"],
            "silhouette_score": silhouette,
            }
    
    def evaluate_metrics(self, metrics: dict) -> None:
        
        if metrics["num_clusters"] < self.gold_v2_ctx.min_clusters:
            self.logger.warning(
                "model_issue",
                extra={
                    "issue": "too_few_clusters",
                    "num_clusters": metrics["num_clusters"],
                },
            )
        
        if metrics["silhouette_score"] is not None and metrics["silhouette_score"] < self.gold_v2_ctx.min_silhouette:
            self.logger.warning(
                "model_quality_degraded",
                extra={
                    "silhouette_score": metrics["silhouette_score"],
                    "threshold": self.gold_v2_ctx.min_silhouette,
                },
            )
        
        if metrics["jobs_embedded"] == 0:
            self.logger.error(
                "embedding_failure",
                extra={"issue": "no_jobs_embedded"}
            )
        

################################

# class GoldV2Stage(BaseStage):
    
    # def __init__(
        # self, 
        # gold_v1_ctx: GoldV1Context, 
        # gold_v2_ctx: GoldV2Context,
        # datasets: DatasetRegistry,
        # partition_manager: PartitionManager,):
        # super().__init__(spark=gold_v2_ctx.spark, datasets=datasets, partition_manager=partition_manager)
        # self.STAGE_NAME="gold_v2"
        # self.gold_v1_ctx = gold_v1_ctx
        # self.gold_v2_ctx = gold_v2_ctx
        
    # def validate_inputs(self) -> None:
        
        # missing = []
        
        # for ds in self.datasets.get_gold_v1().values():
            # if not ds.path.exists():
                # missing.append(str(ds.path))
        
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input datasets: {', '.join(missing)}"
            # )
    
    # def read(self) -> dict:
        # inputs = {}
        # for ds_name, ds in self.datasets.get_gold_v1().items():
            # partitions = ds.get_available_partitions(
                # partition_manager=self.partition_manager, 
                # stage_name=self.STAGE_NAME
                # )
            # if partitions:
                # df = ds.read_partitions(
                    # spark=self.spark,
                    # partitions=partitions
                    # )
            # else:
                # df = None
            # inputs[ds_name] = {"df": df, "partitions": partitions, "dataset": ds}
        # return inputs
    
    # def create_context(self) -> StageExecutionContext:
        # run_context = StageExecutionContext(
            # stage="gold_v2",
            # pipeline_version="1.0.0"
        # )
        # return run_context
    
    # def transform(
        # self, 
        # inputs: dict,
        # ) -> dict:
        
        # dim_skills_df = inputs["gold_v1_dim_skills"]["df"]
        # fact_job_skill_df = inputs["gold_v1_fact_job_skills"]["df"]
        
        # self.logger.info("building_skill_embeddings")
        # skill_embeddings_df = build_skill_embeddings(dim_skills_df=dim_skills_df, spark=self.gold_v2_ctx.spark)
        # self.logger.info("building_job_embeddings")
        # job_embeddings_df = build_job_embeddings(fact_job_skill_df=fact_job_skill_df, skill_embeddings_df=skill_embeddings_df)
        # self.logger.info("building_clusters_data")
        # job_membership_df, job_clusters_df, job_centroids_df, job_metadata_df = build_job_clusters(spark=self.gold_v2_ctx.spark, job_embeddings_df=job_embeddings_df)
        
        # return {
            # "gold_v2_skill_embeddings": skill_embeddings_df,
            # "gold_v2_job_embeddings": job_embeddings_df,
            # "gold_v2_job_clusters": job_clusters_df,
            # "gold_v2_job_membership": job_membership_df,
            # "gold_v2_job_centroids": job_centroids_df,
            # "gold_v2_job_cluster_metadata": job_metadata_df
        # }
    
    # def write(self, inputs: dict, outputs: dict) -> None:
        
        # if not outputs:
            # self.logger.info("No outputs to write for gold_v2 stage")
            
        # self.spark.conf.set(
            # "spark.sql.sources.partitionOverWriteMode",
            # "dynamic"
        # )
        
        # write_strategy = {}
        
        # for output_name, df in outputs.items():
            # dataset = self.datasets.get(output_name)
            # dataset.write(df)
            # write_strategy[output_name] = dataset.write_mode
            
            
        # self.logger.info(
            # "write_strategy",
            # extra=write_strategy,
        # )
        
        
        # # Mark processed partitions for silver datasets
        # for key, data in inputs.items():
            # partitions = data["partitions"]
            # if partitions:
                # self.partition_manager.mark_processed(
                    # stage_name=self.STAGE_NAME,
                    # partitions=partitions
                # )
    
    # def compute_metrics(self, outputs: dict) -> dict:
        # if not outputs:
            # return {}
        # skill_embeddings_df = outputs.get("gold_v2_skill_embeddings")
        # job_embeddings_df = outputs.get("gold_v2_job_embeddings")
        # job_clusters_df = outputs.get("gold_v2_job_clusters")
        # job_membership_df = outputs.get("gold_v2_job_membership")
        # job_metadata_df = outputs.get("gold_v2_job_cluster_metadata")
        
        # # Cache 
        # job_membership_df.cache()
        # job_clusters_df.cache()
        
        # # Counts
        # skills_embedded = skill_embeddings_df.count()
        # jobs_embedded = job_embeddings_df.count()
        
        # # Cluster metrics
        # cluster_stats = (
            # job_membership_df
            # .groupBy()
            # .agg(
                # countDistinct("cluster_id").alias("num_clusters"),
                # avg("distance_to_centroid").alias("avg_distance")
            # )
            # .first()
        # )
        
        # # Silhouette score from metadata
        # silhouette_row = job_metadata_df.select("silhouette_score").first()
        # silhouette = silhouette_row["silhouette_score"] if silhouette_row else None
        
        # job_membership_df.unpersist()
        # job_clusters_df.unpersist()
        
        # return {
            # "skills_embedded": skills_embedded,
            # "jobs_embedded": jobs_embedded,
            # "num_clusters": cluster_stats["num_clusters"],
            # "avg_distance_to_centroid": cluster_stats["avg_distance"],
            # "silhouette_score": silhouette,
            # }
    
    # def evaluate_metrics(self, metrics: dict) -> None:
        
        # if metrics["num_clusters"] < self.gold_v2_ctx.min_clusters:
            # self.logger.warning(
                # "model_issue",
                # extra={
                    # "issue": "too_few_clusters",
                    # "num_clusters": metrics["num_clusters"],
                # },
            # )
        
        # if metrics["silhouette_score"] is not None and metrics["silhouette_score"] < self.gold_v2_ctx.min_silhouette:
            # self.logger.warning(
                # "model_quality_degraded",
                # extra={
                    # "silhouette_score": metrics["silhouette_score"],
                    # "threshold": self.gold_v2_ctx.min_silhouette,
                # },
            # )
        
        # if metrics["jobs_embedded"] == 0:
            # self.logger.error(
                # "embedding_failure",
                # extra={"issue": "no_jobs_embedded"}
            # )
            
            
################### ALREADY MODIFIED BY MISTAKE

# class GoldV2Stage(BaseStage):
    
    # def __init__(
        # self, 
        # gold_v1_ctx: GoldV1Context, 
        # gold_v2_ctx: GoldV2Context,
        # datasets: DatasetRegistry,
        # partition_manager: PartitionManager,):
        # super().__init__(spark=gold_v2_ctx.spark, datasets=datasets, partition_manager=partition_manager)
        # self.STAGE_NAME="gold_v2"
        # self.gold_v1_ctx = gold_v1_ctx
        # self.gold_v2_ctx = gold_v2_ctx
        
    # def validate_inputs(self) -> None:
        
        # missing = []
        
        # for ds in self.datasets.get_gold_v1().values():
            # if not ds.path.exists():
                # missing.append(str(ds.path))
        
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input datasets: {', '.join(missing)}"
            # )
    
    # def read(self) -> dict:
        # inputs = {}
        # for ds_name, ds in self._datasets.get_gold_v1().items():
            # partitions = ds.get_available_partitions(
                # partition_manager=self.partition_manager, 
                # stage_name=self.STAGE_NAME
                # )
            # if partitions:
                # df = ds.read_partitions(
                    # spark=self.spark,
                    # partitions=partitions
                    # )
            # else:
                # df = None
            # inputs[ds_name] = {"df": df, "partitions": partitions, "dataset": ds}
        # return inputs
    
    # def create_context(self) -> StageExecutionContext:
        # run_context = StageExecutionContext(
            # stage="gold_v2",
            # pipeline_version="1.0.0"
        # )
        # return run_context
    
    # def transform(
        # self, 
        # inputs: dict,
        # ) -> dict:
        
        # dim_skills_df = inputs["gold_v1_dim_skills"]["df"]
        # fact_job_skill_df = inputs["gold_v1_fact_job_skills"]["df"]
        
        # self.logger.info("building_skill_embeddings")
        # skill_embeddings_df = build_skill_embeddings(dim_skills_df=dim_skills_df, spark=self.gold_v2_ctx.spark)
        # self.logger.info("building_job_embeddings")
        # job_embeddings_df = build_job_embeddings(fact_job_skill_df=fact_job_skill_df, skill_embeddings_df=skill_embeddings_df)
        # self.logger.info("building_clusters_data")
        # job_membership_df, job_cluster_df, job_centroids_df, job_metadata_df = build_job_clusters(spark=self.gold_v2_ctx.spark, job_embeddings_df=job_embeddings_df)
        
        # data_date = self.gold_v2_ctx.data_date
        
        # skill_embeddings_df = skill_embeddings_df.withColumn("data_date", lit(data_date))
        # job_embeddings_df = job_embeddings_df.withColumn("data_date", lit(data_date))
        # job_membership_df = job_membership_df.withColumn("data_date", lit(data_date))
        # job_clusters_df = job_clusters_df.withColumn("data_date", lit(data_date))
        # job_centroids_df = job_centroids_df.withColumn("data_date", lit(data_date))
        # job_metadata_df = job_metadata_df.withColumn("data_date", lit(data_date))
        
        # return {
            # "skill_embeddings": skill_embeddings_df,
            # "job_embeddings": job_embeddings_df,
            # "job_clusters": job_clusters_df,
            # "job_membership_df": job_membership_df,
            # "job_centroids_df": job_centroids_df,
            # "job_cluster_metadata_df": job_metadata_df
        # }
    
    # def write(self, outputs: dict) -> None:
        
        # self.spark.conf.set(
            # "spark.sql.sources.partitionOverWriteMode",
            # "dynamic"
        # )
        
        # self.logger.info(
            # "write_strategy",
            # extra={
                # "skill_embeddings": "overwrite",
                # "job_embeddings": "overwrite",
                # "clusters": "overwrite",
                # "membership": "overwrite",
                # "centroids": "overwrite",
                # "clusters_metadata": "overwrite",
            # },
        # )
        
        # self.logger.info(
            # "gold_v2_partition",
            # extra={"data_date": str(self.gold_v1_ctx.data_date)}
        # )
        
        
        # for name, mode, path in [
            # ("skill_embeddings", "overwrite", self.gold_v2_ctx.skill_embeddings_path),
            # ("job_embeddings", "overwrite", self.gold_v2_ctx.job_embeddings_path),
            # ("job_clusters", "overwrite", self.gold_v2_ctx.job_cluster_path),
            # ("job_membership_df", "overwrite", self.gold_v2_ctx.job_cluster_membership_path),
            # ("job_centroids_df", "overwrite", self.gold_v2_ctx.job_centroids_path),
            # ("job_cluster_metadata_df", "overwrite", self.gold_v2_ctx.cluster_metadata_path)
        # ]:
            
            
            # self.storage.write_dataframe(
                # df=outputs[name],
                # path=path,
                # mode=mode,
                # partition_cols=["data_date"]
            # )
    
    # def compute_metrics(self, outputs: dict) -> dict:

        # skill_embeddings_df = outputs["skill_embeddings"]
        # job_embeddings_df = outputs["job_embeddings"]
        # job_clusters_df = outputs["job_clusters"]
        # job_membership_df = outputs["job_membership_df"]
        # job_metadata_df = outputs["job_cluster_metadata_df"]
        
        # # Cache 
        # job_membership_df.cache()
        # job_clusters_df.cache()
        
        # # Counts
        # skills_embedded = skill_embeddings_df.count()
        # jobs_embedded = job_embeddings_df.count()
        
        # # Cluster metrics
        # cluster_stats = (
            # job_membership_df
            # .groupBy()
            # .agg(
                # countDistinct("cluster_id").alias("num_clusters"),
                # avg("distance_to_centroid").alias("avg_distance")
            # )
            # .first()
        # )
        
        # # Silhouette score from metadata
        # silhouette_row = job_metadata_df.select("silhouette_score").first()
        # silhouette = silhouette_row["silhouette_score"] if silhouette_row else None
        
        # job_membership_df.unpersist()
        # job_clusters_df.unpersist()
        
        # return {
            # "skills_embedded": skills_embedded,
            # "jobs_embedded": jobs_embedded,
            # "num_clusters": cluster_stats["num_clusters"],
            # "avg_distance_to_centroid": cluster_stats["avg_distance"],
            # "silhouette_score": silhouette,
            # }
    
    # def evaluate_metrics(self, metrics: dict) -> None:
        
        # if metrics["num_clusters"] < self.gold_v2_ctx.min_clusters:
            # self.logger.warning(
                # "model_issue",
                # extra={
                    # "issue": "too_few_clusters",
                    # "num_clusters": metrics["num_clusters"],
                # },
            # )
        
        # if metrics["silhouette_score"] is not None and metrics["silhouette_score"] < self.gold_v2_ctx.min_silhouette:
            # self.logger.warning(
                # "model_quality_degraded",
                # extra={
                    # "silhouette_score": metrics["silhouette_score"],
                    # "threshold": self.gold_v2_ctx.min_silhouette,
                # },
            # )
        
        # if metrics["jobs_embedded"] == 0:
            # self.logger.error(
                # "embedding_failure",
                # extra={"issue": "no_jobs_embedded"}
            # )

##############

# class GoldV2Stage(BaseStage):
    
    # def __init__(
        # self, 
        # gold_v1_ctx: GoldV1Context, 
        # gold_v2_ctx: GoldV2Context,
        # storage: Storage):
        # super().__init__(spark=gold_v2_ctx.spark, storage=storage)
        # self.gold_v1_ctx = gold_v1_ctx
        # self.gold_v2_ctx = gold_v2_ctx
        
    # def validate_inputs(self) -> None:
        
        # missing = []
        
        # for path in [
            # self.gold_v1_ctx.dim_skills_path,
            # self.gold_v1_ctx.fact_job_skill_path
        # ]:
            # if not self._path_exists(path):
                # missing.append(str(path))
        
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input datasets: {', '.join(missing)}"
            # )
    
    # def read(self) -> dict:
        # return {
            # "dim_skills_df":
                # self.spark.read.parquet(self.gold_v1_ctx.dim_skills_path),
            
            # "fact_job_skill_df":
                # self.spark.read.parquet(self.gold_v1_ctx.fact_job_skill_path)
        # }
    
    # def transform(
        # self, 
        # dim_skills_df: DataFrame,
        # fact_job_skill_df: DataFrame,
        # ) -> dict:
        
        # skill_embeddings_df = build_skill_embeddings(dim_skills_df=dim_skills_df, spark=self.gold_v2_ctx.spark)
        # job_embeddings_df = build_job_embeddings(fact_job_skill_df=fact_job_skill_df, skill_embeddings_df=skill_embeddings_df)
        # job_membership_df, job_cluster_df, job_centroids_df, job_metadata_df = build_job_clusters(spark=self.gold_v2_ctx.spark, job_embeddings_df=job_embeddings_df)
        
        # skill_embeddings_df = skill_embeddings_df.withColumn("data_date", lit(data_date))
        # job_embeddings_df = job_embeddings_df.withColumn("data_date", lit(data_date))
        # job_membership_df = job_membership_df.withColumn("data_date", lit(data_date))
        # job_cluster_df = job_clusters_df.withColumn("data_date", lit(data_date))
        # job_centroids_df = job_centroids_df.withColumn("data_date", lit(data_date))
        # job_metadata_df = job_metadata_df.withColumn("data_date", lit(data_date))
        
        # return {
            # "skill_embeddings": skill_embeddings_df,
            # "job_embeddings": job_embeddings_df,
            # "job_clusters": job_clusters_df,
            # "job_membership_df": job_membership_df,
            # "job_centroids_df": job_centroids_df,
            # "job_cluster_metadata_df": job_metadata_df
        # }
    
    # def write(self, outputs: dict) -> None:
        
        # self.spark.conf.set(
            # "spark.sql.sources.partitionOverWriteMode",
            # "dynamic"
        # )
        
        # for name, mode, path in [
            # ("skill_embeddings", "overwrite", self.gold_v1_ctx.skill_embeddings_path),
            # ("job_embeddings", "overwrite", self.gold_v1_ctx.job_embeddings_path),
            # ("job_clusters", "overwrite", self.gold_v1_ctx.job_cluster_path),
            # ("job_membership_df", "overwrite", self.gold_v1_ctx.job_cluster_membership_path),
            # ("job_centroids_df", "overwrite", self.gold_v1_ctx.job_centroids_path),
            # ("job_cluster_metadata_df", "overwrite", self.gold_v1_ctx.cluster_metadata_path)
        # ]:
            
            
            # self.storage.write_dataframe(
                # df=outputs[name],
                # path=path,
                # mode=mode,
                # partition_cols=["data_date"]
            # )
##############
            # outputs[name] \
            # .write \
            # .mode(mode) \
            # .partitionBy("data_date") \
            # .parquet(path)
