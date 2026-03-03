from job_plat.pipelines.stages.base_stage import BaseStage
from job_plat.pipelines.context.contexts import GoldV1Context, GoldV2Context
from job_plat.gold.v2_intelligence.embeddings.build_skill_embeddings import build_skill_embeddings
from job_plat.gold.v2_intelligence.embeddings.build_job_embeddings import build_job_embeddings
from job_plat.gold.v2_intelligence.clusters.build_job_clusters import build_job_clusters
from job_plat.utils.storage import Storage
from pyspark.sql import DataFrame

class GoldV2Stage(BaseStage):
    
    def __init__(
        self, 
        gold_v1_ctx: GoldV1Context, 
        gold_v2_ctx: GoldV2Context,
        storage: Storage):
        super().__init__(spark=gold_v2_ctx.spark, storage=storage)
        self.gold_v1_ctx = gold_v1_ctx
        self.gold_v2_ctx = gold_v2_ctx
        
    def validate_inputs(self) -> None:
        
        missing = []
        
        for path in [
            self.gold_v1_ctx.dim_skills_path,
            self.gold_v1_ctx.fact_job_skill_path
        ]:
            if not self._path_exists(path):
                missing.append(str(path))
        
        if missing:
            raise FileNotFoundError(
                f"Missing input datasets: {', '.join(missing)}"
            )
    
    def read(self) -> dict:
        return {
            "dim_skills_df":
                self.spark.read.parquet(self.gold_v1_ctx.dim_skills_path),
            
            "fact_job_skill_df":
                self.spark.read.parquet(self.gold_v1_ctx.fact_job_skill_path)
        }
    
    def transform(
        self, 
        inputs: dict,
        ) -> dict:
        
        dim_skills_df = inputs["dim_skills_df"]
        fact_job_skill_df = inputs["fact_job_skill_df"]
        
        self.logger.info("building_skill_embeddings")
        skill_embeddings_df = build_skill_embeddings(dim_skills_df=dim_skills_df, spark=self.gold_v2_ctx.spark)
        self.logger.info("building_job_embeddings")
        job_embeddings_df = build_job_embeddings(fact_job_skill_df=fact_job_skill_df, skill_embeddings_df=skill_embeddings_df)
        self.logger.info("building_clusters_data")
        job_membership_df, job_cluster_df, job_centroids_df, job_metadata_df = build_job_clusters(spark=self.gold_v2_ctx.spark, job_embeddings_df=job_embeddings_df)
        
        data_date = self.gold_v2_ctx.data_date
        
        skill_embeddings_df = skill_embeddings_df.withColumn("data_date", lit(data_date))
        job_embeddings_df = job_embeddings_df.withColumn("data_date", lit(data_date))
        job_membership_df = job_membership_df.withColumn("data_date", lit(data_date))
        job_clusters_df = job_clusters_df.withColumn("data_date", lit(data_date))
        job_centroids_df = job_centroids_df.withColumn("data_date", lit(data_date))
        job_metadata_df = job_metadata_df.withColumn("data_date", lit(data_date))
        
        return {
            "skill_embeddings": skill_embeddings_df,
            "job_embeddings": job_embeddings_df,
            "job_clusters": job_clusters_df,
            "job_membership_df": job_membership_df,
            "job_centroids_df": job_centroids_df,
            "job_cluster_metadata_df": job_metadata_df
        }
    
    def write(self, outputs: dict) -> None:
        
        self.spark.conf.set(
            "spark.sql.sources.partitionOverWriteMode",
            "dynamic"
        )
        
        self.logger.info(
            "write_strategy",
            extra={
                "skill_embeddings": "overwrite",
                "job_embeddings": "overwrite",
                "clusters": "overwrite",
                "membership": "overwrite",
                "centroids": "overwrite",
                "clusters_metadata": "overwrite",
            },
        )
        
        self.logger.info(
            "gold_v2_partition",
            extra={"data_date": str(self.gold_v1_ctx.data_date)}
        )
        
        
        for name, mode, path in [
            ("skill_embeddings", "overwrite", self.gold_v2_ctx.skill_embeddings_path),
            ("job_embeddings", "overwrite", self.gold_v2_ctx.job_embeddings_path),
            ("job_clusters", "overwrite", self.gold_v2_ctx.job_cluster_path),
            ("job_membership_df", "overwrite", self.gold_v2_ctx.job_cluster_membership_path),
            ("job_centroids_df", "overwrite", self.gold_v2_ctx.job_centroids_path),
            ("job_cluster_metadata_df", "overwrite", self.gold_v2_ctx.cluster_metadata_path)
        ]:
            
            
            self.storage.write_dataframe(
                df=outputs[name],
                path=path,
                mode=mode,
                partition_cols=["data_date"]
            )
    
    def compute_metrics(outputs: dict) -> dict:
        
        self.logger.info("computing_gold_v2_metrics")
        
        skill_embeddings_df = outputs["skill_embeddings"]
        job_embeddings_df = outputs["job_embeddings"]
        job_clusters_df = outputs["job_clusters"]
        job_membership_df = outputs["job_membership_df"]
        job_metadata_df = outputs["job_cluster_metadata_df"]
        
        # Cache 
        job_membership_df.cache()
        job_clusters_df.cache()
        
        # Counts


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
