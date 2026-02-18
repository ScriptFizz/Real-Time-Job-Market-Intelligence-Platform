from job_plat.pipelines.stages.base_stage import BaseStage
from job_plat.config.context import GoldV1Context, GoldV2Context
from job_plat.gold.v2_intelligence.embeddings.build_skill_embeddings import build_skill_embeddings
from job_plat.gold.v2_intelligence.embeddings.build_job_embeddings import build_job_embeddings
from job_plat.gold.v2_intelligence.clusters.build_job_clusters import build_job_clusters


class GoldV2Stage(BaseStage):
    
    def __init__(self, gold_v1_ctx: GoldV1Context, gold_v2_ctx: GoldV2Context):
        super().__init__(gold_v2_ctx.spark)
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
        dim_skills_df: DataFrame,
        fact_job_skill_df: DataFrame,
        ) -> dict:
        
        skill_embeddings_df = build_skill_embeddings(dim_skills_df=dim_skills_df, spark=self.gold_v2_ctx.spark)
        job_embeddings_df = build_job_embeddings(fact_job_skill_df=fact_job_skill_df, skill_embeddings_df=skill_embeddings_df)
        job_membership_df, job_cluster_df, job_centroids_df, job_metadata_df = build_job_clusters(spark=self.gold_v2_ctx.spark, job_embeddings_df=job_embeddings_df)
        
        skill_embeddings_df = skill_embeddings_df.withColumn("data_date", lit(data_date))
        job_embeddings_df = job_embeddings_df.withColumn("data_date", lit(data_date))
        job_membership_df = job_membership_df.withColumn("data_date", lit(data_date))
        job_cluster_df = job_clusters_df.withColumn("data_date", lit(data_date))
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
        
        for name, mode, path in [
            ("skill_embeddings", "overwrite", self.gold_v1_ctx.skill_embeddings_path),
            ("job_embeddings", "overwrite", self.gold_v1_ctx.job_embeddings_path),
            ("job_clusters", "overwrite", self.gold_v1_ctx.job_cluster_path),
            ("job_membership_df", "overwrite", self.gold_v1_ctx.),
            ("job_centroids_df", "overwrite", self.gold_v1_ctx.),
            ("job_cluster_metadata_df", "overwrite", self.gold_v1_ctx.)
        ]:
            
            outputs[name] \
            .write \
            .mode(mode) \
            .partitionBy("data_date") \
            .parquet(path)
