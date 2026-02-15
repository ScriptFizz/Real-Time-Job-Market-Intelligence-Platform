from job_plat.pipelines.stages.base_stage import BaseStage
from job_plat.config.context import SilverContext, GoldV1Context
from job_plat.gold.v1_analytics.build_dimensions import build_dim_jobs, build_dim_skills, build_fact_job_skills


class GoldV1Stage(BaseStage):
    
    def __init__(self, gold_v1_ctx: GoldV1Context, silver_ctx: SilverContext):
        super().__init__(gold_ctx.spark)
        self.gold_v1_ctx = gold_v1_ctx
        self.silver_ctx = silver_ctx
        
    def validate_inputs(self) -> None:
        
        missing = []
        
        for path in [
            self.silver_ctx.jobs_path,
            self.silver_ctx.job_skills_path
        ]:
            if not self.__path_exists(path):
                missing.append(str(path))
        
        if missing:
            raise FileNotFoundError(
                f"Missing input datasets: {', '.join(missing)}"
            )
    
    def read(self) -> dict:
        return {
            "job_silver_df":
                self.spark.read.parquet(self.silver_ctx.jobs_path),
            
            "job_skills_silver_df":
                self.spark.read.parquet(self.silver_ctx.job_skills_path)
        }
    
    def transform(
        self, 
        job_silver_df: DataFrame,
        job_skills_silver_df: DataFrame,
        ) -> dict:
        
        dim_jobs_df = build_dim_jobs(job_silver_df)
        dim_skills_df = build_dim_skills(job_skills_silver_df)
        fact_df = build_fact_job_skills(
            job_skills_silver_df,
            dim_skills_df
        )
        
        return {
            "dim_jobs": dim_jobs_df,
            "dim_skills": dim_skills_df,
            "fact_job_skills": fact_df
        }
    
    def write(self, outputs: dict) -> None:
        outputs["dim_jobs"].write.mode("overwrite").parquet(
            self.gold_v1_ctx.dim_jobs_path
        )
        outputs["dim_skills"].write.mode("overwrite").parquet(
            self.gold_v1_ctx.dim_skills_path
        )
        outputs["fact_job_skills"].write.mode("overwrite").parquet(
            self.gold_v1_ctx.fact_job_skill_path
        )
    
    def _path_exists(self, path: str | Path) -> bool:
        try:
            self.spark.read.parquet(path).limit(1).collect()
            return True
        except Exception:
            return False
