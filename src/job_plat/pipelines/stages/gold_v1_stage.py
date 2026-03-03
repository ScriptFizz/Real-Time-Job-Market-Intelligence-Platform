from job_plat.pipelines.stages.base_stage import BaseStage
from pyspark.sql import DataFrame
from job_plat.pipelines.context.contexts import SilverContext, GoldV1Context
from job_plat.gold.v1_analytics.build_dimensions import build_dim_jobs, build_dim_skills
from job_plat.gold.v1_analytics.fact_job_skills import build_fact_job_skills
from job_plat.utils.storage import Storage
from pathlib import Path

class GoldV1Stage(BaseStage):
    
    def __init__(
        self, 
        gold_v1_ctx: GoldV1Context, 
        silver_ctx: SilverContext, 
        storage: Storage):
        super().__init__(spark=gold_v1_ctx.spark, storage=storage)
        self.gold_v1_ctx = gold_v1_ctx
        self.silver_ctx = silver_ctx
        
    def validate_inputs(self) -> None:
        
        missing = []
        
        for path in [
            self.silver_ctx.jobs_path,
            self.silver_ctx.job_skills_path
        ]:
            if not self._path_exists(path):
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
        inputs: dict
        ) -> dict:
        
        job_silver_df = inputs["job_silver_df"]
        job_skills_silver_df = inputs["job_skills_silver_df"]
        
        self.logger.info("building_dim_jobs")
        dim_jobs_df = build_dim_jobs(job_silver_df=job_silver_df)
        
        self.logger.info("building_dim_skills")
        dim_skills_df = build_dim_skills(job_skills_silver_df=job_skills_silver_df)
        
        self.logger.info("building_fact_jobs")
        fact_df = build_fact_job_skills(
            job_skills_silver_df=job_skills_silver_df,
            dim_skills_df=dim_skills_df
        )
        
        data_date = self.gold_v1_ctx.data_date
        
        dim_jobs_df = dim_jobs_df.withColumn("data_date", lit(data_date))
        dim_skills_df = dim_skills_df.withColumn("data_date", lit(data_date))
        fact_df = fact_df.withColumn("data_date", lit(data_date))
        
        # Computing metrics
        
        dim_jobs_df.cache()
        dim_skills_df.cache()
        fact_df.cache()
        
        metrics = {
            "jobs": dim_jobs_df.count(),
            "skills": dim_skills_df.count(),
            "fact_rows": fact_df.count(),
            "orphan_facts_detected": (
                fact_df
                .join(dim_jobs_df.select("job_id"), "job_id", "left_anti")
                .limit(1)
                .count()
            ) > 0
        }
        
        self.logger.info(
            "gold_v1_metrics",
            extra={
                "jobs": metrics["jobs"],
                "skills": metrics["skills"],
                "fact_rows": metrics["fact_rows"],
                "fact_per_job_ratio": round(
                    metrics["fact_rows"] / metrics["jobs"], 2
                ) if metrics["jobs"] else 0,
                "orphan_facts_detected": metrics["orphan_facts_detected"],
            },
        )
        
        dim_jobs_df.unpersist()
        dim_skills_df.unpersist()
        fact_df.unpersist()
        
        return {
            "dim_jobs": dim_jobs_df,
            "dim_skills": dim_skills_df,
            "fact_job_skills": fact_df
        }
        
    def compute_metrics(self, outputs: dict) -> dict:
        
        dim_jobs_df = outputs["dim_jobs"]
        #dim_skills_df = outputs["dim_skills"]
        fact_df = outputs["fact_job_skills"]
        
        dim_jobs_df.cache()
        dim_skills_df.cache()
        fact_df.cache()
        
        jobs = dim_jobs_df.count()
        skills =  dim_skills_df.count()
        fact_rows = fact_df.count()
        orphan_facts_detected = (
                fact_df
                .join(dim_jobs_df.select("job_id"), "job_id", "left_anti")
                .limit(1)
                .count()
            ) > 0
        
        dim_jobs_df.unpersist()
        dim_skills_df.unpersist()
        fact_df.unpersist()
        
        return {
            "jobs": jobs,
            "skills": skills,
            "fact_rows": fact_rows,
            "fact_per_job_ratio": round(
                fact_rows / jobs, 2
            ) if jobs else 0,
            "orphan_facts_detected": orphan_facts_detected
        }
    
    def evaluate_metrics(self, metrics: dict) -> None:
        
        if metrics["orphan_facts_detected"]:
            self.logger.warning(
                "data_quality_issue",
                extra={"issue": "orphan_facts_detected"}
            )
        
        if metrics["fact_per_job_ratio"] > self.gold_v1_ctx.fact_per_job_ratio_threshold:
            self.logger.warning(
                "data_anomaly_detected",
                extra={
                    "issue": "fact_per_job_ratio_high",
                    "value": metrics["fact_per_job_ratio"],
                    "threshold": self.gold_v1_ctx.fact_per_job_ratio_threshold,
                }
            )
        
        if metrics["jobs"] == 0:
            self.logger.error(
                "data_quality_issue",
                extra={"issue": "no_jobs_generated"}
            )
    
    def write(self, outputs: dict) -> None:
        
        self.spark.conf.set(
            "spark.sql.sources.partitionOverWriteMode",
            "dynamic"
        )
        
        self.logger.info(
            "write_strategy",
            extra={
                "dim_mode": "overwrite",
                "fact_mode": "append",
            },
        )
        
        self.logger.info(
            "gold_v1_partition",
            extra={"data_date": str(self.gold_v1_ctx.data_date)}
        )
        
        for name, mode, path in [
            ("dim_jobs", "overwrite", self.gold_v1_ctx.dim_jobs_path),
            ("dim_skills", "overwrite", self.gold_v1_ctx.dim_skills_path),
            ("fact_job_skills", "append", self.gold_v1_ctx.fact_job_skill_path),
        ]:
            self.storage.write_dataframe(
                df=outputs[name],
                path=path,
                mode=mode,
                partition_cols=["data_date"]
            )
            
            # outputs[name] \
            # .write \
            # .mode(mode) \
            # .partitionBy("data_date") \
            # .parquet(path)
###########################################################

# class GoldV1Stage(BaseStage):
    
    # def __init__(
        # self, 
        # gold_v1_ctx: GoldV1Context, 
        # silver_ctx: SilverContext, 
        # storage: Storage):
        # super().__init__(spark=gold_v1_ctx.spark, storage=storage)
        # self.gold_v1_ctx = gold_v1_ctx
        # self.silver_ctx = silver_ctx
        
    # def validate_inputs(self) -> None:
        
        # missing = []
        
        # for path in [
            # self.silver_ctx.jobs_path,
            # self.silver_ctx.job_skills_path
        # ]:
            # if not self._path_exists(path):
                # missing.append(str(path))
        
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input datasets: {', '.join(missing)}"
            # )
    
    # def read(self) -> dict:
        # return {
            # "job_silver_df":
                # self.spark.read.parquet(self.silver_ctx.jobs_path),
            
            # "job_skills_silver_df":
                # self.spark.read.parquet(self.silver_ctx.job_skills_path)
        # }
    
    # def transform(
        # self, 
        # job_silver_df: DataFrame,
        # job_skills_silver_df: DataFrame,
        # ) -> dict:
        
        # dim_jobs_df = build_dim_jobs(job_silver_df=job_silver_df)
        # dim_skills_df = build_dim_skills(job_skills_silver_df=job_skills_silver_df)
        # fact_df = build_fact_job_skills(
            # job_skills_silver_df=job_skills_silver_df,
            # dim_skills_df=dim_skills_df
        # )
        
        # data_date = self.gold_v1_ctx.data_date
        
        # dim_jobs_df = dim_jobs_df.withColumn("data_date", lit(data_date))
        # dim_skills_df = dim_skills_df.withColumn("data_date", lit(data_date))
        # fact_df = fact_df.withColumn("data_date", lit(data_date))
        
        # return {
            # "dim_jobs": dim_jobs_df,
            # "dim_skills": dim_skills_df,
            # "fact_job_skills": fact_df
        # }
    
    # def write(self, outputs: dict) -> None:
        
        # self.spark.conf.set(
            # "spark.sql.sources.partitionOverWriteMode",
            # "dynamic"
        # )
        
        # for name, mode, path in [
            # ("dim_jobs", "overwrite", self.gold_v1_ctx.dim_jobs_path),
            # ("dim_skills", "overwrite", self.gold_v1_ctx.dim_skills_path),
            # ("fact_job_skills", "append", self.gold_v1_ctx.fact_job_skill_path),
        # ]:
            # self.storage.write_dataframe(
                # df=outputs[name],
                # path=path,
                # mode=mode,
                # partition_cols=["data_date"]
            # )            
            
            
############################################################
        # outputs["dim_jobs"] \
            # .write \
            # .mode("append") \
            # .partitionBy("data_date") \
            # .parquet(self.gold_v1_ctx.dim_jobs_path)
        
        # outputs["dim_skills"] \
            # .write \
            # .mode("append") \
            # .partitionBy("data_date") \
            # .parquet(self.gold_v1_ctx.dim_skills_path)
        
        # outputs["fact_job_skills"] \
            # .write \
            # .mode("append") \
            # .partitionBy("data_date") \
            # .parquet(self.gold_v1_ctx.fact_job_skill_path)
    
    def _path_exists(self, path: str | Path) -> bool:
        try:
            self.spark.read.parquet(path).limit(1).collect()
            return True
        except Exception:
            return False




# class GoldV1Stage(BaseStage):
    
    # def __init__(self, gold_v1_ctx: GoldV1Context, silver_ctx: SilverContext):
        # super().__init__(gold_v1_ctx.spark)
        # self.gold_v1_ctx = gold_v1_ctx
        # self.silver_ctx = silver_ctx
        
    # def validate_inputs(self) -> None:
        
        # missing = []
        
        # for path in [
            # self.silver_ctx.jobs_path,
            # self.silver_ctx.job_skills_path
        # ]:
            # if not self._path_exists(path):
                # missing.append(str(path))
        
        # if missing:
            # raise FileNotFoundError(
                # f"Missing input datasets: {', '.join(missing)}"
            # )
    
    # def read(self) -> dict:
        # return {
            # "job_silver_df":
                # self.spark.read.parquet(self.silver_ctx.jobs_path),
            
            # "job_skills_silver_df":
                # self.spark.read.parquet(self.silver_ctx.job_skills_path)
        # }
    
    # def transform(
        # self, 
        # job_silver_df: DataFrame,
        # job_skills_silver_df: DataFrame,
        # ) -> dict:
        
        # dim_jobs_df = build_dim_jobs(job_silver_df=job_silver_df)
        # dim_skills_df = build_dim_skills(job_skills_silver_df=job_skills_silver_df)
        # fact_df = build_fact_job_skills(
            # job_skills_silver_df=job_skills_silver_df,
            # dim_skills_df=dim_skills_df
        # )
        
        # return {
            # "dim_jobs": dim_jobs_df,
            # "dim_skills": dim_skills_df,
            # "fact_job_skills": fact_df
        # }
    
    # def write(self, outputs: dict) -> None:
        # outputs["dim_jobs"].write.mode("overwrite").parquet(
            # self.gold_v1_ctx.dim_jobs_path
        # )
        # outputs["dim_skills"].write.mode("overwrite").parquet(
            # self.gold_v1_ctx.dim_skills_path
        # )
        # outputs["fact_job_skills"].write.mode("overwrite").parquet(
            # self.gold_v1_ctx.fact_job_skill_path
        # )
    
    # def _path_exists(self, path: str | Path) -> bool:
        # try:
            # self.spark.read.parquet(path).limit(1).collect()
            # return True
        # except Exception:
            # return False
