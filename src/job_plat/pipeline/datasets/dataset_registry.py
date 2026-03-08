from pathlib import Path
from job_plat.storage.storages import Storage
from job_plat.pipeline.datasets.dataset import Dataset
# class BronzeJobs(Dataset):
    # name = "bronze_job"

class DatasetRegistry:
    
    def __init__(
        self,
        root: Path,
        storage: Storage,
        dataset_defs: list[type]
        ):
            
        self._datasets = {}
        for ds in dataset_defs:
            dataset = Dataset(
                name = ds.NAME,
                path= Path(root) / ds.RELATIVE_PATH,
                storage=storage,
                partition_column=getattr(ds, "PARTITION_COLUMN", "ingestion_date"),
                write_mode=getattr(ds, "WRITE_MODE", "append"),
                file_format=getattr(ds, "FILE_FORMAT", "parquet")
            )
            self._datasets[ds] = dataset
    
    def get(self, dataset_cls: type) -> Dataset:
        return self._datasets[dataset_cls]
    
    def list(self) -> list:
        return list(self._datasets.values())

###################################

# class DatasetRegistry:
    
    # def __init__(
        # self,
        # root: Path,
        # storage: Storage
        # ):
        # self._datasets = {
            # "bronze_jobs": Dataset(name="bronze_jobs", path=root / "bronze/jobs", storage=storage),
        
            # "silver_jobs": Dataset(name="silver_jobs",  path=root / "silver/jobs", storage=storage),
            # "silver_job_skills": Dataset(name="silver_job_skills",  path=root / "silver/job_skills", storage=storage),
            
            # "gold_v1_dim_jobs": Dataset(name="gold_v1_dim_jobs", path=root / "gold_v1/dim_jobs", storage=storage, mode="overwrite"),
            # "gold_v1_dim_skills": = Dataset(name="gold_v1_dim_skills", path=root / "gold_v1/dim_skills", storage=storage, mode="overwrite"),
            # "gold_v1_fact_job_skills": = Dataset(name="gold_v1_fact_job_skills", path=root / "gold_v1/fact_job_skills", storage=storage),
            
            # "gold_v2_skill_embeddings": Dataset(name="gold_v2_skill_embeddings", path=root / "gold_v2/skill_embeddings", storage=storage),
            # "gold_v2_job_embeddings": Dataset(name="gold_v2_job_embeddings", path=root / "gold_v2/job_embeddings", storage=storage),
            # "gold_v2_job_clusters": Dataset(name="gold_v2_job_clusters", path=root / "gold_v2/job_clusters", storage=storage),
            # "gold_v2_job_membership": Dataset(name="gold_v2_job_membership", path=root / "gold_v2/job_membership", storage=storage),
            # "gold_v2_job_centroids": = Dataset(name="gold_v2_job_centroids", path=root / "gold_v2/job_centroids", storage=storage),
            # "gold_v2_job_cluster_metadata": Dataset(name="gold_v2_job_cluster_metadata", path=root / "gold_v2/job_cluster_metadata", storage=storage)

        # }
    
    # def register(self, dataset: Dataset) -> None:
        # self._datasets[dataset.name] = dataset
    
    # def get(self, name: str) -> Dataset:
        # return self._datasets.get(name)
    
    # def list(self) -> list:
        # return list(self._datasets.values())
    
    # def get_bronze(self) -> dict:
        # return {k, v for k, v in self_datasets.items if k in ["bronze_jobs"]}
        
    # def get_silver(self) -> dict:
        # return {k, v for k, v in self_datasets.items if k in ["silver_jobs", "silver_job_skills"]}
    
    # def get_gold_v1(self) -> dict:
        # return {k, v for k, v in self_datasets.items if k in ["gold_v1_dim_jobs", "gold_v1_dim_skills", "gold_v1_fact_job_skills"]}
    
    # def get_gold_v2(self) -> dict:
        # return {k, v for k, v in self_datasets.items if k in ["gold_v2_skill_embeddings", "gold_v2_job_embeddings", "gold_v2_job_clusters", "gold_v2_job_membership", "gold_v2_job_centroids", "gold_v2_job_cluster_metadata"]}
################################


# class Datasets:
    
    # def __init__(self, root: Path):
        
        # self.bronze_jobs = Dataset(name="bronze_jobs", path=root / "bronze/jobs")
        
        # self.silver_jobs = Dataset(name="silver_jobs",  path=root / "silver/jobs")
        # self.silver_job_skills = Dataset(name="silver_job_skills",  path=root / "silver/job_skills")
        
        # self.gold_v1_dim_jobs = Dataset(name="gold_v1_dim_jobs", path=root / "gold_v1/dim_jobs")
        # self.gold_v1_dim_skills = Dataset(name="gold_v1_dim_skills", path=root / "gold_v1/dim_skills")
        # self.gold_v1_fact_job_skills = Dataset(name="gold_v1_fact_job_skills", path=root / "gold_v1/fact_job_skills")
        
        # self.gold_v2_skill_embeddings = Dataset(name="gold_v2_skill_embeddings", path=root / "gold_v2/skill_embeddings")
        # self.gold_v2_job_embeddings = Dataset(name="gold_v2_job_embeddings", path=root / "gold_v2/job_embeddings")
        # self.gold_v2_job_clusters = Dataset(name="gold_v2_job_clusters", path=root / "gold_v2/job_clusters")
        # self.gold_v2_job_membership = Dataset(name="gold_v2_job_membership", path=root / "gold_v2/job_membership")
        # self.gold_v2_job_centroids = Dataset(name="gold_v2_job_centroids", path=root / "gold_v2/job_centroids")
        # self.gold_v2_job_cluster_metadata = Dataset(name="gold_v2_job_cluster_metadata", path=root / "gold_v2/job_cluster_metadata")
