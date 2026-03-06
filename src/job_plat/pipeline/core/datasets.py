from pathlib import Path

class Datasets:
    
    def __init__(self, root: Path):
        
        self.bronze_jobs = Dataset(name="bronze_jobs", path=root / "bronze/jobs")
        
        self.silver_jobs = Dataset(name="silver_jobs",  path=root / "silver/jobs")
        self.silver_job_skills = Dataset(name="silver_job_skills",  path=root / "silver/job_skills")
        
        self.gold_v1_dim_jobs = Dataset(name="gold_v1_dim_jobs", path=root / "gold_v1/dim_jobs")
        self.gold_v1_dim_skills = Dataset(name="gold_v1_dim_skills", path=root / "gold_v1/dim_skills")
        self.gold_v1_fact_job_skills = Dataset(name="gold_v1_fact_job_skills", path=root / "gold_v1/fact_job_skills")
        
        self.gold_v2_skill_embeddings = Dataset(name="gold_v2_skill_embeddings", path=root / "gold_v2/skill_embeddings")
        self.gold_v2_job_embeddings = Dataset(name="gold_v2_job_embeddings", path=root / "gold_v2/job_embeddings")
        self.gold_v2_job_clusters = Dataset(name="gold_v2_job_clusters", path=root / "gold_v2/job_clusters")
        self.gold_v2_job_membership = Dataset(name="gold_v2_job_membership", path=root / "gold_v2/job_membership")
        self.gold_v2_job_centroids = Dataset(name="gold_v2_job_centroids", path=root / "gold_v2/job_centroids")
        self.gold_v2_job_cluster_metadata = Dataset(name="gold_v2_job_cluster_metadata", path=root / "gold_v2/job_cluster_metadata")
