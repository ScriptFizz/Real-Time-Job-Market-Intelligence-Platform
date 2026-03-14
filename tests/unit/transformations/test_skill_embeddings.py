import pytest
from job_plat.transformations.feature.embeddings.build_skill_embeddings import build_skill_embeddings
import numpy as np

class FakeEncoder:
    def encode(self, skills, show_progress_bar=True):
        return np.ones((len(skills), 384))
    
def test_embedding_dimension(gold_dim_skills_data, spark, monkeypatch):
    
    monkeypatch.setattr(
        "job_plat.transformations.feature.embeddings.build_skill_embeddings.SentenceTransformer",
        lambda name: FakeEncoder()
    )
    
    
    result = build_skill_embeddings(gold_dim_skills_data, spark)
    
    row = result.first()
    assert len(row.embedding) == 384
    assert row.embedding_dim == 384
