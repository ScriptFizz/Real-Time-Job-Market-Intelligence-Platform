from typing import List, Dict
import numpy as np

from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN

class EmbeddingSkillNormalizer:
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        eps: float = 0.3,
        min_samples: int = 1
    ):
        self.model = SentenceTransformer(model_name)
        self.eps = eps
        self.min_samples = min_samples
    
    def normalize(self, skills: List[str]) -> Dict[str, str]:
        """
        Returns mapping: raw_skill -> canonical_skill
        """
        embeddings = self.model.encode(skills)
        
        clustering = DBSCAN(
            eps=self.eps,
            min_samples=self.min_samples,
            metric="cosine"
        ).fit(embeddings)
        
        clusters = {}
        for skill, label in zip(skills, clustering.labels_):
            clusters.setdefault(label, []).append(skill)
        
        normalized = {}
        for _, group in clusters.items():
            canonical = self._choose_canonical(group)
            for skill in group:
                normalized[skill] = canonical
        
        return normalized
    
    def _choose_canonical(self, group: List[str]) -> str:
        """
        Simple heuristic:
        - shortest token
        - lowercase
        """
        return min(group, key=len).lower()
