from typing import List, Dict, Any
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
        """
        
        """
        self.model = SentenceTransformer(model_name)
        self.eps = eps
        self.min_samples = min_samples
    
    def normalize(self, skills: List[str]) -> List[Dict[str, Any]]:
        """
        Returns one record per canonical skill:
        {
            canonical_skill (str): Representative name of the skill.
            embedding (List[float]): Embedding vector of the skill.
            aliases (List[str]): List of aliases name for the skill.
        }
        """
        embeddings = self.model.encode(skills, show_progress_bar=True)
        
        clustering = DBSCAN(
            eps=self.eps,
            min_samples=self.min_samples,
            metric="cosine"
        ).fit(embeddings)
        
        clusters = {}
        for skill, emb, label in zip(skills, embeddings, clustering.labels_):
            clusters.setdefault(label, []).append((skill, emb))
        
        results = []
        for group in clusters.values():
            skills_in_cluster = [s for s,_ in group]
            vectors_in_cluster = np.array([e for _, e in group])
            
            canonical = self._choose_canonical(skills_in_cluster)
            canonical_embedding = vectors_in_cluster.mean(axis=0)
            
            results.append({
                "canonical_skill": canonical,
                "embedding": canonical_embedding.tolist(),
                "aliases": skills_in_cluster
            })
        
        return results

    
    def _choose_canonical(self, group: List[str]) -> str:
        """
        Simple heuristic:
        - shortest token
        - lowercase
        
        Args:
            group (List[str]): List of skills.
            
        Returns:
            (str): Representative name of the group of skills.
        """
        return min(group, key=len).lower()
