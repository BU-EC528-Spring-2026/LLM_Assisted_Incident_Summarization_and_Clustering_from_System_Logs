"""
Embedding Model Evaluator
Benchmarks multiple embedding models on retrieval, clustering, and cost metrics.
"""

import json
import time
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from dataclasses import dataclass
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
from sklearn.metrics import (
    silhouette_score,
    davies_bouldin_score,
    calinski_harabasz_score,
    adjusted_rand_score,
    normalized_mutual_info_score,
    v_measure_score,
    f1_score
)
import warnings
warnings.filterwarnings('ignore')


@dataclass
class EmbeddingResult:
    """Store embedding evaluation results."""
    model_name: str
    embeddings: np.ndarray
    encode_time_s: float
    memory_mb: float


@dataclass
class EvaluationMetrics:
    """Store all evaluation metrics."""
    model_name: str
    # Retrieval metrics
    recall_at_1: float
    recall_at_5: float
    recall_at_10: float
    mrr: float
    ndcg_at_5: float
    ndcg_at_10: float
    # Clustering metrics
    silhouette: float
    davies_bouldin: float
    calinski_harabasz: float
    # Cost metrics
    encode_time_ms: float
    memory_mb: float
    throughput_docs_per_sec: float
    # Robustness
    noise_robustness_drop: float


class EmbeddingEvaluator:
    """Evaluate and compare embedding models."""
    
    def __init__(self, seed: int = 42):
        self.seed = seed
        np.random.seed(seed)
    
    def load_incidents(self, incidents_path: str) -> List[Dict]:
        """Load incidents from JSON."""
        with open(incidents_path, 'r') as f:
            incidents = json.load(f)
        return incidents
    
    def extract_texts(self, incidents: List[Dict], text_field: str = 'summary') -> List[str]:
        """Extract text from incidents (assumes LLM summaries exist)."""
        texts = []
        for inc in incidents:
            if text_field in inc:
                texts.append(inc[text_field])
            else:
                text = f"{inc.get('severity', '')} in {','.join(inc.get('components', []))} ({inc.get('num_logs', 0)} logs)"
                texts.append(text)
        return texts
    
    def encode_with_timing(self, model, texts: List[str]) -> Tuple[np.ndarray, float]:
        """Encode texts and measure time."""
        start = time.time()
        embeddings = model.encode(texts, show_progress_bar=False)
        elapsed = time.time() - start
        return embeddings, elapsed
    
    def estimate_memory(self, embeddings: np.ndarray) -> float:
        """Estimate memory usage in MB."""
        return embeddings.nbytes / (1024 * 1024)
    
    def create_retrieval_pairs(
        self,
        texts: List[str],
        similarity_threshold: float = 0.7,
        min_pairs: int = 5
    ) -> List[Tuple[int, int]]:
        """
        Create synthetic query-relevant-incident pairs using text similarity.
        
        For evaluation, we use high-similarity pairs as "relevant matches".
        Query index, relevant incident index.
        """
        from sklearn.metrics.pairwise import cosine_similarity
        from sentence_transformers import SentenceTransformer
        
        # Quick embedding for pair generation (using fast model)
        temp_model = SentenceTransformer('all-MiniLM-L6-v2')
        temp_embeddings = temp_model.encode(texts, show_progress_bar=False)
        
        sim_matrix = cosine_similarity(temp_embeddings)
        
        pairs = []
        for i in range(len(texts)):
            # Find similar incidents
            similar_indices = np.where(sim_matrix[i] > similarity_threshold)[0]
            similar_indices = similar_indices[similar_indices != i]  # Exclude self
            
            for j in similar_indices[:min_pairs]:
                pairs.append((i, j))
        
        return pairs if pairs else [(i, (i + 1) % len(texts)) for i in range(min(5, len(texts)))]
    
    def recall_at_k(
        self,
        embeddings: np.ndarray,
        retrieval_pairs: List[Tuple[int, int]],
        k: int
    ) -> float:
        """Compute Recall@k."""
        hits = 0
        total = len(retrieval_pairs)
        
        if total == 0:
            return 0.0
        
        sim_matrix = cosine_similarity(embeddings)
        
        for query_idx, relevant_idx in retrieval_pairs:
            # Get top-k most similar incidents (excluding query itself)
            sims = sim_matrix[query_idx].copy()
            sims[query_idx] = -1  # Mask self
            top_k_indices = np.argsort(sims)[-k:][::-1]
            
            if relevant_idx in top_k_indices:
                hits += 1
        
        return hits / total
    
    def mrr(self, embeddings: np.ndarray, retrieval_pairs: List[Tuple[int, int]]) -> float:
        """Compute Mean Reciprocal Rank."""
        reciprocal_ranks = []
        sim_matrix = cosine_similarity(embeddings)
        
        for query_idx, relevant_idx in retrieval_pairs:
            sims = sim_matrix[query_idx].copy()
            sims[query_idx] = -1
            ranked = np.argsort(sims)[::-1]
            
            try:
                rank = list(ranked).index(relevant_idx) + 1
                reciprocal_ranks.append(1.0 / rank)
            except ValueError:
                pass
        
        return np.mean(reciprocal_ranks) if reciprocal_ranks else 0.0
    
    def ndcg_at_k(
        self,
        embeddings: np.ndarray,
        retrieval_pairs: List[Tuple[int, int]],
        k: int
    ) -> float:
        """Compute nDCG@k."""
        dcg_scores = []
        sim_matrix = cosine_similarity(embeddings)
        
        for query_idx, relevant_idx in retrieval_pairs:
            sims = sim_matrix[query_idx].copy()
            sims[query_idx] = -1
            top_k_indices = np.argsort(sims)[-k:][::-1]
            
            # DCG: relevance=1 if relevant, 0 otherwise
            dcg = 0
            for i, idx in enumerate(top_k_indices):
                rel = 1 if idx == relevant_idx else 0
                dcg += rel / np.log2(i + 2)
            
            # IDCG: perfect ranking has relevant at position 0
            idcg = 1 / np.log2(2)
            ndcg = dcg / idcg if idcg > 0 else 0
            dcg_scores.append(ndcg)
        
        return np.mean(dcg_scores) if dcg_scores else 0.0
    
    def evaluate_clustering(
        self,
        embeddings: np.ndarray,
        n_clusters: int = 5,
        labels: Optional[np.ndarray] = None
    ) -> Dict[str, float]:
        """Evaluate clustering quality."""
        # Fit KMeans
        kmeans = KMeans(n_clusters=n_clusters, random_state=self.seed, n_init=10)
        cluster_labels = kmeans.fit_predict(embeddings)
        
        metrics = {}
        
        # Unsupervised metrics
        try:
            metrics['silhouette'] = silhouette_score(embeddings, cluster_labels)
        except:
            metrics['silhouette'] = 0.0
        
        try:
            metrics['davies_bouldin'] = davies_bouldin_score(embeddings, cluster_labels)
        except:
            metrics['davies_bouldin'] = float('inf')
        
        try:
            metrics['calinski_harabasz'] = calinski_harabasz_score(embeddings, cluster_labels)
        except:
            metrics['calinski_harabasz'] = 0.0
        
        # Supervised metrics (if labels provided)
        if labels is not None and len(set(labels)) > 1:
            try:
                metrics['ari'] = adjusted_rand_score(labels, cluster_labels)
                metrics['nmi'] = normalized_mutual_info_score(labels, cluster_labels)
                metrics['v_measure'] = v_measure_score(labels, cluster_labels)
            except:
                pass
        
        return metrics
    
    def add_noise_to_texts(self, texts: List[str], noise_level: float = 0.1) -> List[str]:
        """Add noise by truncating or shuffling text."""
        noisy_texts = []
        for text in texts:
            if np.random.rand() < noise_level:
                # Randomly truncate
                truncate_at = max(1, int(len(text) * (1 - noise_level)))
                noisy_texts.append(text[:truncate_at])
            else:
                noisy_texts.append(text)
        return noisy_texts
    
    def evaluate_robustness(
        self,
        model,
        texts: List[str],
        clean_embeddings: np.ndarray,
        retrieval_pairs: List[Tuple[int, int]]
    ) -> float:
        """Evaluate robustness to noisy input."""
        noisy_texts = self.add_noise_to_texts(texts, noise_level=0.1)
        noisy_embeddings, _ = self.encode_with_timing(model, noisy_texts)
        
        clean_recall_at_5 = self.recall_at_k(clean_embeddings, retrieval_pairs, 5)
        noisy_recall_at_5 = self.recall_at_k(noisy_embeddings, retrieval_pairs, 5)
        
        # Return drop in performance (as percentage)
        drop = (clean_recall_at_5 - noisy_recall_at_5) / clean_recall_at_5 * 100 if clean_recall_at_5 > 0 else 0
        return max(0, drop)
    
    def evaluate_model(
        self,
        model,
        model_name: str,
        texts: List[str],
        retrieval_pairs: List[Tuple[int, int]],
        labels: Optional[np.ndarray] = None,
        n_clusters: int = 5,
        evaluate_robustness: bool = False
    ) -> EvaluationMetrics:
        """Run full evaluation on a model."""
        
        print(f"\n{'='*60}")
        print(f"Evaluating: {model_name}")
        print(f"{'='*60}")
        
        # Encoding
        print(f"Encoding {len(texts)} texts...")
        embeddings, encode_time = self.encode_with_timing(model, texts)
        memory_mb = self.estimate_memory(embeddings)
        throughput = len(texts) / encode_time
        
        print(f"  Encoding time: {encode_time:.2f}s ({throughput:.1f} docs/sec)")
        print(f"  Memory: {memory_mb:.1f} MB")
        
        # Retrieval metrics
        print("Computing retrieval metrics...")
        recall_at_1 = self.recall_at_k(embeddings, retrieval_pairs, 1)
        recall_at_5 = self.recall_at_k(embeddings, retrieval_pairs, 5)
        recall_at_10 = self.recall_at_k(embeddings, retrieval_pairs, 10)
        mrr_score = self.mrr(embeddings, retrieval_pairs)
        ndcg_at_5 = self.ndcg_at_k(embeddings, retrieval_pairs, 5)
        ndcg_at_10 = self.ndcg_at_k(embeddings, retrieval_pairs, 10)
        
        print(f"  Recall@1: {recall_at_1:.3f}")
        print(f"  Recall@5: {recall_at_5:.3f}")
        print(f"  Recall@10: {recall_at_10:.3f}")
        print(f"  MRR: {mrr_score:.3f}")
        print(f"  nDCG@5: {ndcg_at_5:.3f}")
        print(f"  nDCG@10: {ndcg_at_10:.3f}")
        
        # Clustering metrics
        print("Computing clustering metrics...")
        cluster_metrics = self.evaluate_clustering(embeddings, n_clusters, labels)
        print(f"  Silhouette: {cluster_metrics.get('silhouette', 0):.3f}")
        print(f"  Davies-Bouldin: {cluster_metrics.get('davies_bouldin', float('inf')):.3f}")
        print(f"  Calinski-Harabasz: {cluster_metrics.get('calinski_harabasz', 0):.1f}")
        
        # Robustness
        noise_drop = 0.0
        if evaluate_robustness:
            print("Evaluating robustness...")
            noise_drop = self.evaluate_robustness(model, texts, embeddings, retrieval_pairs)
            print(f"  Noise robustness (Recall drop %): {noise_drop:.1f}%")
        
        return EvaluationMetrics(
            model_name=model_name,
            recall_at_1=recall_at_1,
            recall_at_5=recall_at_5,
            recall_at_10=recall_at_10,
            mrr=mrr_score,
            ndcg_at_5=ndcg_at_5,
            ndcg_at_10=ndcg_at_10,
            silhouette=cluster_metrics.get('silhouette', 0),
            davies_bouldin=cluster_metrics.get('davies_bouldin', float('inf')),
            calinski_harabasz=cluster_metrics.get('calinski_harabasz', 0),
            encode_time_ms=encode_time * 1000,
            memory_mb=memory_mb,
            throughput_docs_per_sec=throughput,
            noise_robustness_drop=noise_drop
        )
    
    def compare_results(self, results: List[EvaluationMetrics]) -> pd.DataFrame:
        """Create comparison dataframe."""
        data = []
        for r in results:
            data.append({
                'Model': r.model_name,
                'Recall@1': f"{r.recall_at_1:.3f}",
                'Recall@5': f"{r.recall_at_5:.3f}",
                'Recall@10': f"{r.recall_at_10:.3f}",
                'MRR': f"{r.mrr:.3f}",
                'nDCG@5': f"{r.ndcg_at_5:.3f}",
                'nDCG@10': f"{r.ndcg_at_10:.3f}",
                'Silhouette': f"{r.silhouette:.3f}",
                'Davies-Bouldin': f"{r.davies_bouldin:.3f}",
                'Calinski-Harabasz': f"{r.calinski_harabasz:.1f}",
                'Encode (ms)': f"{r.encode_time_ms:.1f}",
                'Memory (MB)': f"{r.memory_mb:.1f}",
                'Throughput': f"{r.throughput_docs_per_sec:.0f}",
                'Noise Drop %': f"{r.noise_robustness_drop:.1f}",
            })
        
        return pd.DataFrame(data)