from abc import ABC, abstractmethod
from FlagEmbedding import FlagReranker
from FlagEmbedding import LayerWiseFlagLLMReranker
import numpy as np

class GReranker(ABC):
    """
    Abstract base class for GraphRAG reranker.
    """
    
    def __init__(self):
        """
        Initialize the graph reranker.
        """

    @abstractmethod
    def rerank_input_with_query(self, query, input, top_k=None):
        """
        Rerank the given input based on the query.

        Args:
            query (str): The query string.
            node text (list): List of node text to be reranked.

        Returns:
            NotImplementedError: If not implemented by child class.
        """
        raise NotImplementedError("Method rerank_input_with_query must be implemented")


class LocalGReranker(GReranker):
    """
        Local reranker on single machine with BGE-reranker-base models.
    """
    def __init__(self, model_name="bge-reranker-base", top_k=10):
        self.load_reranker(model_name)
        self.top_k = top_k

    def load_reranker(self, model_name):
        if model_name == "bge-reranker-v2-minicpm-layerwise":
            self.reranker = LayerWiseFlagLLMReranker(f"BAAI/{model_name}")
        elif model_name.find("bge") != -1:
            self.reranker = FlagReranker(f"BAAI/{model_name}")
        else:
            self.reranker = None

    def calculate_score(self, pairs):
        """
        Calculate the score for the given pairs (query, text)
        """
        if self.reranker:
            return self.reranker.compute_score(pairs)
        else:
            raise NotImplementedError
    
    def filter_topk(self, query, input, top_k=10, return_scores=False):
        """
        Filter the top-k input based on the reranker score.
        """
        if isinstance(query, str):
            pairs = [(query, x) for x in input]
        else:
            pairs = [(x,y) for x,y in zip(query, input)]
        
        score = self.calculate_score(pairs)
        np_score = -np.array(score)
        ids = np.argsort(np_score, kind="stable")

        if return_scores:
            return [input[x] for x in ids[:top_k]], [score[x] for x in ids[:top_k]], ids[:top_k]
        else:
            return [input[x] for x in ids[:top_k]], ids[:top_k]

    def rerank_input_with_query(self, query, input, top_k=None, return_scores=False):
        """
        Rerank the given input based on the query.

        Args:
            query (str): The query string.
            input (list): List of input to be reranked.

        Returns:
            list: Reranked list of input.
        """
        if not top_k:
            top_k = self.top_k
        return self.filter_topk(query, input, top_k=top_k, return_scores=return_scores)
