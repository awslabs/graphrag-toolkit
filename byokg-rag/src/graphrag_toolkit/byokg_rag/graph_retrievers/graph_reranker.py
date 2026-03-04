from abc import ABC, abstractmethod
import numpy as np
import torch

class GReranker(ABC):
    """
    Abstract base class for GraphRAG reranker.
    """
    
    def __init__(self):
        """
        Initialize the graph reranker.
        """

    @abstractmethod
    def rerank_input_with_query(self, query, input, topk=None):
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
    def __init__(self, model_name="BAAI/bge-reranker-base", topk=10, device="cuda"):
        """
        Initialize the LocalGReranker.

        Args:
            model_name: Name of the reranker model to use. Must be one of:
                "BAAI/bge-reranker-base", "BAAI/bge-reranker-large", "BAAI/bge-reranker-v2-m3"
            topk: Default number of top results to return
            device: Device to run the model on (e.g., "cuda" or "cpu")

        Raises:
            AssertionError: If model_name is not supported
        """
        assert model_name in ["BAAI/bge-reranker-base", "BAAI/bge-reranker-large", "BAAI/bge-reranker-v2-m3"], "Model name not supported"
        self.model_name = model_name
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.reranker = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.reranker = self.reranker.to(device)
        self.reranker.eval()
        
        self.topk = topk


    def calculate_score(self, pairs):
        """
        Calculate the score for the given pairs (query, text).

        Args:
            pairs: List of [query, text] pairs to score

        Returns:
            torch.Tensor: Scores for each pair
        """
        if self.model_name in ["BAAI/bge-reranker-base", "BAAI/bge-reranker-large", "BAAI/bge-reranker-v2-m3"]:
            with torch.no_grad():
                inputs = self.tokenizer(pairs, padding=True, truncation=True, return_tensors='pt', max_length=512)
                inputs = inputs.to(self.reranker.device)
                scores = self.reranker(**inputs, return_dict=True).logits.view(-1, ).float()
                return scores
        else:
            raise NotImplementedError
    
    def filter_topk(self, query, input, topk=10, return_scores=False):
        """
        Filter the top-k input based on the reranker score.

        Args:
            query: The query string or list of queries
            input: List of input items to rerank
            topk: Number of top results to return
            return_scores: Whether to return scores along with results

        Returns:
            If return_scores is True:
                tuple: (filtered list, scores, indices)
            If return_scores is False:
                tuple: (filtered list, indices)
        """
        if isinstance(query, str):
            pairs = [[query, x] for x in input]
        else:
            pairs = [[x,y] for x,y in zip(query, input)]
        score = self.calculate_score(pairs)
        # convert to CPU
        score = score.cpu()
        np_score = -np.array(score)
        ids = np.argsort(np_score, kind="stable")

        if return_scores:
            return [input[x] for x in ids[:topk]], [score[x] for x in ids[:topk]], ids[:topk]
        else:
            return [input[x] for x in ids[:topk]], ids[:topk]

    def rerank_input_with_query(self, query, input, topk=None, return_scores=False):
        """
        Rerank the given input based on the query.

        Args:
            query: The query string
            input: List of input to be reranked
            topk: Number of top results to return. If None, uses self.topk
            return_scores: Whether to return scores along with results

        Returns:
            If return_scores is True:
                tuple: (reranked list, scores, indices)
            If return_scores is False:
                tuple: (reranked list, indices)
        """
        if not topk:
            topk = self.topk
        return self.filter_topk(query, input, topk=topk, return_scores=return_scores)
