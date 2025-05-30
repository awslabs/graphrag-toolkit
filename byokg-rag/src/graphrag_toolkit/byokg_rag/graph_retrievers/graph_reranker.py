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
    def __init__(self, model_name="bge-reranker-base", topk=10, device="cuda"):
        assert model_name in ["bge-reranker-base", "bge-reranker-large", "bge-reranker-v2-m3"], "Model name not supported"
        self.model_name = model_name
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained('BAAI/bge-reranker-v2-m3')
        self.reranker = AutoModelForSequenceClassification.from_pretrained('BAAI/bge-reranker-v2-m3')
        self.reranker = self.reranker.to(device)
        self.reranker.eval()
        
        self.topk = topk


    def calculate_score(self, pairs):
        """
        Calculate the score for the given pairs (query, text)
        """
        if self.model_name in ["bge-reranker-base", "bge-reranker-large", "bge-reranker-v2-m3"]:
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
            query (str): The query string.
            input (list): List of input to be reranked.

        Returns:
            list: Reranked list of input.
        """
        if not topk:
            topk = self.topk
        return self.filter_topk(query, input, topk=topk, return_scores=return_scores)
