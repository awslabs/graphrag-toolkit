# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Tuple, Optional, Any

from graphrag_toolkit.lexical_graph.retrieval.post_processors import RerankerMixin
from graphrag_toolkit.lexical_graph.utils.reranker_utils import to_float

from graphrag_toolkit.core.postprocessor import PostProcessor
from graphrag_toolkit.core.types import NodeWithScore, QueryBundle

logger = logging.getLogger(__name__)

class SentenceReranker(PostProcessor, RerankerMixin):
    """
    Represents a specialized sentence reranker that combines functionalities from
    PostProcessor and RerankerMixin.

    This class is designed to rerank sentence pairs using a pre-trained cross-encoder
    model. Users can specify parameters such as the top N results to rerank, the
    underlying model to use, and batch size. The class initializes with support
    for GPU execution if available and relies on external libraries such as
    `sentence_transformers` and `torch`.

    Attributes:
        batch_size_internal (int): Internal batch size used during reranking.
    """
    
    def __init__(
        self,
        top_n: int = 2,
        model: str = "cross-encoder/stsb-distilroberta-base",
        device: Optional[str] = None,
        keep_retrieval_score: Optional[bool] = False,
        batch_size:Optional[int]=128,
        **kwargs:Any
    ):
        """
        Initializes the class with configuration for a model execution environment.

        Args:
            top_n (int): The number of top results to retrieve. Default is 2.
            model (str): The name or identifier of the model to be used.
                Default is "cross-encoder/stsb-distilroberta-base".
            device (Optional[str]): Specifies the device to execute the model.
                For example, 'cpu' or 'cuda'. Default is None.
            keep_retrieval_score (Optional[bool]): Determines whether to retain
                the retrieval score after execution. Default is False.
            batch_size (Optional[int]): Defines the size of batches to be processed
                during execution. Default is 128.
            **kwargs (Any): Additional keyword arguments for further customization
                of the model initialization.

        Raises:
            ImportError: If the required 'torch' and/or 'sentence_transformers'
                packages are not installed.
        """
        try:
            from sentence_transformers import CrossEncoder
            import torch
        except ImportError as e:
            raise ImportError(
                "torch and/or sentence_transformers packages not found, install with 'pip install torch sentence_transformers'"
            ) from e
        
        super().__init__()
        self.top_n = top_n
        self.keep_retrieval_score = keep_retrieval_score
        self.batch_size_internal = batch_size
        
        self._model = CrossEncoder(model, device=device)

    @property
    def batch_size(self):
        """
        Returns the internal batch size value.

        Returns:
            int: The batch size value stored in the internal variable.
        """
        return self.batch_size_internal
    
    def rerank_pairs(
        self,
        pairs: List[Tuple[str, str]],
        batch_size: int = 128
    ) -> List[float]:
        """
        Re-ranks pairs of sentences based on their similarity or relevance by passing them
        through the model for prediction. It processes the pairs in batches for efficiency.

        Args:
            pairs (List[Tuple[str, str]]): A list of sentence pairs, where each pair consists
                of two strings to be compared.
            batch_size (int): The number of sentence pairs to process in a single batch.
                Defaults to 128.

        Returns:
            List[float]: A list of prediction scores for each pair, indicating their similarity
                or relevance.
        """
        return [
            to_float(r) 
            for r in self._model.predict(sentences=pairs, batch_size=batch_size, show_progress_bar=False)
        ] 
    
    def process(
        self,
        nodes: list[NodeWithScore],
        query: QueryBundle,
    ) -> list[NodeWithScore]:
        """
        Reranks nodes using the cross-encoder model and returns the top_n results.

        Args:
            nodes: A list of NodeWithScore objects to rerank.
            query: The query bundle for scoring relevance.

        Returns:
            A list of the top_n NodeWithScore objects sorted by reranking score.
        """
        if not nodes or not query.query_str:
            return nodes

        pairs = [(query.query_str, node.node.text) for node in nodes]
        scores = self.rerank_pairs(pairs, self.batch_size_internal)

        scored_nodes = []
        for node, score in zip(nodes, scores):
            new_node = NodeWithScore(node=node.node, score=to_float(score))
            if self.keep_retrieval_score:
                new_node.node.metadata['retrieval_score'] = node.score
            scored_nodes.append(new_node)

        scored_nodes.sort(key=lambda x: x.score or 0.0, reverse=True)
        return scored_nodes[:self.top_n]
