# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional, Any, Tuple

from graphrag_toolkit.lexical_graph.retrieval.post_processors.reranker_mixin import RerankerMixin
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_top_free_gpus
from graphrag_toolkit.core.postprocessor import PostProcessor
from graphrag_toolkit.core.types import NodeWithScore, QueryBundle

logger = logging.getLogger(__name__)

try:
    import torch
except ImportError as e:
    raise ImportError(
            "torch package not found, install with 'pip install torch'"
        ) from e

class BGEReranker(PostProcessor, RerankerMixin):
    """BGEReranker class for re-ranking nodes or sentence pairs based on a model.

    This class utilizes a pre-trained re-ranker model to re-rank sentence pairs or nodes
    with scores. It uses GPU for computations if available and is designed to work with
    the LayerWiseFlagLLMReranker model from the FlagEmbedding library.

    Attributes:
        model_name (str): Name of the pre-trained re-ranker model to use.
        gpu_id (Optional[int]): ID of the GPU to use for computations. If None and GPUs
            are available, the first free GPU will be used.
        reranker (Any): The re-ranker object initialized with the specified model.
        device (Any): The torch device to be used for computations, either CPU or GPU.
        batch_size_internal (int): Batch size used for processing sentence pairs
            or nodes.
    """

    def __init__(
        self, 
        model_name: str = 'BAAI/bge-reranker-v2-minicpm-layerwise',
        gpu_id: Optional[int] = None,
        batch_size: int = 128
    ):
        """
        Initializes the __init__ function for configuring and setting up the reranker
        model. This includes loading the necessary dependencies, setting GPU device,
        and initializing key model parameters. If the required dependencies are not
        installed or GPU is unavailable, appropriate errors are raised to handle those
        issues.

        Args:
            model_name (str): The name of the model to be loaded. Defaults to
                'BAAI/bge-reranker-v2-minicpm-layerwise'.
            gpu_id (Optional[int]): The ID of the GPU to be utilized. If None, assigns
                the first available free GPU.
            batch_size (int): The batch size for processing inputs. Defaults to 128.

        Raises:
            ImportError: If the FlagEmbedding package is not installed.
            Exception: If no compatible GPU is available or any error occurs during
                initialization of the reranker model.
        """
        super().__init__()
        try:
            from FlagEmbedding import LayerWiseFlagLLMReranker
        except ImportError as e:
            raise ImportError(
                "FlagEmbedding package not found, install with 'pip install FlagEmbedding'"
            ) from e
        self.model_name = model_name
        self.batch_size_internal = batch_size
        self.gpu_id = gpu_id

        try:
            if torch.cuda.is_available() and self.gpu_id is not None:
                self.device = torch.device(f'cuda:{self.gpu_id}')
            elif torch.cuda.is_available():
                self.gpu_id = get_top_free_gpus(n=1)[0]
                self.device = torch.device(f'cuda:{self.gpu_id}')
        except Exception:
            raise("BGEReranker requires a GPU")
        
        torch.cuda.set_device(self.device)
        torch.cuda.empty_cache()
        try:
            self.reranker = LayerWiseFlagLLMReranker(
                model_name,
                use_fp16=True,
                devices=self.gpu_id,
                cutoff_layers=[28]
            )
        except Exception as e:
            logger.error(f"Failed to initialize reranker: {str(e)}")
            raise
    
    @property
    def batch_size(self):
        """
        Gets the batch size for the internal configuration.

        This property retrieves the value of the batch size from the internal
        state of the object. It is commonly used to access the configured
        batch size for operations requiring batching functionality.

        Returns:
            int: The size of the batch currently configured internally.
        """
        return self.batch_size_internal
    
    def rerank_pairs(
        self,
        pairs: List[Tuple[str, str]],
        batch_size: int = 128
    ) -> List[float]:
        """
        Re-ranks a list of sentence pairs based on a pre-trained reranker model and
        returns the computed scores. This method utilizes a single GPU for computing
        the scores and is optimized for batch processing.

        Args:
            pairs (List[Tuple[str, str]]): A list of sentence pairs to be re-ranked.
                Each pair includes two sentences as strings.
            batch_size (int): The size of the batch for processing sentence pairs.
                Defaults to 128.

        Returns:
            List[float]: A list of scores corresponding to the input sentence pairs,
                where higher scores indicate higher relevance or similarity.

        Raises:
            Exception: If an error occurs during the re-ranking process, a detailed
                exception is logged and re-raised.
        """
        try:
            with torch.cuda.device(self.device):
                scores = self.reranker.compute_score_single_gpu(
                    sentence_pairs=pairs,
                    batch_size=batch_size,
                    cutoff_layers=[28]
                )
                return scores
        except Exception as e:
            logger.error(f"Error in rerank_pairs: {str(e)}")
            raise

    def process(
        self,
        nodes: list[NodeWithScore],
        query: QueryBundle,
    ) -> list[NodeWithScore]:
        """
        Postprocesses a list of nodes by reranking them based on a query using a scoring mechanism.

        This method takes a list of nodes and a query bundle, calculates a relevance
        score for each node based on the provided query, and sorts the nodes based on the
        calculated scores. If the reranking process fails, it logs the error and returns
        the original list of nodes. Additionally, the method manages CUDA memory cache if a
        GPU is available.

        Args:
            nodes (list[NodeWithScore]): A list of nodes with initial scores to be processed.
            query (QueryBundle): The query bundle containing the query
                string used for relevance scoring.

        Returns:
            list[NodeWithScore]: A list of nodes reranked by their calculated relevance scores.
            If reranking fails, the returned list will be the same as the input nodes.
        """
        if not nodes:
            return nodes
            
        try:
            pairs = [(query.query_str, node.node.text) for node in nodes]

            scores = self.rerank_pairs(pairs, self.batch_size_internal)
            
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            
            scored_nodes = [
                NodeWithScore(
                    node=node.node,
                    score=float(score) if isinstance(score, torch.Tensor) else score
                )
                for node, score in zip(nodes, scores)
            ]
            
            scored_nodes.sort(key=lambda x: x.score or 0.0, reverse=True)
            return scored_nodes
            
        except Exception as e:
            logger.error(f"BGE reranking failed: {str(e)}. Returning original nodes.")
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            return nodes

    def __del__(self):
        """Cleanup when the object is deleted."""
        if torch.cuda.is_available():
            try:
                torch.cuda.empty_cache()
            except:
                pass