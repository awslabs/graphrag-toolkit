# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Tuple, Optional, Any

from graphrag_toolkit.lexical_graph.retrieval.post_processors import RerankerMixin

from llama_index.core.bridge.pydantic import Field
from llama_index.core.postprocessor import SentenceTransformerRerank

logger = logging.getLogger(__name__)

class SentenceReranker(SentenceTransformerRerank, RerankerMixin):

    batch_size_internal: int = Field(default=128)

    def __init__(
        self,
        top_n: int = 2,
        model: str = "cross-encoder/stsb-distilroberta-base",
        device: Optional[str] = None,
        keep_retrieval_score: Optional[bool] = False,
        batch_size:Optional[int]=128,
        **kwargs:Any
    ):
        try:
            import sentence_transformers
            import torch
        except ImportError as e:
            raise ImportError(
                "torch and/or sentence_transformers packages not found, install with 'pip install torch sentence_transformers'"
            ) from e
        
        super().__init__(
            top_n=top_n,
            model=model,
            device=device,
            keep_retrieval_score=keep_retrieval_score, 
        )
        
        self.batch_size_internal=batch_size

    @property
    def batch_size(self):
        return self.batch_size_internal
    
    def rerank_pairs(
        self,
        pairs: List[Tuple[str, str]],
        batch_size: int = 128
    ) -> List[float]:
        return self._model.predict(sentences=pairs, batch_size=batch_size, show_progress_bar=False)

