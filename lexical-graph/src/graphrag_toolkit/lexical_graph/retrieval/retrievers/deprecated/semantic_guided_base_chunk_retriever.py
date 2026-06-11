# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import abstractmethod
from typing import List, Optional

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndex

from graphrag_toolkit.core.retriever import Retriever
from graphrag_toolkit.core.types import NodeWithScore, QueryBundle

logger = logging.getLogger(__name__)

class SemanticGuidedBaseChunkRetriever(Retriever):

    def __init__(self, 
                vector_store:VectorStore,
                graph_store:GraphStore,
                filter_config:Optional[FilterConfig]=None,
                **kwargs):

        self.graph_store = graph_store
        self.vector_store = vector_store
        self.filter_config = filter_config or FilterConfig()
        self.debug_results = kwargs.pop('debug_results', None) is not None

        if isinstance(self.vector_store.get_index('chunk'), DummyVectorIndex):
            logger.warning("'chunk' vector index is a DummyVectorIndex")

    @abstractmethod
    def retrieve(self, query_bundle: QueryBundle) -> list[NodeWithScore]:
        raise NotImplementedError()
