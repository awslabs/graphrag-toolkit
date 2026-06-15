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

class SemanticGuidedBaseRetriever(Retriever):
    """
    Base class for semantic-guided retrievers.

    This class serves as a blueprint for implementing retrievers that leverage
    a combination of vector and graph stores for semantic-guided retrieval. It
    facilitates structured retrieval processes and allows for filtering configurations.
    The class is designed to be extended by implementing the `retrieve` method.

    Attributes:
        graph_store (GraphStore): The graph store used for managing and accessing
            graph-based data relevant to the retrieval process.
        vector_store (VectorStore): The vector store used for managing and querying
            vectorized data for semantic search.
        filter_config (FilterConfig): Configuration settings used for applying filters
            during the retrieval process. A default configuration is applied if not provided.
        debug_results (bool): Indicates whether to enable debugging for retrieved
            results. Debug mode is enabled when `debug_results` is explicitly specified
            in the keyword arguments.
    """
    def __init__(self, 
                vector_store:VectorStore,
                graph_store:GraphStore,
                filter_config:Optional[FilterConfig]=None,
                **kwargs):
        """
        Initializes an instance of the class, configuring the necessary storage interfaces
        and optional filtering capabilities. Allows additional keyword arguments for
        extended configurability.

        Args:
            vector_store: An instance of VectorStore for managing and querying vector
                data.
            graph_store: An instance of GraphStore utilized for maintaining and
                querying graph-related information.
            filter_config: Optional. An instance of FilterConfig to specify filtering
                rules or criteria. Defaults to a new instance of FilterConfig if not
                provided.
            **kwargs: Additional keyword arguments allowing further customization.
                For example, 'debug_results' to enable debugging-related settings.
        """
        self.graph_store = graph_store
        self.vector_store = vector_store
        self.filter_config = filter_config or FilterConfig()
        self.debug_results = kwargs.pop('debug_results', None) is not None

        if isinstance(self.vector_store.get_index('statement'), DummyVectorIndex):
            logger.warning("'statement' vector index is a DummyVectorIndex")

    @abstractmethod
    def retrieve(self, query_bundle: QueryBundle) -> list[NodeWithScore]:
        """
        Retrieves a list of nodes with associated scores based on the provided query bundle.

        This is an abstract method that must be implemented by subclasses. It defines the
        core logic for retrieving relevant nodes that match the criteria specified in the
        query bundle. The specific implementation details of the retrieval should take into
        account how the nodes are stored and how relevance is determined.

        Args:
            query_bundle: An instance of QueryBundle, which encapsulates the parameters
                and data needed to perform the retrieval operation. This may include
                query text, filters, or any other necessary metadata.

        Returns:
            List[NodeWithScore]: A list of NodeWithScore objects, where each object
                represents a node and its associated relevance score to the given query.
                The nodes should be ordered based on their relevance.

        Raises:
            NotImplementedError: This method must be implemented by any subclass. A call
                to the method in the base class will raise this exception.
        """
        raise NotImplementedError()
