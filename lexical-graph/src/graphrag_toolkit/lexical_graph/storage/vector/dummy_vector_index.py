# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import List, Sequence, Any, Optional

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod

from llama_index.core.schema import QueryBundle
from llama_index.core.vector_stores.types import MetadataFilters


DUMMY = 'dummy://'

logger = logging.getLogger(__name__)

class DummyVectorIndexFactory(VectorIndexFactoryMethod):
    """
    Factory class for creating dummy vector indexes.

    This class is used to instantiate and return dummy vector index objects if specified
    criteria are met. It inherits from the base `VectorIndexFactoryMethod` and
    overrides the `try_create` method to provide a mechanism for conditionally
    creating dummy vector indexes based on input parameters.

    Attributes:
        None
    """
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:
        """
        Tries to create vector indexes based on the provided index names and vector index information.

        This method checks whether the provided vector index information starts with a specific
        prefix (DUMMY). If it does, it creates and returns a list of `DummyVectorIndex` objects
        for the provided index names. If not, it returns `None`.

        Args:
            index_names (List[str]): A list of index names to create vector indexes for.
            vector_index_info (str): A string containing vector index information used to
            determine the type of vector indexes to create.
            **kwargs: Additional keyword arguments to be passed to the creation process.

        Returns:
            List[VectorIndex]: A list of `DummyVectorIndex` objects created based on the input
            index names if `vector_index_info` starts with the prefix DUMMY, otherwise `None`.
        """
        if vector_index_info.startswith(DUMMY):
            logger.debug(f'Opening dummy vector indexes [index_names: {index_names}]')
            return [DummyVectorIndex(index_name=index_name) for index_name in index_names]
        else:
            return None


class DummyVectorIndex(VectorIndex):
    """
    Represents a dummy vector index.

    This class is a placeholder implementation of a vector index. It includes methods
    for adding embeddings, retrieving embeddings for given IDs, and performing a
    top-k query. This implementation is meant for demonstration or testing purposes
    and does not provide meaningful functionality.

    Attributes:
        index_name (str): The name of the index used for logging and identification.
    """
    def add_embeddings(self, nodes):
        """
        Adds embeddings for the given list of nodes.

        This method processes the provided nodes and logs the action of adding
        embeddings for each node by accessing their unique identifiers.

        Args:
            nodes (list): A list of node objects for which embeddings are to be added.
        """ 
        return nodes
    
    def top_k(self, query_bundle:QueryBundle, top_k:int=5, filter_config:Optional[FilterConfig]=None) -> Sequence[Any]:
        """
        Retrieves and returns the top-k results based on the given query and optional filter configuration.

        This method processes the input query bundle and applies the given filtering criteria
        (if provided) to retrieve a specific number of top-ranked elements from the dataset.

        Args:
            query_bundle (QueryBundle): The bundle containing query information including
            the query string and other associated metadata.
            top_k (int, optional): The number of top-ranked results to retrieve. Defaults to 5.
            filter_config (Optional[FilterConfig], optional): An optional filter configuration
            object for applying additional query constraints. Defaults to None.

        Returns:
            Sequence[Any]: A sequence of top-ranked results matching the query and filter criteria.
        """
        logger.debug(f'[{self.index_name}] top k query: {query_bundle.query_str}, top_k: {top_k}, filter_config: {filter_config}')
        return []

    def get_embeddings(self, ids:List[str]=[]) -> Sequence[Any]:
        """
        Gets embeddings for a list of document IDs.

        This function retrieves embeddings associated with the provided document IDs.
        Embeddings represent vectorized representations of data, which might be
        used for similarity search, machine learning, or other analytical tasks.

        Args:
            ids (List[str]): A list of document IDs for which embeddings are to
            be retrieved.

        Returns:
            Sequence[Any]: A sequence of embeddings corresponding to the provided
            document IDs.
        """
        logger.debug(f'[{self.index_name}] get embeddings for ids: {ids}')
        return []
    
    def update_versioning(self, versioning_timestamp:int, ids:List[str]=[]):
        return []
    
    def enable_for_versioning(self, ids:List[str]=[]):
        return []
    
    def delete_embeddings(self, ids:List[str]=[]):
        return []
