# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import abc
import queue

from typing import Sequence, Any, List, Dict, Optional
from llama_index.core.schema import QueryBundle, BaseNode
from llama_index.core.bridge.pydantic import BaseModel, Field, field_validator
from llama_index.core.vector_stores.types import MetadataFilters

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph import EmbeddingType, TenantId
from graphrag_toolkit.lexical_graph.storage.constants import ALL_EMBEDDING_INDEXES


logger = logging.getLogger(__name__)

def to_embedded_query(query_bundle:QueryBundle, embed_model:EmbeddingType) -> QueryBundle:
    """
    Converts a query bundle into an embedded query if not already embedded.

    This function takes a query bundle and an embedding model as input. It checks
    if the query bundle already contains an embedding. If the embedding is missing,
    it computes the embedding using the provided embedding model and updates the
    query bundle accordingly. Finally, it returns the embedded query bundle.

    Args:
        query_bundle (QueryBundle): The query bundle to be converted into an
            embedded query. It may or may not already contain an embedding.
        embed_model (EmbeddingType): The embedding model used to compute the
            embeddings for the given query bundle.

    Returns:
        QueryBundle: The input query bundle with the embedding attached. If the
            embedding already existed, the original query bundle is returned.
    """
    if query_bundle.embedding:
        return query_bundle
    
    query_bundle.embedding = (
        embed_model.get_agg_embedding_from_queries(
            query_bundle.embedding_strs
        )
    ) 
    return query_bundle   

class VectorIndex(BaseModel):
    """Represents a vector-based index for storing and retrieving embeddings.

    This class serves as a base model for managing vector-based indexes used for
    storing and querying embeddings. It provides a framework for ensuring the correctness
    of the index name, managing tenant-specific index names, and defining abstract
    methods for functionality such as adding embeddings, retrieving the top-k results,
    and accessing embeddings by IDs. Intended to be subclassed to implement specific
    indexing behavior.

    Attributes:
        index_name (str): The name of the index to be used.
        tenant_id (TenantId): Specifies the tenant information, with a default value
        generated using the TenantId default factory.
        writeable (bool): A flag indicating if the index is in writable mode. Defaults to True.
    """
    index_name: str
    tenant_id:TenantId = Field(default_factory=lambda: TenantId())
    writeable:bool = True

    @field_validator('index_name')
    def validate_option(cls, v):
        """
        Validates the 'index_name' field to ensure it matches one of the allowed values
        defined in 'ALL_EMBEDDING_INDEXES'. This validation method is used to guarantee
        that the input for 'index_name' is appropriate for its intended use. If the value
        is invalid, an exception is raised to prevent incorrect configurations.

        Args:
            v: The value of the 'index_name' field to be validated.

        Returns:
            The validated value of 'index_name' if it is valid.

        Raises:
            ValueError: If 'v' is not one of the predefined options in 'ALL_EMBEDDING_INDEXES'.
        """
        if v not in ALL_EMBEDDING_INDEXES:
            raise ValueError(f'Invalid index_name: must be one of {ALL_EMBEDDING_INDEXES}')
        return v
    
    def underlying_index_name(self) -> str:
        """
        Determines the underlying index name based on the tenant configuration.

        This method evaluates whether the current tenant is the default tenant. If the
        tenant is default, it directly returns the stored index name. Otherwise, it
        formats the index name using the tenant's custom format function.

        Returns:
            str: The underlying index name, either as is for the default tenant or
            formatted for a custom tenant.
        """
        if self.tenant_id.is_default_tenant():
            return self.index_name
        else:
            return self.tenant_id.format_index_name(self.index_name)
    
    @abc.abstractmethod
    def add_embeddings(self, nodes:Sequence[BaseNode]) -> Sequence[BaseNode]:
        """
        Provides an interface for implementing the method to add embeddings to
        a sequence of nodes.

        Args:
            nodes: A sequence of BaseNode instances to which embeddings will
                be added.

        Returns:
            Sequence[BaseNode]: A sequence of BaseNode instances with added
                embeddings.

        Raises:
            NotImplementedError: If the method is not implemented in a derived
                class.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def top_k(self, query_bundle:QueryBundle, top_k:int=5, filter_config:Optional[FilterConfig]=None) -> Sequence[Dict[str, Any]]:
        """
        Abstract method to retrieve the top-k relevant items based on a query. Implementing
        classes are expected to define the logic for fetching the most relevant results
        given a query bundle, a specified number of top results, and an optional filter
        configuration. This method should return a sequence of dictionaries where each
        dictionary represents a matched item.

        Args:
            query_bundle: Encapsulates all necessary query-related data used to fetch the
            relevant items.
            top_k: The maximum number of top relevant items to return.
            filter_config: An optional filter configuration used to apply additional
            constraints or parameters for refining the results.

        Returns:
            Sequence[Dict[str, Any]]: A sequence of dictionaries. Each dictionary holds
            relevant data for the matched item based on the query and optional
            filtering.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError


    @abc.abstractmethod
    def get_embeddings(self, ids:List[str]=[]) -> Sequence[Dict[str, Any]]:
        """
        Abstract method for retrieving embeddings associated with a given list of IDs.

        This method must be implemented in subclasses, and its implementation should
        return embeddings for the provided list of IDs. The embeddings are expected to
        be a sequence of dictionaries, where each dictionary represents the embedding
        data for an ID.

        Args:
            ids (List[str], optional): A list of string identifiers for which
            embeddings are to be retrieved. Defaults to an empty list.

        Returns:
            Sequence[Dict[str, Any]]: A sequence of dictionaries representing
            the embeddings corresponding to the provided IDs.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def update_versioning(self, versioning_timestamp:int, ids:List[str]=[]) -> List[str]:
        raise NotImplementedError
    
    @abc.abstractmethod
    def enable_for_versioning(self, ids:List[str]=[]) -> List[str]:
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_embeddings(self, ids:List[str]=[]):
        raise NotImplementedError
