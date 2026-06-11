# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import yaml
import json
from tqdm import tqdm
from typing import List, Any, Union, Dict

from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.build.vector_batch_client import VectorBatchClient
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY, ALL_EMBEDDING_INDEXES, DEFAULT_EMBEDDING_INDEXES
from graphrag_toolkit.lexical_graph.metadata import format_datetime, is_datetime_key
from graphrag_toolkit.core.compat import BaseNode, TextNode
from graphrag_toolkit.core.types import Document


logger = logging.getLogger(__name__)

VectorStoreInfoType = Union[str, VectorStore]

class VectorIndexing(NodeHandler):
    """
    Handles vector indexing operations, including interacting with vector stores and managing batch operations.

    This class provides tools to index vectors from nodes, allowing for integration with vector stores. It facilitates
    operations such as batch writes and handling embedding indexing, making it easier to process and store large amounts
    of vector data. The class supports flexible configuration and provides mechanisms to manage and optimize vector
    processing using external vector stores.

    Attributes:
        vector_store (VectorStore): The vector store instance used for vector indexing and operations.
    """
    @staticmethod
    def for_vector_store(vector_store_info:VectorStoreInfoType=None, index_names=DEFAULT_EMBEDDING_INDEXES, **kwargs):
        """
        Creates a VectorIndexing instance for the given vector store configuration.

        This method is used to create a VectorIndexing object that wraps around
        a vector store. The vector store can be directly passed as an instance
        of VectorStore or indirectly specified using configuration parameters
        through the factory method.

        Args:
            vector_store_info: Optional; Configuration or instance of the vector
                store. If it is an instance of VectorStore, it will be directly
                used. Otherwise, it should represent configuration parameters
                required to create a vector store using the factory.
            index_names: List of default index names that will be utilized for
                embedding lookups. If not specified, defaults to
                DEFAULT_EMBEDDING_INDEXES.
            **kwargs: Additional keyword arguments to configure the vector store
                when created indirectly through the factory.

        Returns:
            VectorIndexing: An instance of VectorIndexing configured with the
            specified vector store.

        Raises:
            TypeError: If the provided arguments are not compatible with the
            vector store configuration or creation requirements.

        """
        if isinstance(vector_store_info, VectorStore):
            return VectorIndexing(vector_store=vector_store_info)
        else:
            return VectorIndexing(vector_store=VectorStoreFactory.for_vector_store(vector_store_info, index_names, **kwargs))
    
    vector_store:VectorStore

    def _json_to_yaml(self, s:str) -> str:
        try:
            j = json.loads(s)
            return yaml.dump(j, sort_keys=False)
        except Exception:
            return None

    def _to_indexable_metadata(self, metadata:Dict[str, Any]):
        source = metadata.get('source', None)
        source_metadata = None
        if source:
            source_metadata = source.get('metadata', None)
            if source_metadata:
                keys_to_delete = []
                for k, v in source_metadata.items():
                    if is_datetime_key(k):
                        if v:
                            try:
                                formatted_date = format_datetime(v)
                                if formatted_date:
                                    source_metadata[k] = formatted_date 
                                else:
                                    keys_to_delete.append(k)
                            except ValueError as e:
                                keys_to_delete.append(k)
                        else:
                            keys_to_delete.append(k)
                for k in keys_to_delete:
                    del source_metadata[k]
        if source_metadata:
            metadata['source']['metadata'] = source_metadata
            
        return metadata         

    def _to_indexable_node(self, node:BaseNode) -> BaseNode:
        indexable_node = node.model_copy()
        indexable_node.metadata = self._to_indexable_metadata(indexable_node.metadata)
        indexable_content = self._json_to_yaml(node.get_content())
        if indexable_content:
            indexable_node.set_content(indexable_content)
        return indexable_node

    def accept(self, nodes: List[BaseNode], **kwargs: Any):
        """
        Processes and yields nodes for vector indexing using a batch client.

        This function processes the given list of nodes to build vector indices.
        It uses a batch client to handle batch operations if batch writes are enabled.
        Nodes can be processed with optional progress display, and vector indexing is
        applied based on metadata. If any error occurs during indexing, it is logged
        and re-raised. Nodes are yielded either individually or in batches based on
        batch operations.

        Args:
            nodes (List[BaseNode]): A list of nodes to be indexed into the vector store.
            **kwargs (Any): Additional configuration options including:
                - batch_writes_enabled (bool): Determines whether batch operations are
                  enabled.
                - batch_write_size (int): Specifies the size of batches for batch
                  operations.

        Yields:
            BaseNode: Nodes after they are processed and indexed, either individually or
            in batches.

        Raises:
            Exception: Re-raises any exceptions that occur during vector indexing with
            logging.
        """
        batch_writes_enabled = kwargs.pop('batch_writes_enabled')
        batch_write_size = kwargs.pop('batch_write_size')

        logger.debug(f'Batch config: [batch_writes_enabled: {batch_writes_enabled}, batch_write_size: {batch_write_size}]')
        logger.debug(f'Vector indexing kwargs: {kwargs}')
        
        with VectorBatchClient(vector_store=self.vector_store, batch_writes_enabled=batch_writes_enabled, batch_write_size=batch_write_size) as batch_client:

            node_iterable = nodes if not self.show_progress else tqdm(nodes, desc=f'Building vector index [batch_writes_enabled: {batch_writes_enabled}, batch_write_size: {batch_write_size}]')

            for node in node_iterable:
                if [key for key in [INDEX_KEY] if key in node.metadata]:
                    try:
                        index_name = node.metadata[INDEX_KEY]['index']
                        if index_name in ALL_EMBEDDING_INDEXES:
                            index = batch_client.get_index(index_name)

                            index.add_embeddings([self._to_indexable_node(node)])
                    except Exception as e:
                        logger.exception('An error occurred while indexing vectors')
                        raise e
                if batch_client.allow_yield(node):
                    yield node

            batch_nodes = batch_client.apply_batch_operations()
            for node in batch_nodes:
                yield node
        