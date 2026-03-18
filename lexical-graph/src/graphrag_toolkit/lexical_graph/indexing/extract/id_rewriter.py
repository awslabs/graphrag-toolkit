# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from typing import Any, List, Sequence, Optional, Iterable

from graphrag_toolkit.lexical_graph.versioning import VERSIONING_METADATA_KEYS
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import DoNotCheckpoint
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument

from llama_index.core.schema import BaseNode, Document
from llama_index.core.node_parser import NodeParser
from llama_index.core.schema import NodeRelationship

class IdRewriter(NodeParser, DoNotCheckpoint):
    """
    Rewrites and assigns new IDs to nodes and processes source documents.

    The IdRewriter class is a specialized implementation that processes nodes and generates new IDs
    using an `IdGenerator`. It is typically used to standardize and rewrite IDs of nodes and metadata
    within a structured document processing pipeline. It can also handle source documents by parsing
    and modifying their nodes, ensuring consistency in ID generation and relationships among nodes.
    The class relies on optional inner parsers to further process nodes, in addition to its ID rewriting functionality.

    Attributes:
        inner (Optional[NodeParser]): An optional inner parser used to process nodes after ID rewriting.
        id_generator (IdGenerator): An object used to generate source and chunk IDs for nodes.
    """
    inner:Optional[NodeParser]=None
    id_generator:IdGenerator
    
    def _get_properties_str(self, properties, default):
        """
        Generates a string representation of properties or returns a default value.

        This function processes a dictionary of properties, formats them as "key:value"
        pairs sorted alphabetically by key, and combines them into a single string using
        a semicolon as the separator. If no properties are provided, it returns the
        supplied default value.

        Args:
            properties (dict): A dictionary where keys and values represent the
                properties to be formatted into a string.
            default (str): The default string to return if no properties are supplied.

        Returns:
            str: A semicolon-separated string of formatted "key:value" pairs if
            properties are supplied; otherwise, the default string.

        """

        if properties:
            return ';'.join(sorted([f'{k}:{v}' for k,v in properties.items() if k not in VERSIONING_METADATA_KEYS]))
        else:
            return default
    
    def _new_doc_id(self, node):
        """
        Generates a new document ID based on node metadata and text content.

        This method processes the metadata and text of a given node to create a
        unique document ID using the `id_generator` attribute. It formats the
        metadata, combines it with the text data, and passes it to the ID generator
        to produce a deterministic and consistent ID.

        Args:
            node: The node object containing the `text`, `metadata`, and `doc_id`
                properties necessary for ID generation.

        Returns:
            str: A newly generated document ID.
        """
        metadata_str = self._get_properties_str(node.metadata, '')  
        return self.id_generator.create_source_id(str(node.text), metadata_str)     
        
    def _new_node_id(self, node):
        """
        Generates a new node identifier based on the node's source information, text content,
        and metadata.

        This function retrieves the source information associated with the node and generates
        a unique identifier if no source is available. The metadata and text of the node are then
        processed to create a unique chunk identifier for the node.

        Args:
            node (Node): The node for which an identifier is to be generated. This node should
                contain relationships, metadata, and text attributes.

        Returns:
            str: A new identifier uniquely representing the node.

        """
        source_info = node.relationships.get(NodeRelationship.SOURCE, None)
        source_id = source_info.node_id if source_info else f'aws:{uuid.uuid4().hex}' 
        metadata_str = self._get_properties_str(node.metadata, '') 

        return self.id_generator.create_chunk_id(source_id, str(node.text), metadata_str)
        
        
    def _new_id(self, node):
        """
        Generates a new identifier for a given node based on its type and properties.

        If the node's `id_` starts with the prefix 'aws:', the function returns the
        existing `id_` unchanged. For instances of the `Document` type, a unique
        identifier is generated using `_new_doc_id`. Otherwise, for all other node types,
        the identifier is generated using `_new_node_id`.

        Args:
            node: The input object for which the identifier is generated. It can be of
                different types, including `Document` or other node-like objects.

        Returns:
            The generated or existing identifier of the node as a string based on its
            type and specific attributes.
        """
        if node.id_.startswith('aws:'):
            return node.id_
        elif isinstance(node, Document):
            return self._new_doc_id(node)
        else:
            return self._new_node_id(node)
    
    def _parse_nodes(
        self,
        nodes: Sequence[BaseNode],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> List[BaseNode]:
        """
        Parses a collection of nodes to update their IDs and relationships, and supports an optional
        inner transformation logic.

        This method assigns new IDs to the provided nodes, maintains mappings between the
        old and new IDs, applies any inner processing logic if defined, and updates node
        relationships with the new IDs if necessary. The modified nodes are then returned.

        Args:
            nodes (Sequence[BaseNode]): A sequence of nodes to be processed. Each node must
                be an instance of BaseNode.
            show_progress (bool): A flag to indicate whether to show progress of the processing.
                Defaults to False.
            **kwargs (Any): Additional keyword arguments passed to the inner processing logic,
                if defined.

        Returns:
            List[BaseNode]: A list of processed nodes, where each node has updated IDs and
            relationships reflecting the transformation applied.
        """
        id_mappings = {}
        
        for n in nodes:
            n.id_ = self._new_id(n)
            id_mappings[n.id_] = n.id_
                      
        if not self.inner:
            return nodes
            
        results = self.inner(nodes, **kwargs)
        
        for n in results:
            id_mappings[n.id_] = self._new_id(n)
        
        def update_ids(n):
            n.id_ = id_mappings[n.id_]
            for r in n.relationships.values():
                r.node_id = id_mappings.get(r.node_id, r.node_id)
            return n
            
        return [
            update_ids(n) 
            for n in results
        ]
    
    def handle_source_docs(self, source_documents:Iterable[SourceDocument]) -> List[SourceDocument]:
        for source_document in source_documents:
            if source_document.refNode:
                source_document.refNode = self._parse_nodes([source_document.refNode])[0]
            source_document.nodes = self._parse_nodes(source_document.nodes)
        return source_documents
    