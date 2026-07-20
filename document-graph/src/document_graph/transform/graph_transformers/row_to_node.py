# Copyright (c) Evan Erwee. All rights reserved.

"""Row To Node module for document graph operations.

This module provides the RowToNodeTransformer, which converts tabular records
into graph nodes with appropriate metadata. This is useful for building graph
representations from structured data by transforming records into nodes that
can be used in a graph database or other graph-based applications.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class RowToNodeTransformer(TransformerProvider):
    """Transformer provider that converts records to node format with graph metadata.
    
    This transformer transforms tabular records into graph nodes by adding
    necessary graph metadata such as node type and graph element identifier.
    It also ensures each node has an ID and creates a content field from all
    other fields if one doesn't exist.
    
    Args:
        The provider expects the following arguments in the `args` dictionary:
            type (str): The node type to assign to the created nodes.
                       Default is "Row".
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert records to node format with graph metadata.
        
        This method processes a list of records and converts each one to a node
        format by adding graph metadata such as node_type and graph_element.
        It ensures each node has an ID and creates a content field from all
        other fields if one doesn't exist.
        
        Args:
            records (List[Dict[str, Any]]): A list of records to process, where each
                                           record is a dictionary with string keys.
        
        Returns:
            List[Dict[str, Any]]: A new list of records converted to node format.
        """
        node_type = self.args.get("type", "Row")
        
        node_records = []
        for idx, record in enumerate(records):
            node_record = record.copy()
            
            # Add graph metadata
            node_record["node_type"] = node_type
            node_record["graph_element"] = "node"
            
            # Ensure ID exists
            if "id" not in node_record:
                node_record["id"] = f"row_{idx}"
            
            # Create content from all fields if not exists
            if "content" not in node_record:
                content_parts = [f"{k}: {v}" for k, v in record.items() if k != "id"]
                node_record["content"] = ", ".join(content_parts)
            
            node_records.append(node_record)

        return node_records