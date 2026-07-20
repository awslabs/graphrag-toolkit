# Copyright (c) Evan Erwee. All rights reserved.

"""Infer Edges module for document graph operations.

This module provides the EdgeInferencer transformer, which creates edges between
records based on matching field values. This is useful for building graph
representations from structured data by establishing relationships between
records that share common attribute values.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class EdgeInferencer(TransformerProvider):
    """Transformer provider that infers edges between records based on matching fields.
    
    This transformer creates edge records between records that share the same value
    in a specified field. It's useful for building graph representations from
    structured data by establishing relationships between related records.
    
    Args:
        The provider expects the following arguments in the `args` dictionary:
            source_field (str): The field name to use for grouping records.
                               Default is "project_id".
            edge_type (str): The relationship type to assign to the created edges.
                           Default is "RELATED_TO".
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create edge records between records that share the same field value.
        
        This method processes a list of records and creates edge records between
        consecutive records that share the same value in the specified source field.
        The edges are created with the specified relationship type.
        
        Args:
            records (List[Dict[str, Any]]): A list of records to process, where each
                                           record is a dictionary with string keys.
        
        Returns:
            List[Dict[str, Any]]: A new list containing the original records plus
                                 the newly created edge records.
        """
        source_field = self.args.get("source_field", "project_id")
        edge_type = self.args.get("edge_type", "RELATED_TO")
        
        # Group records by source_field
        groups = {}
        for record in records:
            key = record.get(source_field)
            if key is not None:
                groups.setdefault(key, []).append(record)

        # Create edge records within each group
        edge_records = []
        for group_records in groups.values():
            for i in range(len(group_records) - 1):
                source = group_records[i]
                target = group_records[i + 1]
                
                edge_record = {
                    "edge_type": "edge",
                    "source_id": source.get("id", f"record_{i}"),
                    "target_id": target.get("id", f"record_{i+1}"),
                    "relationship": edge_type,
                    "group_key": key
                }
                edge_records.append(edge_record)

        return records + edge_records  # Return original records plus inferred edges