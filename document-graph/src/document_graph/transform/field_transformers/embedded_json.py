# Copyright (c) Evan Erwee. All rights reserved.

"""Embedded JSON field transformer for document graph operations.

This module provides a transformer that parses JSON data from string fields
and flattens the JSON structure into the record with prefixed field names.
This is useful for extracting structured data embedded within JSON strings
and making it available as individual fields for querying and analysis.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


import json
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class EmbeddedJSONFieldTransformer(TransformerProvider):
    """Parses JSON fields and flattens them into the record.
    
    This transformer extracts structured data from JSON strings stored in a specified field
    and adds each key-value pair from the JSON object as a separate field in the record.
    The new fields are prefixed to avoid name collisions with existing fields.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config)
            - field: The field containing the JSON data (default: "json_data")
            - prefix: Prefix to add to extracted field names (default: "embedded_")
            
    Examples:
        >>> # Create a transformer to extract JSON data from the "metadata" field
        >>> config = TransformerProviderConfig(
        ...     name="json_extractor",
        ...     args={"field": "metadata", "prefix": "meta_"}
        ... )
        >>> transformer = EmbeddedJSONFieldTransformer(config)
        >>> result = transformer.transform([
        ...     {"id": 1, "metadata": '{"author": "Jane Doe", "year": 2023}'}
        ... ])
        >>> result[0]
        {'id': 1, 'metadata': '{"author": "Jane Doe", "year": 2023}', 'meta_author': 'Jane Doe', 'meta_year': 2023}
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by extracting and flattening JSON data.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with flattened JSON fields
            
        Note:
            If the JSON parsing fails, an error field will be added to the record
            with the prefix and "error" suffix.
        """
        target_field = self.args.get("field", "json_data")
        prefix = self.args.get("prefix", "embedded_")
        
        transformed_records = []
        for record in records:
            transformed_record = record.copy()
            
            try:
                field_value = record.get(target_field, "")
                if isinstance(field_value, str):
                    embedded_data = json.loads(field_value)
                else:
                    embedded_data = field_value

                if isinstance(embedded_data, dict):
                    for key, value in embedded_data.items():
                        transformed_record[f"{prefix}{key}"] = value
                        
            except Exception as e:
                transformed_record[f"{prefix}error"] = str(e)

            transformed_records.append(transformed_record)

        return transformed_records