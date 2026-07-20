# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Enum Provider module for document graph operations.

This module provides a transformer provider implementation for normalizing enumeration
values in document records using a mapping dictionary. It supports:

- Standardizing categorical field values using a configurable mapping
- Case-insensitive matching (optional, enabled by default)
- Handling of variations in spelling, abbreviations, or formatting
- Preserving original values when no mapping is found

Enum normalization is useful for standardizing categorical data, ensuring consistent
terminology across datasets, and improving data quality for analysis and processing.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class NormalizeEnumProvider(TransformerProvider):
    """Normalizes enum values using a mapping dictionary.
    
    This transformer provider standardizes categorical field values in document records
    using a configurable mapping dictionary. It supports case-insensitive matching
    and preserves original values when no mapping is found.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config), including:
            - field: The field to normalize (default: "category")
            - enum_map: Dictionary mapping input values to standardized forms
            - case_insensitive: Whether to use case-insensitive matching (default: True)
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by normalizing enum values in the specified field.
        
        Processes each record and applies enum normalization to the specified field
        using the provided mapping dictionary. If case_insensitive is True (default),
        the matching is done after converting keys and values to lowercase.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with normalized enum values
        """
        field = self.args.get("field", "category")
        enum_map = self.args.get("enum_map", {})
        case_insensitive = self.args.get("case_insensitive", True)
        
        # Normalize enum_map keys to lowercase if case_insensitive
        if case_insensitive:
            enum_map = {k.lower(): v for k, v in enum_map.items()}
        
        normalized_records = []
        for record in records:
            normalized_record = record.copy()
            value = record.get(field)
            
            if value:
                lookup_key = str(value).strip().lower() if case_insensitive else str(value).strip()
                normalized_record[field] = enum_map.get(lookup_key, value)
            
            normalized_records.append(normalized_record)

        return normalized_records