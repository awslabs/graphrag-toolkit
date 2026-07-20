# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Nulls Provider module for document graph operations.

This module provides a transformer provider implementation for normalizing null-like
values in document records to actual None values. It supports:

- Recognition of common null-like string representations (e.g., "n/a", "null", "none")
- Case-insensitive matching of null-like strings
- Conversion of null-like strings to Python None
- Configurable list of fields to process
- Customizable set of null-like string patterns

Null normalization is useful for standardizing missing data representation,
improving data quality, and ensuring consistent handling of null values in
downstream processing and analysis.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class NormalizeNullsProvider(TransformerProvider):
    """Normalizes null-like values to actual None.
    
    This transformer provider identifies and converts common string representations
    of null or missing values to Python None in specified fields of document records.
    It uses a configurable set of null-like strings for matching.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config), including:
            - fields: List of field names to process
            - null_like: Set of string values that should be considered as null
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by normalizing null-like values to None.
        
        Processes each record and converts null-like string values to None in the
        specified fields. The matching is done after stripping whitespace and
        converting to lowercase.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with normalized null values
        """
        fields = self.args.get("fields", [])
        null_like = set(self.args.get("null_like", ["", "n/a", "na", "null", "none", "-"]))
        
        normalized_records = []
        for record in records:
            normalized_record = record.copy()
            
            for field in fields:
                if field in normalized_record:
                    value = normalized_record[field]
                    if isinstance(value, str) and value.strip().lower() in null_like:
                        normalized_record[field] = None
            
            normalized_records.append(normalized_record)

        return normalized_records