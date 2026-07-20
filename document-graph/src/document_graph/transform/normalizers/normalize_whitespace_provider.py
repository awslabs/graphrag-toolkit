# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Whitespace Provider module for document graph operations.

This module provides a transformer provider implementation for normalizing whitespace
in text fields of document records. It supports:

- Removing leading and trailing whitespace
- Replacing multiple consecutive whitespace characters with a single space
- Processing of multiple configurable fields
- Preservation of non-string and empty values

Whitespace normalization is useful for improving text consistency, enhancing
search and matching operations, and preparing text for further processing or
display.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


import re
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class NormalizeWhitespaceProvider(TransformerProvider):
    """Normalizes whitespace in text fields.
    
    This transformer provider standardizes whitespace in text fields of document
    records by removing leading and trailing whitespace and replacing multiple
    consecutive whitespace characters with a single space.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config), including:
            - fields: List of field names to process (default: ["content"])
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by normalizing whitespace in text fields.
        
        Processes each record and applies whitespace normalization to the specified
        fields. Non-string values and empty strings are preserved unchanged.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with normalized whitespace
        """
        fields = self.args.get("fields", ["content"])
        
        normalized_records = []
        for record in records:
            normalized_record = record.copy()
            
            for field in fields:
                value = record.get(field)
                if value and isinstance(value, str):
                    normalized_record[field] = re.sub(r'\s+', ' ', value.strip())
            
            normalized_records.append(normalized_record)

        return normalized_records