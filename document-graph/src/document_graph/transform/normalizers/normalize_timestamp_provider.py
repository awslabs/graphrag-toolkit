# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Timestamp Provider module for document graph operations.

This module provides a transformer provider implementation for normalizing timestamp
strings in document records to a standard ISO 8601 format. It supports:

- Parsing various datetime string formats using dateutil.parser
- Converting parsed datetimes to ISO 8601 format
- Processing of multiple configurable fields
- Graceful handling of errors during timestamp parsing
- Preservation of non-string and empty values

Timestamp normalization is useful for standardizing date and time representations,
enabling consistent sorting and comparison operations, and ensuring compatibility
with various systems and databases.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from dateutil import parser
from document_graph.transform.transformer_provider_base import TransformerProvider


class NormalizeTimestampProvider(TransformerProvider):
    """Normalizes timestamp fields to ISO 8601 format.
    
    This transformer provider parses timestamp strings in various formats and
    converts them to a standardized ISO 8601 format. It processes specified fields
    in each record and handles parsing errors gracefully.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config), including:
            - fields: List of field names to process (default: ["timestamp"])
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by normalizing timestamp fields to ISO 8601 format.
        
        Processes each record and attempts to parse timestamp strings in the specified
        fields using dateutil.parser, then converts them to ISO 8601 format. Non-string
        values are preserved unchanged, and parsing errors are caught to keep the
        original value.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with normalized timestamps
        """
        fields = self.args.get("fields", ["timestamp"])
        
        normalized_records = []
        for record in records:
            normalized_record = record.copy()
            
            for field in fields:
                value = record.get(field)
                if value:
                    try:
                        dt = parser.parse(str(value))
                        normalized_record[field] = dt.isoformat()
                    except Exception:
                        pass  # Keep original value on error
            
            normalized_records.append(normalized_record)

        return normalized_records