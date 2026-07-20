# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Case Provider module for document graph operations.

This module provides a transformer provider implementation for normalizing the case
of text strings in document records. It supports:

- Converting text to lowercase (default)
- Converting text to uppercase
- Other case transformations supported by Python string methods

The provider applies case normalization to all string fields in each record,
preserving non-string fields unchanged. This is useful for standardizing text data
across records, improving search and matching operations, and preparing text for
further processing.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class NormalizeCaseProvider(TransformerProvider):
    """Normalizes text case in record fields.
    
    This transformer provider applies case normalization to all string fields
    in each record, preserving non-string fields unchanged. It supports various
    case transformations through the 'mode' configuration parameter.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config), including:
            - mode: The case transformation to apply (default: 'lower')
    """
    
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply case normalization to string fields in records.
        
        Processes each record and applies the specified case transformation
        (lowercase by default) to all string fields, leaving non-string fields
        unchanged.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with normalized case in string fields
        """
        self._log_transform_start(len(records))
        
        mode = self.args.get('mode', 'lower')
        
        transformed_records = []
        for record in records:
            transformed_record = {}
            for key, value in record.items():
                if isinstance(value, str):
                    transformed_record[key] = getattr(value, mode)()
                else:
                    transformed_record[key] = value
            transformed_records.append(transformed_record)
        
        self._log_transform_end(len(records), len(transformed_records))
        return transformed_records