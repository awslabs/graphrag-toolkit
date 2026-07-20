# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Spelling Provider module for document graph operations.

This module provides a transformer provider implementation for correcting spelling
errors in text fields of document records using the TextBlob library. It supports:

- Automatic detection and correction of misspelled words
- Processing of multiple configurable fields
- Graceful handling of errors during spelling correction
- Preservation of non-string and empty values

Spelling normalization is useful for improving text quality, enhancing search
and matching operations, and preparing text for further natural language processing.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider

try:
    from textblob import TextBlob
except ImportError:
    TextBlob = None


class NormalizeSpellingProvider(TransformerProvider):
    """Normalizes spelling using TextBlob.
    
    This transformer provider corrects spelling errors in text fields of document
    records using the TextBlob library's spell checking capabilities. It processes
    specified fields in each record and handles errors gracefully.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config), including:
            - fields: List of field names to process (default: ["content"])
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by correcting spelling errors in text fields.
        
        Processes each record and applies spelling correction to the specified fields
        using TextBlob. Non-string values and empty strings are preserved unchanged.
        Errors during spelling correction are caught and the original value is kept.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with corrected spelling
            
        Raises:
            ImportError: If TextBlob is not installed
        """
        if TextBlob is None:
            raise ImportError("TextBlob required for spelling normalization")
            
        fields = self.args.get("fields", ["content"])
        
        normalized_records = []
        for record in records:
            normalized_record = record.copy()
            
            for field in fields:
                value = record.get(field)
                if value and isinstance(value, str):
                    try:
                        blob = TextBlob(value)
                        normalized_record[field] = str(blob.correct())
                    except Exception:
                        pass  # Keep original value on error
            
            normalized_records.append(normalized_record)

        return normalized_records