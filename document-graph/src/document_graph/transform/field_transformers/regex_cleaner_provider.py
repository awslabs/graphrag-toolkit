# Copyright (c) Evan Erwee. All rights reserved.

"""Regex-based text cleaning transformer for document graph operations.

This module provides a transformer that applies regular expression patterns to clean
and standardize text fields in records. It can be used to remove unwanted characters,
normalize formats, extract specific patterns, or perform other text cleaning operations
using the power and flexibility of regular expressions.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any, Pattern
import re
from document_graph.transform.transformer_provider_base import TransformerProvider


class RegexCleanerProvider(TransformerProvider):
    """Applies regex patterns to clean text fields in records.
    
    This transformer applies a list of regular expression patterns to specified fields
    in each record, replacing matches with a configured replacement string. This is useful
    for standardizing text formats, removing unwanted characters or patterns, and
    performing other text cleaning operations.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config)
            - patterns: List of regex patterns to apply (default: [])
            - replacement: String to replace matches with (default: "")
            - fields: List of fields to clean
        replacement: The replacement string to use
        _compiled_patterns: List of compiled regex pattern objects
            
    Examples:
        >>> # Create a transformer to remove HTML tags from text fields
        >>> config = TransformerProviderConfig(
        ...     name="html_cleaner",
        ...     args={
        ...         "patterns": ["<[^>]*>"],
        ...         "replacement": "",
        ...         "fields": ["description", "content"]
        ...     }
        ... )
        >>> transformer = RegexCleanerProvider(config)
        >>> result = transformer.transform([
        ...     {"id": 1, "description": "<p>This is a <b>sample</b> text.</p>"}
        ... ])
        >>> result[0]["description"]
        'This is a sample text.'
    """
    
    def __init__(self, config):
        """Initialize the regex cleaner with configuration.
        
        Args:
            config: Transformer configuration with name, type, and args
                - patterns: List of regex patterns to apply
                - replacement: String to replace matches with
                - fields: List of fields to clean
        """
        super().__init__(config)
        patterns = self.args.get('patterns', [])
        self.replacement = self.args.get('replacement', '')
        self._compiled_patterns: List[Pattern] = [re.compile(p) for p in patterns]

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by applying regex patterns to specified fields.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with cleaned text fields
            
        Note:
            The transformation modifies the specified fields in-place in each record.
            Only string fields are processed; non-string fields are left unchanged.
        """
        fields_to_clean = self.args.get('fields', [])
        
        cleaned_records = []
        for record in records:
            cleaned_record = record.copy()
            for field in fields_to_clean:
                if field in cleaned_record and isinstance(cleaned_record[field], str):
                    result = cleaned_record[field]
                    for pattern in self._compiled_patterns:
                        result = pattern.sub(self.replacement, result)
                    cleaned_record[field] = result
            cleaned_records.append(cleaned_record)
        return cleaned_records