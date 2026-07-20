# Copyright (c) Evan Erwee. All rights reserved.

"""Length Truncator module for document graph operations.

This module provides a transformer that limits the character length of string fields
in a record to prevent oversized text fields and ensure consistent processing.
It truncates specified string fields to a maximum character length.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from document_graph.transform.transformer_provider_base import TransformerProvider

class LengthTruncator(TransformerProvider):
    """Transformer that limits the character length of string fields.
    
    This transformer ensures string fields don't exceed a maximum character length
    by truncating them to the specified maximum. This helps prevent oversized text
    fields and ensures consistent processing downstream.
    
    Attributes:
        config: Configuration object containing transformer settings
        
    Examples:
        >>> from document_graph.transform.transformer_provider_config import TransformerProviderConfig
        >>> config = TransformerProviderConfig(
        ...     name="length_truncator",
        ...     args={"max_length": 10, "fields": ["text", "description"]}
        ... )
        >>> truncator = LengthTruncator(config)
        >>> result = truncator.transform({"text": "This is a long text that will be truncated", "description": "Short desc"})
        >>> result["text"]
        'This is a '
        >>> result["description"]
        'Short desc'
    """
    
    def transform(self, record: dict) -> dict:
        """Truncate specified string fields to a maximum character length.
        
        Args:
            record: A dictionary containing record data
            
        Returns:
            A dictionary with string fields truncated to max_length characters
            
        Note:
            This implementation differs from the base class by accepting a single
            record dictionary rather than a list of records.
            
            Only processes fields specified in the "fields" configuration parameter.
            Fields that don't exist in the record or aren't strings are ignored.
        """
        max_length = self.config.args.get("max_length", 512)
        fields = self.config.args.get("fields", [])

        for field in fields:
            if field in record and isinstance(record[field], str):
                record[field] = record[field][:max_length]
        return record
