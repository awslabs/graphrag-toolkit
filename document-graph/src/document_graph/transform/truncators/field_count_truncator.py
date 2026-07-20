# Copyright (c) Evan Erwee. All rights reserved.

"""Field Count Truncator module for document graph operations.

This module provides a transformer that limits the number of fields in a record
to prevent oversized documents and ensure consistent processing. It keeps only
the first N fields from the input record, discarding any additional fields.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from document_graph.transform.transformer_provider_base import TransformerProvider

class FieldCountTruncator(TransformerProvider):
    """Transformer that limits the number of fields in a record.
    
    This transformer ensures records don't exceed a maximum number of fields
    by keeping only the first N fields and discarding any additional ones.
    This helps prevent oversized documents and ensures consistent processing
    downstream.
    
    Attributes:
        config: Configuration object containing transformer settings
        
    Examples:
        >>> from document_graph.transform.transformer_provider_config import TransformerProviderConfig
        >>> config = TransformerProviderConfig(
        ...     name="field_count_truncator",
        ...     args={"max_fields": 5}
        ... )
        >>> truncator = FieldCountTruncator(config)
        >>> result = truncator.transform({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7})
        >>> len(result)
        5
        >>> "f" in result
        False
    """
    
    def transform(self, record: dict) -> dict:
        """Limit the number of fields in a record.
        
        Args:
            record: A dictionary containing record data
            
        Returns:
            A dictionary with at most max_fields keys
            
        Note:
            This implementation differs from the base class by accepting a single
            record dictionary rather than a list of records.
        """
        max_fields = self.config.args.get("max_fields", 20)
        if len(record) > max_fields:
            keys = list(record.keys())[:max_fields]
            record = {k: record[k] for k in keys}
        return record
