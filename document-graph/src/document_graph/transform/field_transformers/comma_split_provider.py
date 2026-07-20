# Copyright (c) Evan Erwee. All rights reserved.

"""Comma-split transformer for document graph operations.

This module provides a transformer that splits comma-separated values in specified
fields and creates multiple records from a single input record. This is useful for
handling fields that contain multiple values separated by commas, such as tags,
categories, or IDs that need to be processed as separate entities.
"""

import logging

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class CommaSplitProvider(TransformerProvider):
    """Splits comma-separated values in fields and creates multiple records.
    
    This transformer takes records with comma-separated values in specified fields
    and creates multiple output records, one for each split value. This is useful
    for normalizing data where multiple values are stored in a single field.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config)
            - fields: List of fields to split on commas
            - separator: Character to split on (default: ",")
            - strip_whitespace: Whether to strip whitespace from split values (default: True)
            
    Examples:
        >>> # Create a transformer to split project IDs
        >>> config = TransformerProviderConfig(
        ...     name="project_splitter",
        ...     args={
        ...         "fields": ["project_ids"],
        ...         "separator": ",",
        ...         "strip_whitespace": True
        ...     }
        ... )
        >>> transformer = CommaSplitProvider(config)
        >>> result = transformer.transform([
        ...     {"id": 1, "project_ids": "proj-1, proj-2, proj-3", "name": "Resource A"}
        ... ])
        >>> len(result)
        3
        >>> result[0]["project_ids"]
        'proj-1'
        >>> result[1]["project_ids"]
        'proj-2'
    """
    
    def __init__(self, config):
        """Initialize the comma split transformer with configuration.
        
        Args:
            config: Transformer configuration with name, type, and args
                - fields: List of fields to split on commas
                - separator: Character to split on (default: ",")
                - strip_whitespace: Whether to strip whitespace (default: True)
        """
        super().__init__(config)
        self.separator = self.args.get('separator', ',')
        self.strip_whitespace = self.args.get('strip_whitespace', True)

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by splitting comma-separated values into multiple records.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with split values
            
        Note:
            Each input record may generate multiple output records, one for each
            split value in the specified fields. If multiple fields are specified,
            the transformation is applied to each field independently.
        """
        fields_to_split = self.args.get('fields', [])
        
        if not fields_to_split:
            return records
        
        transformed_records = []
        
        for record in records:
            # For each field that needs splitting, create separate records
            for field in fields_to_split:
                if field in record and record[field]:
                    field_value = str(record[field])
                    split_values = field_value.split(self.separator)
                    
                    if self.strip_whitespace:
                        split_values = [val.strip() for val in split_values if val.strip()]
                    else:
                        split_values = [val for val in split_values if val]
                    
                    # Create a new record for each split value
                    for split_value in split_values:
                        new_record = record.copy()
                        new_record[field] = split_value
                        transformed_records.append(new_record)
                else:
                    # If field doesn't exist or is empty, keep original record
                    transformed_records.append(record.copy())
        
        return transformed_records