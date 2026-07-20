# Copyright (c) Evan Erwee. All rights reserved.

"""Enumeration standardization transformer for document graph operations.

This module provides a transformer that maps field values to standardized enumeration values
using a mapping dictionary. This is useful for normalizing categorical data that may have
different representations across data sources, ensuring consistent values for analysis,
querying, and visualization.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class EnumStandardizer(TransformerProvider):
    """Maps field values to standard enumeration using a mapping dictionary.
    
    This transformer standardizes categorical data by mapping input values to a set of
    predefined standard values using a mapping dictionary. This is useful for normalizing
    data from different sources that may use different terminology for the same concepts.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config)
            - field: The field containing values to standardize (default: "category")
            - mapping: Dictionary mapping input values to standardized values (default: {})
            - case_insensitive: Whether to ignore case when looking up mappings (default: True)
            - output_field: The field to store the standardized value (default: same as input field)
            
    Examples:
        >>> # Create a transformer to standardize status values
        >>> config = TransformerProviderConfig(
        ...     name="status_standardizer",
        ...     args={
        ...         "field": "status",
        ...         "mapping": {
        ...             "active": "ACTIVE",
        ...             "enabled": "ACTIVE",
        ...             "inactive": "INACTIVE",
        ...             "disabled": "INACTIVE",
        ...             "pending": "PENDING"
        ...         },
        ...         "case_insensitive": True
        ...     }
        ... )
        >>> transformer = EnumStandardizer(config)
        >>> result = transformer.transform([
        ...     {"id": 1, "status": "Active"},
        ...     {"id": 2, "status": "DISABLED"},
        ...     {"id": 3, "status": "unknown"}
        ... ])
        >>> result[0]["status"]
        'ACTIVE'
        >>> result[1]["status"]
        'INACTIVE'
        >>> result[2]["status_unmapped"]
        'unknown'
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by standardizing enumeration values.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with standardized enumeration values
            
        Note:
            If a value cannot be mapped (not found in the mapping dictionary),
            it will be stored in a separate field with the "_unmapped" suffix.
            When case_insensitive is True, all keys in the mapping dictionary
            are compared in lowercase.
        """
        field = self.args.get("field", "category")
        mapping: Dict[str, str] = self.args.get("mapping", {})
        case_insensitive: bool = self.args.get("case_insensitive", True)
        output_field = self.args.get("output_field", field)
        
        standardized_records = []
        for record in records:
            standardized_record = record.copy()
            original = record.get(field)
            
            if original:
                key = str(original).lower() if case_insensitive else str(original)
                mapped_value = mapping.get(key)
                
                if mapped_value:
                    standardized_record[output_field] = mapped_value
                else:
                    standardized_record[f"{output_field}_unmapped"] = original
            
            standardized_records.append(standardized_record)

        return standardized_records