# Copyright (c) Evan Erwee. All rights reserved.

"""Timestamp normalization transformer for document graph operations.

This module provides a transformer that converts timestamp fields from various formats
into a standardized ISO 8601 format. This normalization enables consistent timestamp
handling, comparison, and querying across different data sources that may use
different timestamp formats.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from datetime import datetime
from dateutil import parser
from document_graph.transform.transformer_provider_base import TransformerProvider


class TimestampNormalizer(TransformerProvider):
    """Normalizes timestamp fields to ISO 8601 format.
    
    This transformer converts timestamp fields from various formats into a standardized
    ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ). It uses the dateutil parser to handle
    a wide variety of input formats, making it flexible for different data sources.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config)
            - field: The field containing the timestamp to normalize (default: "timestamp")
            - output_field: The field to store the normalized timestamp 
              (default: "{field}_normalized")
            
    Examples:
        >>> # Create a transformer to normalize dates in the "created_at" field
        >>> config = TransformerProviderConfig(
        ...     name="date_normalizer",
        ...     args={"field": "created_at", "output_field": "iso_date"}
        ... )
        >>> transformer = TimestampNormalizer(config)
        >>> result = transformer.transform([
        ...     {"id": 1, "created_at": "Jan 1, 2023 14:30:45"},
        ...     {"id": 2, "created_at": "2023/02/15 09:15 AM"}
        ... ])
        >>> result[0]["iso_date"]
        '2023-01-01T14:30:45'
        >>> result[1]["iso_date"]
        '2023-02-15T09:15:00'
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by normalizing timestamp fields to ISO 8601 format.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with normalized timestamp fields
            
        Note:
            If timestamp parsing fails, an error field will be added to the record
            with the output field name and "_error" suffix.
        """
        field = self.args.get("field", "timestamp")
        output_field = self.args.get("output_field", f"{field}_normalized")
        
        normalized_records = []
        for record in records:
            normalized_record = record.copy()
            raw_value = record.get(field)
            
            if raw_value:
                try:
                    dt: datetime = parser.parse(str(raw_value))
                    normalized_record[output_field] = dt.isoformat()
                except Exception as e:
                    normalized_record[f"{output_field}_error"] = str(e)
            
            normalized_records.append(normalized_record)

        return normalized_records