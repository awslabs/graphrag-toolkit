# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Timestamp module for document graph operations.

This module provides functionality for normalizing timestamp strings to a standard
ISO 8601 format. It supports:

- Parsing various datetime string formats using dateutil.parser
- Converting parsed datetimes to ISO 8601 format
- Handling of invalid or unparseable datetime strings
- Graceful handling of None values

Timestamp normalization is useful for standardizing date and time representations,
enabling consistent sorting and comparison operations, and ensuring compatibility
with various systems and databases.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from dateutil import parser
from typing import Optional

class TimestampNormalizer:
    """Normalizes timestamp strings to ISO 8601 format.
    
    This class provides functionality to parse various datetime string formats
    and convert them to a standardized ISO 8601 format, which is widely supported
    and enables consistent sorting and comparison operations.
    """
    
    def normalize(self, value: str) -> Optional[str]:
        """Normalize a timestamp string to ISO 8601 format.
        
        Attempts to parse the input string as a datetime using dateutil.parser
        and converts it to ISO 8601 format. If parsing fails, returns None.
        
        Args:
            value (str): The timestamp string to normalize
            
        Returns:
            Optional[str]: The normalized timestamp in ISO 8601 format if parsing
                succeeds, None otherwise
                
        Raises:
            No exceptions are raised; parsing errors are caught and None is returned
        """
        try:
            dt = parser.parse(value)
            return dt.isoformat()
        except (ValueError, TypeError):
            return None