# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Nulls module for document graph operations.

This module provides functionality for normalizing null-like values in text data
to actual None values. It supports:

- Recognition of common null-like string representations (e.g., "n/a", "null", "none")
- Case-insensitive matching of null-like strings
- Conversion of null-like strings to Python None

Null normalization is useful for standardizing missing data representation,
improving data quality, and ensuring consistent handling of null values in
downstream processing and analysis.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import Optional, Set

class NullNormalizer:
    """Normalizes null-like string values to actual None values.
    
    This class provides functionality to identify and convert common string
    representations of null or missing values to Python None. It uses a predefined
    set of null-like strings for matching.
    
    Attributes:
        NULL_LIKE (Set[str]): Set of string values that should be considered as null
    """
    
    NULL_LIKE = {"", "n/a", "na", "null", "none", "-"}

    def normalize(self, value: str) -> Optional[str]:
        """Normalize a string value, converting null-like strings to None.
        
        Checks if the input value is None or matches any of the predefined null-like
        strings (after stripping whitespace and converting to lowercase). If it does,
        returns None; otherwise, returns the original value unchanged.
        
        Args:
            value (str): The string value to normalize
            
        Returns:
            Optional[str]: None if the value is null-like, otherwise the original value
        """
        if value is None:
            return None
        return None if value.strip().lower() in self.NULL_LIKE else value