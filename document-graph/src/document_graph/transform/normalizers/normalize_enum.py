# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Enum module for document graph operations.

This module provides functionality for normalizing enumeration values in text data
using a mapping dictionary. It supports:

- Case-insensitive mapping of input values to standardized forms
- Handling of variations in spelling, abbreviations, or formatting
- Fallback to original values when no mapping is found

Enum normalization is useful for standardizing categorical data, ensuring consistent
terminology across datasets, and improving data quality for analysis and processing.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import Optional, Dict

class EnumNormalizer:
    """Normalizes enumeration values using a mapping dictionary.
    
    This class provides functionality to standardize categorical or enumeration values
    by mapping input strings to standardized forms using a dictionary. It handles
    case-insensitive matching and preserves the original value when no mapping is found.
    
    Attributes:
        enum_map (Dict[str, str]): Dictionary mapping input values (lowercase) to 
            their standardized forms
    """
    
    def __init__(self, enum_map: Dict[str, str]):
        """Initialize the enum normalizer with a mapping dictionary.
        
        Args:
            enum_map (Dict[str, str]): Dictionary mapping input values to their 
                standardized forms. Keys will be converted to lowercase for 
                case-insensitive matching.
        """
        self.enum_map = {k.lower(): v for k, v in enum_map.items()}

    def normalize(self, value: str) -> Optional[str]:
        """Normalize an enumeration value using the mapping dictionary.
        
        Looks up the input value (after stripping whitespace and converting to lowercase)
        in the mapping dictionary and returns the standardized form if found.
        If not found, returns the original value unchanged.
        
        Args:
            value (str): The string value to normalize
            
        Returns:
            Optional[str]: The normalized value if found in the mapping dictionary,
                otherwise the original value
        """
        return self.enum_map.get(value.strip().lower(), value)