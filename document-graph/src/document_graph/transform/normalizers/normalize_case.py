# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Case module for document graph operations.

This module provides functionality for normalizing the case of text strings
in document processing pipelines. It supports:

- Converting text to lowercase (default)
- Converting text to uppercase
- Other case transformations supported by Python string methods

Case normalization is useful for standardizing text data, improving search
and matching operations, and preparing text for further processing.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


def normalize(value: str, mode="lower") -> str:
    """Normalize the case of a string value.
    
    Applies case transformation to the input string using the specified mode.
    The mode parameter corresponds to a Python string method name (e.g., 'lower',
    'upper', 'capitalize', 'title').
    
    Args:
        value (str): The string to normalize
        mode (str, optional): The case transformation to apply. Defaults to "lower".
            Valid values include 'lower', 'upper', 'capitalize', 'title', etc.
            
    Returns:
        str: The normalized string with the specified case transformation applied
        
    Raises:
        AttributeError: If the specified mode is not a valid string method
    """
    return getattr(value, mode)()
