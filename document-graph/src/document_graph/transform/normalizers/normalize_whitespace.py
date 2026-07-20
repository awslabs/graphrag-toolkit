# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Whitespace module for document graph operations.

This module provides functionality for normalizing whitespace in text strings.
It supports:

- Removing leading and trailing whitespace
- Replacing multiple consecutive whitespace characters with a single space
- Standardizing whitespace representation across different text sources

Whitespace normalization is useful for improving text consistency, enhancing
search and matching operations, and preparing text for further processing or
display.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


import re

def normalize(value: str) -> str:
    """Normalize whitespace in a text string.
    
    Removes leading and trailing whitespace and replaces multiple consecutive
    whitespace characters (spaces, tabs, newlines, etc.) with a single space.
    
    Args:
        value (str): The string to normalize
        
    Returns:
        str: The normalized string with standardized whitespace
        
    Examples:
        >>> normalize("  Hello   world!\\n")
        'Hello world!'
    """
    return re.sub(r'\s+', ' ', value.strip())
