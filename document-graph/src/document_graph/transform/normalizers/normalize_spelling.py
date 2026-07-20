# Copyright (c) Evan Erwee. All rights reserved.

"""Normalize Spelling module for document graph operations.

This module provides functionality for correcting spelling errors in text data
using the TextBlob library. It supports:

- Automatic detection and correction of misspelled words
- Preservation of original text structure and formatting
- Handling of empty or None values

Spelling normalization is useful for improving text quality, enhancing search
and matching operations, and preparing text for further natural language processing.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


# normalize_spelling.py
from textblob import TextBlob
from typing import Optional

class SpellingNormalizer:
    """Normalizes spelling in text by correcting common spelling errors.
    
    This class provides functionality to automatically detect and correct
    spelling errors in text using the TextBlob library's spell checking
    capabilities.
    """
    
    def normalize(self, value: str) -> Optional[str]:
        """Normalize spelling in a text string.
        
        Uses TextBlob to detect and correct spelling errors in the input text.
        If the input is empty or None, returns it unchanged.
        
        Args:
            value (str): The text string to normalize
            
        Returns:
            Optional[str]: The text with corrected spelling, or the original value
                if it was empty or None
                
        Raises:
            ImportError: If TextBlob is not installed
            Various exceptions from TextBlob if text processing fails
        """
        if not value:
            return value
        blob = TextBlob(value)
        return str(blob.correct())