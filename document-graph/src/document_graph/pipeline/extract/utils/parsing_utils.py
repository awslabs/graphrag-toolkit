# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Parsing utilities for text processing in document graph operations.

This module provides utility functions for cleaning and parsing text data
before it is processed further in the document graph pipeline.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401


import re
from typing import List

def clean_text(text: str) -> str:
    """Clean text by removing excess whitespace.
    
    This function strips leading and trailing whitespace and normalizes
    all internal whitespace sequences (spaces, tabs, newlines) to a single space.
    
    Args:
        text: The input text to clean
        
    Returns:
        The cleaned text with normalized whitespace
    """
    return re.sub(r'\s+', ' ', text.strip())

def split_into_paragraphs(text: str) -> List[str]:
    """Split text into logical paragraphs.
    
    This function divides text into paragraphs by looking for double line breaks
    (i.e., an empty line between paragraphs). It strips whitespace from each
    paragraph and filters out empty paragraphs.
    
    Args:
        text: The input text to split into paragraphs
        
    Returns:
        A list of paragraphs, with each paragraph as a string
    """
    return [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip()]
