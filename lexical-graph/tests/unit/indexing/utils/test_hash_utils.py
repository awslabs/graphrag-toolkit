# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.utils.hash_utils import get_hash


class TestGetHash:
    """Tests for get_hash function."""
    
    def test_hash_simple_string(self):
        """Verify hashing of simple string."""
        text = "Hello, World!"
        hash_value = get_hash(text)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) > 0
    
    def test_hash_deterministic(self):
        """Verify hash is deterministic for same input."""
        text = "Test string"
        hash1 = get_hash(text)
        hash2 = get_hash(text)
        
        assert hash1 == hash2
    
    def test_hash_different_inputs(self):
        """Verify different inputs produce different hashes."""
        text1 = "String 1"
        text2 = "String 2"
        
        hash1 = get_hash(text1)
        hash2 = get_hash(text2)
        
        assert hash1 != hash2
    
    def test_hash_empty_string(self):
        """Verify hashing of empty string."""
        hash_value = get_hash("")
        
        assert isinstance(hash_value, str)
        assert len(hash_value) > 0
    
    def test_hash_unicode_string(self):
        """Verify hashing of Unicode string."""
        text = "Hello 世界 🌍"
        hash_value = get_hash(text)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) > 0
    
    def test_hash_long_string(self):
        """Verify hashing of long string."""
        text = "A" * 10000
        hash_value = get_hash(text)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) > 0
    
    def test_hash_special_characters(self):
        """Verify hashing of string with special characters."""
        text = "!@#$%^&*()_+-=[]{}|;:',.<>?/~`"
        hash_value = get_hash(text)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) > 0
    
    def test_hash_whitespace_variations(self):
        """Verify different whitespace produces different hashes."""
        text1 = "Hello World"
        text2 = "Hello  World"  # Two spaces
        
        hash1 = get_hash(text1)
        hash2 = get_hash(text2)
        
        assert hash1 != hash2
    
    def test_hash_case_sensitivity(self):
        """Verify hash is case-sensitive."""
        text1 = "Hello"
        text2 = "hello"
        
        hash1 = get_hash(text1)
        hash2 = get_hash(text2)
        
        assert hash1 != hash2
    
    def test_hash_numeric_strings(self):
        """Verify hashing of numeric strings."""
        text1 = "12345"
        text2 = "54321"
        
        hash1 = get_hash(text1)
        hash2 = get_hash(text2)
        
        assert hash1 != hash2
        assert isinstance(hash1, str)
        assert isinstance(hash2, str)
