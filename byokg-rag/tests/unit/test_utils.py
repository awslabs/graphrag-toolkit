"""Tests for utils.py functions.

This module tests utility functions including YAML loading, response parsing,
token counting, and input validation.
"""

import pytest
import tempfile
import os
from pathlib import Path
from graphrag_toolkit.byokg_rag.utils import (
    load_yaml,
    parse_response,
    count_tokens,
    validate_input_length
)


class TestLoadYaml:
    """Tests for load_yaml function."""
    
    def test_load_yaml_valid_file(self):
        """Verify YAML loading with valid file content."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('key: value\nlist:\n  - item1\n  - item2\nnumber: 42')
            temp_path = f.name
        
        try:
            result = load_yaml(temp_path)
            assert result == {'key': 'value', 'list': ['item1', 'item2'], 'number': 42}
        finally:
            os.unlink(temp_path)
    
    def test_load_yaml_relative_path(self, monkeypatch):
        """Verify relative path resolution from module directory."""
        # Create a temporary YAML file
        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_file = Path(tmpdir) / 'test.yaml'
            yaml_file.write_text('test_key: test_value')
            
            # Mock __file__ to point to the temp directory
            # The load_yaml function uses osp.dirname(osp.abspath(__file__))
            # We need to patch the utils module's __file__ attribute
            import graphrag_toolkit.byokg_rag.utils as utils_module
            original_file = utils_module.__file__
            
            try:
                # Set the module's __file__ to be in our temp directory
                utils_module.__file__ = str(Path(tmpdir) / 'utils.py')
                
                # Test with relative path
                result = load_yaml('test.yaml')
                assert result == {'test_key': 'test_value'}
            finally:
                # Restore original __file__
                utils_module.__file__ = original_file


class TestParseResponse:
    """Tests for parse_response function."""
    
    def test_parse_response_valid_pattern(self):
        """Verify regex pattern matching extracts content correctly."""
        response = "Some text <tag>line1\nline2\nline3</tag> more text"
        pattern = r"<tag>(.*?)</tag>"
        
        result = parse_response(response, pattern)
        
        assert result == ['line1', 'line2', 'line3']
    
    def test_parse_response_no_match(self):
        """Verify empty list returned when pattern doesn't match."""
        response = "No tags here"
        pattern = r"<tag>(.*?)</tag>"
        
        result = parse_response(response, pattern)
        
        assert result == []
    
    def test_parse_response_non_string_input(self):
        """Verify empty list returned for non-string input."""
        result = parse_response(None, r"<tag>(.*?)</tag>")
        assert result == []
        
        result = parse_response(123, r"<tag>(.*?)</tag>")
        assert result == []


class TestCountTokens:
    """Tests for count_tokens function."""
    
    def test_count_tokens_empty_string(self):
        """Verify token counting returns 0 for empty string."""
        assert count_tokens("") == 0
    
    def test_count_tokens_none_input(self):
        """Verify token counting returns 0 for None input."""
        assert count_tokens(None) == 0
    
    def test_count_tokens_normal_text(self):
        """Verify token counting for normal text (~4 chars per token)."""
        text = "This is a test"  # 14 chars
        assert count_tokens(text) == 3  # 14 // 4 = 3
    
    def test_count_tokens_long_text(self):
        """Verify token counting for longer text."""
        text = "x" * 1000  # 1000 chars
        assert count_tokens(text) == 250  # 1000 // 4 = 250


class TestValidateInputLength:
    """Tests for validate_input_length function."""
    
    def test_validate_input_length_within_limit(self):
        """Verify validation passes when input is within limit."""
        validate_input_length("short text", max_tokens=100)
        # Should not raise any exception
    
    def test_validate_input_length_at_limit(self):
        """Verify validation passes when input is exactly at limit."""
        text = "x" * 400  # Exactly 100 tokens
        validate_input_length(text, max_tokens=100)
        # Should not raise any exception
    
    def test_validate_input_length_exceeds_limit(self):
        """Verify ValueError raised when input exceeds limit."""
        long_text = "x" * 1000  # ~250 tokens
        
        with pytest.raises(ValueError) as exc_info:
            validate_input_length(long_text, max_tokens=100, input_name="test_input")
        
        assert "test_input exceeds maximum token limit" in str(exc_info.value)
        assert "~250 tokens" in str(exc_info.value)
        assert "Maximum: 100 tokens" in str(exc_info.value)
    
    def test_validate_input_length_empty_string(self):
        """Verify validation passes for empty string."""
        validate_input_length("", max_tokens=100)
        # Should not raise any exception
    
    def test_validate_input_length_none_input(self):
        """Verify validation passes for None input."""
        validate_input_length(None, max_tokens=100)
        # Should not raise any exception
