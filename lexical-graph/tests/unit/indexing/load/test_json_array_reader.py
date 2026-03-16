# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.indexing.load.json_array_reader import JSONArrayReader


class TestJSONArrayReaderInitialization:
    """Tests for JSONArrayReader initialization."""
    
    def test_initialization_default(self):
        """Verify JSONArrayReader initializes with default parameters."""
        reader = JSONArrayReader()
        assert reader is not None


class TestJSONArrayReaderReading:
    """Tests for JSON array reading functionality."""
    
    def test_read_valid_json_array(self):
        """Verify reading valid JSON array."""
        reader = JSONArrayReader()
        
        json_data = [
            {"id": "1", "text": "Document 1"},
            {"id": "2", "text": "Document 2"}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(json_data, f)
            temp_path = f.name
        
        try:
            result = reader.load_data(temp_path)
            
            assert result is not None
            assert len(result) == 2
        finally:
            Path(temp_path).unlink()
    
    def test_read_empty_json_array(self):
        """Verify reading empty JSON array."""
        reader = JSONArrayReader()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([], f)
            temp_path = f.name
        
        try:
            result = reader.load_data(temp_path)
            
            assert result is not None
            assert len(result) == 0
        finally:
            Path(temp_path).unlink()
    
    def test_read_json_with_nested_objects(self):
        """Verify reading JSON with nested objects."""
        reader = JSONArrayReader()
        
        json_data = [
            {
                "id": "1",
                "text": "Document 1",
                "metadata": {"source": "test", "date": "2024-01-01"}
            }
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(json_data, f)
            temp_path = f.name
        
        try:
            result = reader.load_data(temp_path)
            
            assert result is not None
            assert len(result) == 1
        finally:
            Path(temp_path).unlink()


class TestJSONArrayReaderErrorHandling:
    """Tests for error handling in JSON reading."""
    
    def test_handle_invalid_json(self):
        """Verify handling of invalid JSON."""
        reader = JSONArrayReader()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            temp_path = f.name
        
        try:
            with pytest.raises(json.JSONDecodeError):
                reader.load_data(temp_path)
        finally:
            Path(temp_path).unlink()
    
    def test_handle_missing_file(self):
        """Verify handling of missing file."""
        reader = JSONArrayReader()
        
        with pytest.raises(FileNotFoundError):
            reader.load_data("/nonexistent/path.json")
