# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.utils.metadata_utils import (
    remove_collection_items_from_metadata,
    get_properties_str,
    last_accessed_date
)


class TestRemoveCollectionItemsFromMetadata:
    """Tests for remove_collection_items_from_metadata function."""
    
    def test_remove_list_from_metadata(self):
        """Verify removal of list items from metadata."""
        metadata = {
            "source": "test",
            "tags": ["tag1", "tag2"],  # List should be removed
            "date": "2024-01-01"
        }
        
        clean_metadata, invalid_items = remove_collection_items_from_metadata(metadata)
        
        assert "source" in clean_metadata
        assert "date" in clean_metadata
        assert "tags" not in clean_metadata
        assert "tags" in invalid_items
    
    def test_remove_dict_from_metadata(self):
        """Verify removal of dict items from metadata."""
        metadata = {
            "source": "test",
            "nested": {"key": "value"},  # Dict should be removed
            "date": "2024-01-01"
        }
        
        clean_metadata, invalid_items = remove_collection_items_from_metadata(metadata)
        
        assert "source" in clean_metadata
        assert "date" in clean_metadata
        assert "nested" not in clean_metadata
        assert "nested" in invalid_items
    
    def test_remove_set_from_metadata(self):
        """Verify removal of set items from metadata."""
        metadata = {
            "source": "test",
            "tags_set": {"tag1", "tag2"},  # Set should be removed
            "date": "2024-01-01"
        }
        
        clean_metadata, invalid_items = remove_collection_items_from_metadata(metadata)
        
        assert "source" in clean_metadata
        assert "date" in clean_metadata
        assert "tags_set" not in clean_metadata
        assert "tags_set" in invalid_items
    
    def test_keep_simple_types(self):
        """Verify simple types are kept in metadata."""
        metadata = {
            "source": "test",
            "count": 42,
            "active": True,
            "date": "2024-01-01"
        }
        
        clean_metadata, invalid_items = remove_collection_items_from_metadata(metadata)
        
        assert len(clean_metadata) == 4
        assert len(invalid_items) == 0
        assert clean_metadata["source"] == "test"
        assert clean_metadata["count"] == 42
        assert clean_metadata["active"] is True
    
    def test_empty_metadata(self):
        """Verify handling of empty metadata."""
        metadata = {}
        
        clean_metadata, invalid_items = remove_collection_items_from_metadata(metadata)
        
        assert len(clean_metadata) == 0
        assert len(invalid_items) == 0


class TestGetPropertiesStr:
    """Tests for get_properties_str function."""
    
    def test_get_properties_str_with_properties(self):
        """Verify string generation from properties."""
        properties = {"key1": "value1", "key2": "value2"}
        
        result = get_properties_str(properties, "default")
        
        assert isinstance(result, str)
        assert "key1:value1" in result
        assert "key2:value2" in result
    
    def test_get_properties_str_empty_properties(self):
        """Verify default value returned for empty properties."""
        properties = {}
        default = "default_value"
        
        result = get_properties_str(properties, default)
        
        assert result == default
    
    def test_get_properties_str_none_properties(self):
        """Verify default value returned for None properties."""
        properties = None
        default = "default_value"
        
        result = get_properties_str(properties, default)
        
        assert result == default
    
    def test_get_properties_str_sorted(self):
        """Verify properties are sorted in output."""
        properties = {"z_key": "z_value", "a_key": "a_value", "m_key": "m_value"}
        
        result = get_properties_str(properties, "default")
        
        # Should be sorted alphabetically
        assert result.startswith("a_key:a_value")


class TestLastAccessedDate:
    """Tests for last_accessed_date function."""
    
    def test_last_accessed_date_returns_dict(self):
        """Verify last_accessed_date returns dictionary."""
        result = last_accessed_date()
        
        assert isinstance(result, dict)
        assert "last_accessed_date" in result
    
    def test_last_accessed_date_format(self):
        """Verify date format is YYYY-MM-DD."""
        result = last_accessed_date()
        
        date_str = result["last_accessed_date"]
        assert isinstance(date_str, str)
        assert len(date_str) == 10  # YYYY-MM-DD format
        assert date_str[4] == "-"
        assert date_str[7] == "-"
    
    def test_last_accessed_date_with_args(self):
        """Verify function accepts arbitrary positional arguments."""
        # last_accessed_date only accepts positional args, not keyword args
        result = last_accessed_date("arg1", "arg2")
        
        assert isinstance(result, dict)
        assert "last_accessed_date" in result

