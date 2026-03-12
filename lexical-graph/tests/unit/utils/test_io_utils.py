# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json

import pytest

from graphrag_toolkit.lexical_graph.utils.io_utils import (
    read_text,
    write_text,
    read_json,
    write_json,
)


# ---------------------------------------------------------------------------
# write_text + read_text
# ---------------------------------------------------------------------------

class TestTextReadWrite:

    def test_roundtrip(self, tmp_path):
        path = str(tmp_path / "file.txt")
        write_text(path, "hello world")
        assert read_text(path) == "hello world"

    def test_creates_parent_directories(self, tmp_path):
        path = str(tmp_path / "a" / "b" / "c" / "file.txt")
        write_text(path, "content")
        assert read_text(path) == "content"

    def test_unicode_roundtrip(self, tmp_path):
        path = str(tmp_path / "unicode.txt")
        content = "caf\u00e9 \u2603 \U0001f30d"
        write_text(path, content)
        assert read_text(path) == content

    def test_empty_string_roundtrip(self, tmp_path):
        path = str(tmp_path / "empty.txt")
        write_text(path, "")
        assert read_text(path) == ""

    def test_multiline_roundtrip(self, tmp_path):
        path = str(tmp_path / "multi.txt")
        content = "line1\nline2\nline3"
        write_text(path, content)
        assert read_text(path) == content

    def test_read_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            read_text("/nonexistent/path/file.txt")


# ---------------------------------------------------------------------------
# write_json + read_json
# ---------------------------------------------------------------------------

class TestJsonReadWrite:

    def test_dict_roundtrip(self, tmp_path):
        path = str(tmp_path / "data.json")
        data = {"key": "value", "number": 42}
        write_json(path, data)
        assert read_json(path) == data

    def test_list_roundtrip(self, tmp_path):
        path = str(tmp_path / "list.json")
        data = [1, "two", {"three": 3}]
        write_json(path, data)
        assert read_json(path) == data

    def test_unicode_preservation(self, tmp_path):
        path = str(tmp_path / "unicode.json")
        data = {"name": "caf\u00e9", "emoji": "\U0001f30d"}
        write_json(path, data)
        assert read_json(path) == data

    def test_creates_parent_directories(self, tmp_path):
        path = str(tmp_path / "x" / "y" / "data.json")
        write_json(path, {"nested": True})
        assert read_json(path) == {"nested": True}

    def test_read_invalid_json_raises(self, tmp_path):
        path = str(tmp_path / "bad.json")
        write_text(path, "not valid json {{{")
        with pytest.raises(json.JSONDecodeError):
            read_json(path)

    def test_read_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            read_json("/nonexistent/path/data.json")


# ---------------------------------------------------------------------------
# s3_read_data + s3_write_data
# ---------------------------------------------------------------------------

class TestS3ReadWrite:

    def test_s3_read_data_success(self, monkeypatch):
        from unittest.mock import mock_open, MagicMock
        import smart_open
        
        mock_file = mock_open(read_data='s3 content')()
        mock_smart_open = MagicMock(return_value=mock_file)
        monkeypatch.setattr('smart_open.smart_open', mock_smart_open)
        
        from graphrag_toolkit.lexical_graph.utils.io_utils import s3_read_data
        result = s3_read_data('s3://bucket/key')
        
        assert result == 's3 content'
        mock_smart_open.assert_called_once_with('s3://bucket/key', 'r')

    def test_s3_read_data_failure(self, monkeypatch):
        from unittest.mock import MagicMock
        import smart_open
        
        mock_smart_open = MagicMock(side_effect=OSError('S3 read error'))
        monkeypatch.setattr('smart_open.smart_open', mock_smart_open)
        
        from graphrag_toolkit.lexical_graph.utils.io_utils import s3_read_data
        with pytest.raises(OSError, match='S3 read error'):
            s3_read_data('s3://bucket/key')

    def test_s3_write_data_success(self, monkeypatch):
        from unittest.mock import mock_open, MagicMock
        import smart_open
        
        mock_file = mock_open()()
        mock_smart_open = MagicMock(return_value=mock_file)
        monkeypatch.setattr('smart_open.smart_open', mock_smart_open)
        
        from graphrag_toolkit.lexical_graph.utils.io_utils import s3_write_data
        s3_write_data('s3://bucket/key', 'test data')
        
        mock_smart_open.assert_called_once_with('s3://bucket/key', 'w')
        mock_file.write.assert_called_once_with('test data')

    def test_s3_write_data_failure(self, monkeypatch):
        from unittest.mock import MagicMock
        import smart_open
        
        mock_smart_open = MagicMock(side_effect=OSError('S3 write error'))
        monkeypatch.setattr('smart_open.smart_open', mock_smart_open)
        
        from graphrag_toolkit.lexical_graph.utils.io_utils import s3_write_data
        with pytest.raises(OSError, match='S3 write error'):
            s3_write_data('s3://bucket/key', 'test data')


# ---------------------------------------------------------------------------
# Path validation and normalization
# ---------------------------------------------------------------------------

class TestPathHandling:

    def test_write_text_with_relative_path(self, tmp_path, monkeypatch):
        """Test write_text handles relative paths correctly."""
        monkeypatch.chdir(tmp_path)
        write_text("relative/path/file.txt", "content")
        assert read_text("relative/path/file.txt") == "content"

    def test_write_json_with_relative_path(self, tmp_path, monkeypatch):
        """Test write_json handles relative paths correctly."""
        monkeypatch.chdir(tmp_path)
        data = {"key": "value"}
        write_json("relative/path/data.json", data)
        assert read_json("relative/path/data.json") == data

    def test_write_text_creates_deeply_nested_directories(self, tmp_path):
        """Test write_text creates multiple nested directory levels."""
        path = str(tmp_path / "a" / "b" / "c" / "d" / "e" / "file.txt")
        write_text(path, "deep content")
        assert read_text(path) == "deep content"

    def test_write_json_creates_deeply_nested_directories(self, tmp_path):
        """Test write_json creates multiple nested directory levels."""
        path = str(tmp_path / "a" / "b" / "c" / "d" / "e" / "data.json")
        data = {"nested": "deep"}
        write_json(path, data)
        assert read_json(path) == data

    def test_read_text_with_special_characters_in_path(self, tmp_path):
        """Test read_text handles paths with special characters."""
        # Create directory with special characters (that are valid on most filesystems)
        special_dir = tmp_path / "dir-with_special.chars"
        special_dir.mkdir()
        path = str(special_dir / "file.txt")
        write_text(path, "special path content")
        assert read_text(path) == "special path content"

    def test_write_text_overwrites_existing_file(self, tmp_path):
        """Test write_text overwrites existing file content."""
        path = str(tmp_path / "file.txt")
        write_text(path, "original content")
        write_text(path, "new content")
        assert read_text(path) == "new content"

    def test_write_json_overwrites_existing_file(self, tmp_path):
        """Test write_json overwrites existing file content."""
        path = str(tmp_path / "data.json")
        write_json(path, {"old": "data"})
        write_json(path, {"new": "data"})
        assert read_json(path) == {"new": "data"}


# ---------------------------------------------------------------------------
# Edge cases and error handling
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_read_text_large_file(self, tmp_path):
        """Test read_text handles large files."""
        path = str(tmp_path / "large.txt")
        # Create a large file (1MB of text)
        large_content = "x" * (1024 * 1024)
        write_text(path, large_content)
        assert len(read_text(path)) == len(large_content)

    def test_write_json_with_nested_structures(self, tmp_path):
        """Test write_json handles deeply nested structures."""
        path = str(tmp_path / "nested.json")
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep"
                        }
                    }
                }
            }
        }
        write_json(path, data)
        assert read_json(path) == data

    def test_write_json_with_list_of_dicts(self, tmp_path):
        """Test write_json handles list of dictionaries."""
        path = str(tmp_path / "list.json")
        data = [
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second"},
            {"id": 3, "name": "third"}
        ]
        write_json(path, data)
        assert read_json(path) == data

    def test_read_json_with_numbers(self, tmp_path):
        """Test read_json handles various number types."""
        path = str(tmp_path / "numbers.json")
        data = {
            "int": 42,
            "float": 3.14,
            "negative": -10,
            "zero": 0,
            "large": 1000000
        }
        write_json(path, data)
        result = read_json(path)
        assert result == data

    def test_read_json_with_boolean_and_null(self, tmp_path):
        """Test read_json handles boolean and null values."""
        path = str(tmp_path / "bool_null.json")
        data = {
            "true_val": True,
            "false_val": False,
            "null_val": None
        }
        write_json(path, data)
        result = read_json(path)
        assert result == data

    def test_write_text_with_tabs(self, tmp_path):
        """Test write_text preserves tab characters."""
        path = str(tmp_path / "tabs.txt")
        content = "col1\tcol2\tcol3\nval1\tval2\tval3"
        write_text(path, content)
        assert read_text(path) == content
