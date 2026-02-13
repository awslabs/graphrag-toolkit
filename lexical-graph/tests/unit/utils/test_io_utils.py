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
