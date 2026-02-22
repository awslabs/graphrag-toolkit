"""Tests for metadata utilities (metadata_utils.py).

get_properties_str — converts a properties dict to a sorted 'key:value;...' string,
                     returning a default value when the dict is empty or None.

last_accessed_date — returns {'last_accessed_date': '<YYYY-MM-DD>'} using today's date.
                     datetime.datetime.now() is mocked to give deterministic output.
"""

import datetime
import re

import pytest
from unittest.mock import patch

from graphrag_toolkit.lexical_graph.indexing.utils.metadata_utils import (
    get_properties_str,
    last_accessed_date,
)


# ---------------------------------------------------------------------------
# get_properties_str
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("properties,default,expected", [
    ({"b": 2, "a": 1}, "x", "a:1;b:2"),       # keys sorted alphabetically
    ({"single": "v"}, "x", "single:v"),         # single entry
    ({}, "default", "default"),                  # empty dict -> default
    (None, "default", "default"),                # None -> default
])
def test_get_properties_str(properties, default, expected):
    assert get_properties_str(properties, default) == expected


# ---------------------------------------------------------------------------
# last_accessed_date
# ---------------------------------------------------------------------------


def test_last_accessed_date_returns_dict():
    result = last_accessed_date()
    assert isinstance(result, dict)
    assert "last_accessed_date" in result


def test_last_accessed_date_format():
    """Date value matches YYYY-MM-DD format."""
    result = last_accessed_date()
    assert re.match(r"^\d{4}-\d{2}-\d{2}$", result["last_accessed_date"])


def test_last_accessed_date_mocked():
    """With a mocked datetime, the returned date matches the fixed value."""
    fake_now = datetime.datetime(2025, 6, 15, 12, 0, 0)
    with patch(
        "graphrag_toolkit.lexical_graph.indexing.utils.metadata_utils.datetime"
    ) as mock_dt:
        mock_dt.datetime.now.return_value = fake_now
        result = last_accessed_date()
    assert result == {"last_accessed_date": "2025-06-15"}


def test_last_accessed_date_accepts_extra_args():
    """The function signature is (*args), so extra positional args are silently ignored."""
    result = last_accessed_date("ignored", "also ignored")
    assert "last_accessed_date" in result
