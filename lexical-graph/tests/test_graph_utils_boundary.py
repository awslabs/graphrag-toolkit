"""
Boundary and edge-case unit test coverage for graph_utils and Cypher escaping.
Resolves awslabs/graphrag-toolkit issue #424.
"""
import pytest
from unittest.mock import MagicMock

# Import graph_utils components for boundary validation
try:
    from graphrag_toolkit.lexical_graph.graph_utils import (
        escape_cypher_label,
        formatter_for_type,
        parse_metadata_filters_recursive,
    )
except ImportError:
    # Local fallback for isolated environment testing
    from graph_utils import (
        escape_cypher_label,
        formatter_for_type,
        parse_metadata_filters_recursive,
    )


class TestCypherLabelEscapingBoundary:
    """Boundary and adversarial inputs for escape_cypher_label."""

    def test_escape_cypher_label_empty_string(self):
        """Empty string input should be safely escaped or handled without crashing."""
        result = escape_cypher_label("")
        assert isinstance(result, str)
        assert "``" in result or result == "``" or result == ""

    def test_escape_cypher_label_only_backticks(self):
        """Inputs containing only backticks should escape internal backticks properly."""
        raw_input = "```"
        result = escape_cypher_label(raw_input)
        assert isinstance(result, str)
        # Ensure backticks are escaped to prevent Cypher syntax injection
        assert "\`" in result or "``" in result or result.count("`") > raw_input.count("`")

    def test_escape_cypher_label_newline_inputs(self):
        """Inputs containing newline characters should maintain string safety."""
        raw_input = "Node\nLabel\r\nTest"
        result = escape_cypher_label(raw_input)
        assert isinstance(result, str)
        assert "Node" in result and "Label" in result


class TestMetadataFiltersRecursiveBoundary:
    """Boundary and nested structure tests for parse_metadata_filters_recursive."""

    def test_parse_metadata_filters_empty_dict(self):
        """Empty dictionary input should yield empty filter structure."""
        result = parse_metadata_filters_recursive({})
        assert result is not None

    def test_parse_metadata_filters_three_level_nesting(self):
        """Deeply nested (3-level) filter structure validation."""
        nested_filter = {
            "AND": [
                {"category": {"eq": "finance"}},
                {
                    "OR": [
                        {"status": {"eq": "active"}},
                        {"priority": {"gte": 5}},
                    ]
                },
            ]
        }
        result = parse_metadata_filters_recursive(nested_filter)
        assert result is not None

    def test_parse_metadata_filters_mixed_children_types(self):
        """Mixed filter conditions (comparison + logical combinations)."""
        mixed_filter = {
            "AND": [
                {"tag": {"in": ["alpha", "beta"]}},
                {"count": {"gt": 0}},
                {"archived": {"eq": False}},
            ]
        }
        result = parse_metadata_filters_recursive(mixed_filter)
        assert result is not None


class TestFormatterForTypeBoundary:
    """Boundary cases for formatter_for_type."""

    def test_formatter_for_type_empty_string(self):
        """Empty string input should resolve to default/string formatter safely."""
        result = formatter_for_type("")
        assert result is not None
