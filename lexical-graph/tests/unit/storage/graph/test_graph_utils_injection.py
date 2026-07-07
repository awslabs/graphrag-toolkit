# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Red-state tests for OpenCypher injection in the Neptune graph filter builder.

Metadata filter values and property keys are interpolated into OpenCypher filter
substrings (`graph_utils.parse_metadata_filters_recursive`). A value or key
containing a single quote or backtick can break out and inject Cypher. These
tests assert that values are escaped inside their single-quoted literal and that
keys are backtick-quoted (with embedded backticks doubled), so neither can
escape its token. Before the fix they fail, showing the payload reaches the
emitted Cypher unescaped.
"""

from llama_index.core.vector_stores.types import (
    FilterCondition, FilterOperator, MetadataFilter, MetadataFilters,
)

from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import (
    parse_metadata_filters_recursive,
)

VALUE_PAYLOAD = "' OR '1'='1"


def _clause(key, value, operator=FilterOperator.EQ):
    return parse_metadata_filters_recursive(MetadataFilters(
        filters=[MetadataFilter(key=key, value=value, operator=operator)],
        condition=FilterCondition.AND,
    ))


class TestValueEscaping:
    def test_single_quote_in_value_is_escaped(self):
        """A single quote in a filter value is escaped so it cannot close the literal."""
        clause = _clause('category', VALUE_PAYLOAD)
        assert "\\'" in clause, f'value quote not escaped: {clause}'
        assert "OR '1'='1" not in clause, f'value broke out of its literal: {clause}'

    def test_backslash_in_value_is_escaped(self):
        """A backslash in a filter value is doubled so it cannot escape the closing quote."""
        clause = _clause('category', "a\\' OR true")
        assert "\\\\" in clause, f'backslash not doubled: {clause}'


class TestKeyEscaping:
    def test_key_is_backtick_quoted(self):
        """A property key is backtick-quoted so a key with special characters cannot break out."""
        clause = _clause("x' OR '1'='1", 'tech')
        assert "source.`" in clause, f'key not backtick-quoted: {clause}'
        assert "source.x'" not in clause, f'raw key reached the query: {clause}'

    def test_backtick_in_key_is_doubled(self):
        """An embedded backtick in a key is doubled so it cannot close the identifier."""
        clause = _clause("a`b", 'tech')
        assert "`a``b`" in clause, f'embedded backtick not doubled: {clause}'


class TestBenignFilterStillBuilds:
    def test_plain_eq_filter(self):
        """A quote-free filter still builds: key backtick-quoted, value unchanged."""
        clause = _clause('category', 'tech')
        assert clause == "((source.`category` = 'tech'))"

    def test_numeric_value_not_quoted(self):
        """A numeric value is emitted unquoted alongside a backtick-quoted key."""
        clause = _clause('count', 5, operator=FilterOperator.GT)
        assert "source.`count` > 5" in clause
