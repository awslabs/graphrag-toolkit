# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Red-state tests for filter injection in the S3 Vectors metadata filter builder.

The filter used to be hand-assembled as a JSON string and then round-tripped
through `json.loads`, so a metadata value or key containing '"', '}', ']' and a
second '$and' could close the application's clause list and start a new one.
Python's JSON parser keeps the last duplicate key, which replaced the whole
clause set — dropping, for example, a tenant-scoping clause.

These tests assert the filter is built as native dicts: exactly one top-level
condition, every application clause still present, and the payload preserved as a
literal string. Before the fix they fail — the value payload silently replaces the
clause set and the issue's own payload raises JSONDecodeError.
"""

import pytest
from llama_index.core.vector_stores.types import (
    FilterCondition, FilterOperator, MetadataFilter, MetadataFilters,
)

from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes import (
    parse_metadata_filters_recursive,
)

# Reported payload. Against this module's clause template it is one '}' short, so
# pre-fix it raises JSONDecodeError rather than replacing the clause set.
REPORTED_PAYLOAD = 'x"}], "$and": [{"source.metadata.anything": {"$gt": ""}}'
# Same idea, balanced for the template: pre-fix this parses and the resulting dict
# keeps only the attacker's trailing '$and'.
VALUE_PAYLOAD = 'x"}}], "$and": [{"source.metadata.anything": {"$gt": "'
# A key takes the same unescaped path.
KEY_PAYLOAD = 'k": {"$exists": false}}], "$and": [{"source.metadata.anything'

SCOPE_CLAUSE = {'source.metadata.tenant': {'$eq': 'acme'}}


def _scoped(key, value):
    """An app-scoped filter ('tenant' = acme) AND one attacker-influenced filter."""
    return parse_metadata_filters_recursive(MetadataFilters(
        filters=[
            MetadataFilter(key='tenant', value='acme', operator=FilterOperator.EQ),
            MetadataFilter(key=key, value=value, operator=FilterOperator.EQ),
        ],
        condition=FilterCondition.AND,
    ))


class TestValueCannotReplaceClauses:
    @pytest.mark.parametrize('payload', [REPORTED_PAYLOAD, VALUE_PAYLOAD])
    def test_payload_stays_a_literal_value(self, payload):
        """A value full of JSON punctuation and a second '$and' adds no clause."""
        result = _scoped('category', payload)

        assert list(result.keys()) == ['$and'], f'clause set replaced: {result}'
        assert result['$and'] == [
            SCOPE_CLAUSE,
            {'source.metadata.category': {'$eq': payload}},
        ], f'payload altered the filter structure: {result}'

    def test_scope_clause_is_not_dropped(self):
        """The application's own clause survives alongside the payload."""
        result = _scoped('category', VALUE_PAYLOAD)

        assert SCOPE_CLAUSE in result['$and'], f'scope clause dropped: {result}'
        assert len(result['$and']) == 2, f'unexpected clause count: {result}'

    def test_no_clause_matches_the_injected_key(self):
        """The payload's 'source.metadata.anything' never becomes a real clause."""
        result = _scoped('category', VALUE_PAYLOAD)

        assert all(
            'source.metadata.anything' not in clause
            for clause in result['$and']
        ), f'injected key became a clause: {result}'


class TestKeyCannotReplaceClauses:
    def test_payload_stays_a_literal_key(self):
        """A key full of JSON punctuation is prefixed and kept literal."""
        result = _scoped(KEY_PAYLOAD, 'z')

        assert list(result.keys()) == ['$and'], f'clause set replaced: {result}'
        assert result['$and'] == [
            SCOPE_CLAUSE,
            {f'source.metadata.{KEY_PAYLOAD}': {'$eq': 'z'}},
        ], f'payload altered the filter structure: {result}'


class TestBenignFiltersStillBuild:
    def test_plain_eq_filter(self):
        """A punctuation-free filter still builds, with the value unquoted."""
        result = _scoped('category', 'tech')

        assert result == {'$and': [
            SCOPE_CLAUSE,
            {'source.metadata.category': {'$eq': 'tech'}},
        ]}

    def test_numeric_value_stays_a_number(self):
        """A numeric value reaches the filter as a number, not a string."""
        result = parse_metadata_filters_recursive(MetadataFilters(
            filters=[MetadataFilter(key='count', value=5, operator=FilterOperator.GT)],
            condition=FilterCondition.AND,
        ))

        assert result == {'$and': [{'source.metadata.count': {'$gt': 5}}]}

    def test_is_empty_uses_a_json_boolean(self):
        """IS_EMPTY maps to a real False, not the string 'false'."""
        result = parse_metadata_filters_recursive(MetadataFilters(
            filters=[MetadataFilter(key='category', value=None, operator=FilterOperator.IS_EMPTY)],
            condition=FilterCondition.AND,
        ))

        assert result == {'$and': [{'source.metadata.category': {'$exists': False}}]}

    def test_nested_group_is_a_nested_dict(self):
        """A nested filter group nests as a dict, not a re-parsed JSON string."""
        inner = MetadataFilters(
            filters=[MetadataFilter(key='a', value='x', operator=FilterOperator.EQ)],
            condition=FilterCondition.OR,
        )
        result = parse_metadata_filters_recursive(MetadataFilters(
            filters=[inner], condition=FilterCondition.AND,
        ))

        assert result == {'$and': [{'$or': [{'source.metadata.a': {'$eq': 'x'}}]}]}
