"""Tests for fact_utils.py — fact manipulation utilities."""

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import (
    string_complement_to_entity,
)
from graphrag_toolkit.lexical_graph.indexing.model import Entity, Fact, Relation
from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION


# ---------------------------------------------------------------------------
# string_complement_to_entity
# ---------------------------------------------------------------------------

class TestStringComplementToEntity:

    def test_converts_string_complement_to_entity(self):
        fact = Fact(
            subject=Entity(value="A", classification="X"),
            predicate=Relation(value="relates"),
            complement="some string",
        )
        result = string_complement_to_entity(fact)

        assert isinstance(result.complement, Entity)
        assert result.complement.value == "some string"
        assert result.complement.classification == LOCAL_ENTITY_CLASSIFICATION

    def test_preserves_existing_entity_complement(self):
        entity = Entity(value="B", classification="Y")
        fact = Fact(
            subject=Entity(value="A", classification="X"),
            predicate=Relation(value="relates"),
            complement=entity,
        )
        result = string_complement_to_entity(fact)

        assert result.complement is entity

    def test_none_complement_unchanged(self):
        fact = Fact(
            subject=Entity(value="A", classification="X"),
            predicate=Relation(value="relates"),
            complement=None,
        )
        result = string_complement_to_entity(fact)

        assert result.complement is None
