# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity
from graphrag_toolkit.lexical_graph.indexing.model import Fact, Entity, Relation
from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION


class TestStringComplementToEntity:
    """Tests for string_complement_to_entity function."""
    
    def test_converts_string_complement_to_entity(self):
        """Verify string complement is converted to Entity."""
        fact = Fact(
            subject=Entity(value="Subject", classification="Person"),
            predicate=Relation(value="works at"),
            complement="Company Name"
        )
        
        result = string_complement_to_entity(fact)
        
        assert isinstance(result.complement, Entity)
        assert result.complement.value == "Company Name"
        assert result.complement.classification == LOCAL_ENTITY_CLASSIFICATION
    
    def test_preserves_entity_complement(self):
        """Verify Entity complement is preserved unchanged."""
        entity_complement = Entity(value="Entity Value", classification="Organization")
        fact = Fact(
            subject=Entity(value="Subject", classification="Person"),
            predicate=Relation(value="founded"),
            complement=entity_complement
        )
        
        result = string_complement_to_entity(fact)
        
        assert isinstance(result.complement, Entity)
        assert result.complement.value == "Entity Value"
        assert result.complement.classification == "Organization"
        assert result.complement is entity_complement
    
    def test_handles_none_complement(self):
        """Verify function handles None complement."""
        fact = Fact(
            subject=Entity(value="Subject", classification="Person"),
            predicate=Relation(value="exists"),
            complement=None
        )
        
        result = string_complement_to_entity(fact)
        
        assert result.complement is None
    
    def test_handles_empty_string_complement(self):
        """Verify function handles empty string complement."""
        fact = Fact(
            subject=Entity(value="Subject", classification="Person"),
            predicate=Relation(value="has property"),
            complement=""
        )
        
        result = string_complement_to_entity(fact)
        
        assert isinstance(result.complement, Entity)
        assert result.complement.value == ""
        assert result.complement.classification == LOCAL_ENTITY_CLASSIFICATION
    
    def test_preserves_subject_and_predicate(self):
        """Verify subject and predicate are unchanged."""
        subject = Entity(value="Alice", classification="Person")
        predicate = Relation(value="knows")
        
        fact = Fact(
            subject=subject,
            predicate=predicate,
            complement="Bob"
        )
        
        result = string_complement_to_entity(fact)
        
        assert result.subject is subject
        assert result.predicate is predicate
        assert result.subject.value == "Alice"
        assert result.predicate.value == "knows"
    
    def test_handles_unicode_string_complement(self):
        """Verify function handles Unicode string complement."""
        fact = Fact(
            subject=Entity(value="Subject", classification="Person"),
            predicate=Relation(value="lives in"),
            complement="北京"
        )
        
        result = string_complement_to_entity(fact)
        
        assert isinstance(result.complement, Entity)
        assert result.complement.value == "北京"
        assert result.complement.classification == LOCAL_ENTITY_CLASSIFICATION
    
    def test_handles_special_characters_in_complement(self):
        """Verify function handles special characters in complement."""
        fact = Fact(
            subject=Entity(value="Subject", classification="Person"),
            predicate=Relation(value="uses"),
            complement="C++ & Python"
        )
        
        result = string_complement_to_entity(fact)
        
        assert isinstance(result.complement, Entity)
        assert result.complement.value == "C++ & Python"
        assert result.complement.classification == LOCAL_ENTITY_CLASSIFICATION
    
    def test_returns_same_fact_object(self):
        """Verify function modifies and returns the same Fact object."""
        fact = Fact(
            subject=Entity(value="Subject", classification="Person"),
            predicate=Relation(value="has"),
            complement="value"
        )
        
        result = string_complement_to_entity(fact)
        
        assert result is fact
    
    def test_handles_long_string_complement(self):
        """Verify function handles long string complement."""
        long_text = "A" * 1000
        fact = Fact(
            subject=Entity(value="Subject", classification="Document"),
            predicate=Relation(value="contains"),
            complement=long_text
        )
        
        result = string_complement_to_entity(fact)
        
        assert isinstance(result.complement, Entity)
        assert result.complement.value == long_text
        assert len(result.complement.value) == 1000
    
    def test_handles_numeric_string_complement(self):
        """Verify function handles numeric string complement."""
        fact = Fact(
            subject=Entity(value="Product", classification="Item"),
            predicate=Relation(value="costs"),
            complement="99.99"
        )
        
        result = string_complement_to_entity(fact)
        
        assert isinstance(result.complement, Entity)
        assert result.complement.value == "99.99"
        assert result.complement.classification == LOCAL_ENTITY_CLASSIFICATION
