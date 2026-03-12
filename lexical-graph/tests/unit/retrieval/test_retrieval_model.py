# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.retrieval.model import (
    Statement,
    Chunk,
    Topic,
    Source,
    SearchResult,
    Entity,
    ScoredEntity,
    EntityContext,
    EntityContexts,
    SearchResultCollection,
    Versioning
)


class TestEntityContexts:
    """Tests for EntityContexts model."""
    
    def test_context_strs_with_entities(self):
        """Test context_strs property with entities."""
        entity1 = Entity(entityId="e1", value="Python", classification="Language")
        entity2 = Entity(entityId="e2", value="Java", classification="Language")
        scored1 = ScoredEntity(entity=entity1, score=0.9)
        scored2 = ScoredEntity(entity=entity2, score=0.8)
        
        context1 = EntityContext(entities=[scored1, scored2])
        contexts = EntityContexts(contexts=[context1], keywords=[])
        
        assert contexts.context_strs == ["python, java"]
    
    def test_context_strs_empty(self):
        """Test context_strs property with no entities."""
        contexts = EntityContexts(contexts=[], keywords=[])
        assert contexts.context_strs == []
    
    def test_keywords_str_with_keywords(self):
        """Test keywords_str property with keywords."""
        contexts = EntityContexts(contexts=[], keywords=["test", "example"])
        assert contexts.keywords_str == "test, example"
    
    def test_keywords_str_empty(self):
        """Test keywords_str property with no keywords."""
        contexts = EntityContexts(contexts=[], keywords=[])
        assert contexts.keywords_str == ""
    
    def test_all_context_strs_with_keywords_and_entities(self):
        """Test all_context_strs property with both keywords and entities."""
        entity = Entity(entityId="e1", value="Python", classification="Language")
        scored = ScoredEntity(entity=entity, score=0.9)
        context = EntityContext(entities=[scored])
        
        contexts = EntityContexts(contexts=[context], keywords=["test"])
        
        assert contexts.all_context_strs == ["test", "python"]
    
    def test_all_context_strs_without_keywords(self):
        """Test all_context_strs property without keywords."""
        entity = Entity(entityId="e1", value="Python", classification="Language")
        scored = ScoredEntity(entity=entity, score=0.9)
        context = EntityContext(entities=[scored])
        
        contexts = EntityContexts(contexts=[context], keywords=[])
        
        assert contexts.all_context_strs == ["python"]


class TestSearchResultCollection:
    """Tests for SearchResultCollection model."""
    
    def test_add_search_result(self):
        """Test adding a search result to the collection."""
        versioning = Versioning()
        source = Source(sourceId="s1", metadata={}, versioning=versioning)
        result = SearchResult(source=source)
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[], entity_contexts=entity_contexts)
        
        collection.add_search_result(result)
        
        assert len(collection.results) == 1
        assert collection.results[0] == result
    
    def test_with_new_results(self):
        """Test replacing results with new results."""
        versioning = Versioning()
        source1 = Source(sourceId="s1", metadata={}, versioning=versioning)
        source2 = Source(sourceId="s2", metadata={}, versioning=versioning)
        result1 = SearchResult(source=source1)
        result2 = SearchResult(source=source2)
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[result1], entity_contexts=entity_contexts)
        
        new_collection = collection.with_new_results([result2])
        
        assert len(new_collection.results) == 1
        assert new_collection.results[0] == result2
        assert new_collection is collection  # Returns same instance
