# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for FilterByMetadata processor.

This module tests metadata-based filtering of search results including
filtering by source metadata and versioning information.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.processors.filter_by_metadata import FilterByMetadata
from graphrag_toolkit.lexical_graph.retrieval.model import (
    SearchResultCollection, SearchResult, Topic, Statement, Source, Versioning, EntityContexts
)
from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, VALID_TO
from llama_index.core.schema import QueryBundle


@pytest.fixture
def processor_args():
    """Fixture providing ProcessorArgs for testing."""
    return ProcessorArgs(debug_results=[])


@pytest.fixture
def sample_versioning():
    """Fixture providing sample versioning data."""
    return Versioning(valid_from=1000, valid_to=2000)


class TestFilterByMetadataInitialization:
    """Tests for FilterByMetadata initialization."""
    
    def test_initialization_with_args(self, processor_args):
        """Verify processor initializes with args and filter config."""
        filter_config = FilterConfig()
        processor = FilterByMetadata(processor_args, filter_config)
        
        assert processor.args == processor_args
        assert processor.filter_config == filter_config


class TestFilterByMetadataProcessing:
    """Tests for metadata filtering."""
    
    def test_process_results_no_filter(self, processor_args, sample_versioning):
        """Verify all results pass when no filter is configured."""
        filter_config = FilterConfig()
        processor = FilterByMetadata(processor_args, filter_config)
        
        source1 = Source(sourceId='doc1', metadata={'category': 'tech'}, versioning=sample_versioning)
        source2 = Source(sourceId='doc2', metadata={'category': 'science'}, versioning=sample_versioning)
        
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic])
        result2 = SearchResult(source=source2, topics=[topic])
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = processor.process_results(collection, query, 'test_retriever')
        
        # All results should pass
        assert len(processed.results) == 2
    
    def test_process_results_filters_by_metadata(self, processor_args, sample_versioning):
        """Verify results are filtered based on metadata criteria."""
        from llama_index.core.vector_stores import MetadataFilter, FilterOperator
        
        # Create filter that only accepts 'tech' category
        metadata_filter = MetadataFilter(key='category', value='tech', operator=FilterOperator.EQ)
        filter_config = FilterConfig(source_filters=metadata_filter)
        processor = FilterByMetadata(processor_args, filter_config)
        
        source1 = Source(sourceId='doc1', metadata={'category': 'tech'}, versioning=sample_versioning)
        source2 = Source(sourceId='doc2', metadata={'category': 'science'}, versioning=sample_versioning)
        source3 = Source(sourceId='doc3', metadata={'category': 'tech'}, versioning=sample_versioning)
        
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic])
        result2 = SearchResult(source=source2, topics=[topic])
        result3 = SearchResult(source=source3, topics=[topic])
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[result1, result2, result3], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = processor.process_results(collection, query, 'test_retriever')
        
        # Only tech results should pass
        assert len(processed.results) == 2
        assert all(r.source.metadata['category'] == 'tech' for r in processed.results)
    
    def test_process_results_includes_versioning_metadata(self, processor_args):
        """Verify versioning information is included in filter evaluation."""
        # Create filter that checks valid_from
        filter_config = FilterConfig()
        processor = FilterByMetadata(processor_args, filter_config)
        
        versioning1 = Versioning(valid_from=1000, valid_to=2000)
        versioning2 = Versioning(valid_from=3000, valid_to=4000)
        
        source1 = Source(sourceId='doc1', metadata={}, versioning=versioning1)
        source2 = Source(sourceId='doc2', metadata={}, versioning=versioning2)
        
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic])
        result2 = SearchResult(source=source2, topics=[topic])
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        # Process without specific filter - all should pass
        processed = processor.process_results(collection, query, 'test_retriever')
        
        assert len(processed.results) == 2
    
    def test_process_results_empty_collection(self, processor_args):
        """Verify processing empty collection returns empty collection."""
        filter_config = FilterConfig()
        processor = FilterByMetadata(processor_args, filter_config)
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = processor.process_results(collection, query, 'test_retriever')
        
        assert len(processed.results) == 0
    
    def test_process_results_removes_results_without_topics(self, processor_args, sample_versioning):
        """Verify results without topics or statements are removed."""
        filter_config = FilterConfig()
        processor = FilterByMetadata(processor_args, filter_config)
        
        source = Source(sourceId='doc1', metadata={'category': 'tech'}, versioning=sample_versioning)
        
        # Result with empty topics
        result = SearchResult(source=source, topics=[])
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[result], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = processor.process_results(collection, query, 'test_retriever')
        
        # Result should be filtered out due to no topics
        assert len(processed.results) == 0
