# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ProcessorBase.

This module tests the base processor functionality including result processing,
logging, and helper methods for applying transformations.
"""

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.processors.processor_base import ProcessorBase
from graphrag_toolkit.lexical_graph.retrieval.model import (
    SearchResultCollection, SearchResult, Topic, Statement, Source, Versioning, EntityContexts
)
from llama_index.core.schema import QueryBundle


class ConcreteProcessor(ProcessorBase):
    """Concrete implementation of ProcessorBase for testing."""
    
    def _process_results(self, search_results, query):
        """Simple pass-through implementation."""
        return search_results


@pytest.fixture
def processor_args():
    """Fixture providing ProcessorArgs for testing."""
    return ProcessorArgs(debug_results=[])


@pytest.fixture
def filter_config():
    """Fixture providing FilterConfig for testing."""
    return FilterConfig()


@pytest.fixture
def concrete_processor(processor_args, filter_config):
    """Fixture providing concrete processor instance."""
    return ConcreteProcessor(processor_args, filter_config)


@pytest.fixture
def sample_versioning():
    """Fixture providing sample versioning data."""
    return Versioning(valid_from=0, valid_to=9999999999)


@pytest.fixture
def entity_contexts():
    """Fixture providing empty EntityContexts for SearchResultCollection."""
    return EntityContexts(contexts=[], keywords=[])


class TestProcessorBaseInitialization:
    """Tests for ProcessorBase initialization."""
    
    def test_initialization_with_args(self, processor_args, filter_config):
        """Verify processor initializes with args and filter config."""
        processor = ConcreteProcessor(processor_args, filter_config)
        
        assert processor.args == processor_args
        assert processor.filter_config == filter_config


class TestProcessorBaseHelperMethods:
    """Tests for ProcessorBase helper methods."""
    
    def test_apply_to_search_results(self, concrete_processor, sample_versioning, entity_contexts):
        """Verify _apply_to_search_results applies handler to all results."""
        source1 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        source2 = Source(sourceId='doc2', metadata={}, versioning=sample_versioning)
        
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic])
        result2 = SearchResult(source=source2, topics=[topic])
        
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        
        # Handler that modifies source metadata
        def handler(index, search_result):
            search_result.source.metadata['processed'] = True
            return search_result
        
        processed = concrete_processor._apply_to_search_results(collection, handler)
        
        assert len(processed.results) == 2
        assert all(r.source.metadata.get('processed') for r in processed.results)
    
    def test_apply_to_search_results_filters_none_returns(self, concrete_processor, sample_versioning, entity_contexts):
        """Verify _apply_to_search_results filters out None returns."""
        source1 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        source2 = Source(sourceId='doc2', metadata={}, versioning=sample_versioning)
        
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic])
        result2 = SearchResult(source=source2, topics=[topic])
        
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        
        # Handler that filters out first result
        def handler(index, search_result):
            return search_result if index > 0 else None
        
        processed = concrete_processor._apply_to_search_results(collection, handler)
        
        assert len(processed.results) == 1
        assert processed.results[0].source.sourceId == 'doc2'
    
    def test_apply_to_search_results_filters_empty_topics(self, concrete_processor, sample_versioning, entity_contexts):
        """Verify _apply_to_search_results filters results without topics."""
        source = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        # Result with empty topics
        result = SearchResult(source=source, topics=[])
        collection = SearchResultCollection(results=[result], entity_contexts=entity_contexts)
        
        def handler(index, search_result):
            return search_result
        
        processed = concrete_processor._apply_to_search_results(collection, handler)
        
        # Should be filtered out
        assert len(processed.results) == 0
    
    def test_apply_to_topics(self, concrete_processor, sample_versioning):
        """Verify _apply_to_topics applies handler to all topics."""
        source = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        topic1 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        topic2 = Topic(topic='Topic B', topicId='t2', statements=[
            Statement(statement='Statement 2', score=0.8)
        ])
        
        result = SearchResult(source=source, topics=[topic1, topic2])
        
        # Handler that modifies topic
        def handler(topic):
            topic.topic = topic.topic + ' (processed)'
            return topic
        
        processed_result = concrete_processor._apply_to_topics(result, handler)
        
        assert len(processed_result.topics) == 2
        assert all('(processed)' in t.topic for t in processed_result.topics)
    
    def test_apply_to_topics_filters_empty_statements(self, concrete_processor, sample_versioning):
        """Verify _apply_to_topics filters topics without statements."""
        source = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        topic1 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        topic2 = Topic(topic='Topic B', topicId='t2', statements=[])
        
        result = SearchResult(source=source, topics=[topic1, topic2])
        
        def handler(topic):
            return topic
        
        processed_result = concrete_processor._apply_to_topics(result, handler)
        
        # Only topic with statements should remain
        assert len(processed_result.topics) == 1
        assert processed_result.topics[0].topic == 'Topic A'
    
    def test_format_statement_context(self, concrete_processor):
        """Verify _format_statement_context formats correctly."""
        formatted = concrete_processor._format_statement_context(
            source_str='Source 1',
            topic_str='Technology',
            statement_str='GraphRAG is powerful'
        )
        
        assert formatted == 'Technology: GraphRAG is powerful'
        assert 'Technology' in formatted
        assert 'GraphRAG is powerful' in formatted


class TestProcessorBaseProcessResults:
    """Tests for process_results method."""
    
    @patch('graphrag_toolkit.lexical_graph.retrieval.processors.processor_base.logger')
    def test_process_results_calls_abstract_method(self, mock_logger, concrete_processor, sample_versioning, entity_contexts):
        """Verify process_results calls _process_results."""
        source = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        result = SearchResult(source=source, topics=[topic])
        collection = SearchResultCollection(results=[result], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = concrete_processor.process_results(collection, query, 'test_retriever')
        
        assert processed is not None
        assert len(processed.results) == 1
    
    @patch('graphrag_toolkit.lexical_graph.retrieval.processors.processor_base.logger')
    def test_process_results_logs_counts(self, mock_logger, concrete_processor, sample_versioning, entity_contexts):
        """Verify process_results logs before and after counts."""
        source = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        result = SearchResult(source=source, topics=[topic])
        collection = SearchResultCollection(results=[result], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        concrete_processor.process_results(collection, query, 'test_retriever')
        
        # Should log before and after
        assert mock_logger.debug.call_count >= 2


class TestProcessorBaseAbstractMethod:
    """Tests for abstract method enforcement."""
    
    def test_abstract_method_must_be_implemented(self, processor_args, filter_config):
        """Verify _process_results raises NotImplementedError if not overridden."""
        # ProcessorBase can be instantiated but calling _process_results raises NotImplementedError
        processor = ProcessorBase(processor_args, filter_config)
        
        entity_contexts = EntityContexts(contexts=[], keywords=[])
        collection = SearchResultCollection(results=[], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        with pytest.raises(NotImplementedError):
            processor._process_results(collection, query)
