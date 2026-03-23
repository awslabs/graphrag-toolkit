# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for DedupResults processor.

This module tests result deduplication functionality including merging
duplicate results, combining topics, and aggregating scores.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.processors.dedup_results import DedupResults
from graphrag_toolkit.lexical_graph.retrieval.model import (
    SearchResultCollection, SearchResult, Topic, Statement, Source, Versioning, Chunk, EntityContexts
)
from llama_index.core.schema import QueryBundle


@pytest.fixture
def processor_args():
    """Fixture providing ProcessorArgs for testing."""
    return ProcessorArgs(debug_results=[])


@pytest.fixture
def filter_config():
    """Fixture providing FilterConfig for testing."""
    return FilterConfig()


@pytest.fixture
def dedup_processor(processor_args, filter_config):
    """Fixture providing DedupResults processor instance."""
    return DedupResults(processor_args, filter_config)


@pytest.fixture
def sample_versioning():
    """Fixture providing sample versioning data."""
    return Versioning(valid_from=0, valid_to=9999999999)


@pytest.fixture
def entity_contexts():
    """Fixture providing empty EntityContexts for SearchResultCollection."""
    return EntityContexts(contexts=[], keywords=[])


class TestDedupResultsInitialization:
    """Tests for DedupResults initialization."""
    
    def test_initialization_with_args(self, processor_args, filter_config):
        """Verify processor initializes with args and filter config."""
        processor = DedupResults(processor_args, filter_config)
        
        assert processor.args == processor_args
        assert processor.filter_config == filter_config


class TestDedupResultsProcessing:
    """Tests for deduplication processing."""
    
    def test_process_results_no_duplicates(self, dedup_processor, sample_versioning, entity_contexts):
        """Verify processing preserves results with no duplicates."""
        source1 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        source2 = Source(sourceId='doc2', metadata={}, versioning=sample_versioning)
        
        topic1 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        topic2 = Topic(topic='Topic B', topicId='t2', statements=[
            Statement(statement='Statement 2', score=0.8)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic1])
        result2 = SearchResult(source=source2, topics=[topic2])
        
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = dedup_processor.process_results(collection, query, 'test_retriever')
        
        assert len(processed.results) == 2
        assert processed.results[0].source.sourceId == 'doc1'
        assert processed.results[1].source.sourceId == 'doc2'
    
    def test_process_results_with_duplicate_sources(self, dedup_processor, sample_versioning, entity_contexts):
        """Verify duplicate sources are merged."""
        source1 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        source2 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        topic1 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        topic2 = Topic(topic='Topic B', topicId='t2', statements=[
            Statement(statement='Statement 2', score=0.8)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic1])
        result2 = SearchResult(source=source2, topics=[topic2])
        
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = dedup_processor.process_results(collection, query, 'test_retriever')
        
        # Should merge into single result
        assert len(processed.results) == 1
        assert processed.results[0].source.sourceId == 'doc1'
        # Should have both topics
        assert len(processed.results[0].topics) == 2
    
    def test_process_results_merges_duplicate_topics(self, dedup_processor, sample_versioning, entity_contexts):
        """Verify duplicate topics within same source are merged."""
        source1 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        source2 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        topic1 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        topic2 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Statement 2', score=0.8)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic1])
        result2 = SearchResult(source=source2, topics=[topic2])
        
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = dedup_processor.process_results(collection, query, 'test_retriever')
        
        # Should merge into single result with single topic
        assert len(processed.results) == 1
        assert len(processed.results[0].topics) == 1
        # Should have both statements
        assert len(processed.results[0].topics[0].statements) == 2
    
    def test_process_results_aggregates_duplicate_statements(self, dedup_processor, sample_versioning, entity_contexts):
        """Verify duplicate statements have scores aggregated."""
        source1 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        source2 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        topic1 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Same statement', score=0.9, retrievers=['retriever1'])
        ])
        topic2 = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Same statement', score=0.8, retrievers=['retriever2'])
        ])
        
        result1 = SearchResult(source=source1, topics=[topic1])
        result2 = SearchResult(source=source2, topics=[topic2])
        
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = dedup_processor.process_results(collection, query, 'test_retriever')
        
        # Should have single statement with aggregated score
        assert len(processed.results[0].topics[0].statements) == 1
        statement = processed.results[0].topics[0].statements[0]
        assert abs(statement.score - 1.7) < 0.0001  # 0.9 + 0.8 (with floating point tolerance)
        assert 'retriever1' in statement.retrievers
        assert 'retriever2' in statement.retrievers
    
    def test_process_results_merges_chunks(self, dedup_processor, sample_versioning, entity_contexts):
        """Verify chunks from duplicate topics are merged."""
        source1 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        source2 = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        chunk1 = Chunk(chunkId='chunk1', value='Chunk 1 text', score=0.9)
        chunk2 = Chunk(chunkId='chunk2', value='Chunk 2 text', score=0.8)
        
        topic1 = Topic(topic='Topic A', topicId='t1', chunks=[chunk1], statements=[
            Statement(statement='Statement 1', score=0.9)
        ])
        topic2 = Topic(topic='Topic A', topicId='t1', chunks=[chunk2], statements=[
            Statement(statement='Statement 2', score=0.8)
        ])
        
        result1 = SearchResult(source=source1, topics=[topic1])
        result2 = SearchResult(source=source2, topics=[topic2])
        
        collection = SearchResultCollection(results=[result1, result2], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = dedup_processor.process_results(collection, query, 'test_retriever')
        
        # Should have both chunks
        assert len(processed.results[0].topics[0].chunks) == 2
        chunk_ids = [c.chunkId for c in processed.results[0].topics[0].chunks]
        assert 'chunk1' in chunk_ids
        assert 'chunk2' in chunk_ids
    
    def test_process_results_sorts_statements_by_score(self, dedup_processor, sample_versioning, entity_contexts):
        """Verify statements are sorted by score in descending order."""
        source = Source(sourceId='doc1', metadata={}, versioning=sample_versioning)
        
        topic = Topic(topic='Topic A', topicId='t1', statements=[
            Statement(statement='Low score', score=0.5),
            Statement(statement='High score', score=0.9),
            Statement(statement='Medium score', score=0.7)
        ])
        
        result = SearchResult(source=source, topics=[topic])
        collection = SearchResultCollection(results=[result], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = dedup_processor.process_results(collection, query, 'test_retriever')
        
        statements = processed.results[0].topics[0].statements
        assert statements[0].statement == 'High score'
        assert statements[1].statement == 'Medium score'
        assert statements[2].statement == 'Low score'
    
    def test_process_results_empty_collection(self, dedup_processor, entity_contexts):
        """Verify processing empty collection returns empty collection."""
        collection = SearchResultCollection(results=[], entity_contexts=entity_contexts)
        query = QueryBundle(query_str='test query')
        
        processed = dedup_processor.process_results(collection, query, 'test_retriever')
        
        assert len(processed.results) == 0
