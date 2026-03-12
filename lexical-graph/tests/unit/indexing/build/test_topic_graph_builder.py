# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.topic_graph_builder import TopicGraphBuilder


class TestTopicGraphBuilderInitialization:
    """Tests for TopicGraphBuilder initialization."""
    
    def test_initialization(self):
        """Verify TopicGraphBuilder initializes correctly."""
        builder = TopicGraphBuilder()
        assert builder is not None


class TestTopicGraphBuilding:
    """Tests for topic graph building functionality."""
    
    def test_build_topic_node(self, mock_neptune_store):
        """Verify building topic node with metadata."""
        builder = TopicGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'topic_001'
        mock_node.text = 'Artificial Intelligence'
        mock_node.metadata = {
            'topic': {
                'topicId': 'topic_001',
                'name': 'Artificial Intelligence',
                'metadata': {
                    'category': 'Technology',
                    'relevance': 0.95
                }
            }
        }
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='topicId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_topic_with_subtopics(self):
        """Verify building topic with subtopic relationships."""
        builder = TopicGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'topic_002'
        mock_node.text = 'Machine Learning'
        mock_node.metadata = {
            'topic': {
                'topicId': 'topic_002',
                'name': 'Machine Learning',
                'parent_topic': 'topic_001',
                'subtopics': ['topic_003', 'topic_004'],
                'metadata': {}
            }
        }
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='topicId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_multiple_topics(self):
        """Verify building multiple topic nodes."""
        builder = TopicGraphBuilder()
        
        topics = [
            Mock(metadata={'topic': {'topicId': 't1', 'name': 'Topic 1'}}),
            Mock(metadata={'topic': {'topicId': 't2', 'name': 'Topic 2'}}),
            Mock(metadata={'topic': {'topicId': 't3', 'name': 'Topic 3'}})
        ]
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='topicId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        for topic in topics:
            topic.node_id = topic.metadata['topic']['topicId']
            topic.text = topic.metadata['topic']['name']
            topic.relationships = {}
            builder.build(topic, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.call_count >= 3


class TestTopicGraphBuilderErrorHandling:
    """Tests for topic graph builder error handling."""
    
    def test_build_with_missing_topic_id(self):
        """Verify handling of topic with missing ID."""
        builder = TopicGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'topic_001'
        mock_node.metadata = {'topic': {'name': 'Topic without ID'}}
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert not mock_graph_client.execute_query_with_retry.called
    
    def test_build_with_empty_topic_name(self):
        """Verify handling of topic with empty name."""
        builder = TopicGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'topic_001'
        mock_node.text = ''
        mock_node.metadata = {
            'topic': {
                'topicId': 'topic_001',
                'name': '',
                'metadata': {}
            }
        }
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='topicId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert mock_graph_client.execute_query_with_retry.called
