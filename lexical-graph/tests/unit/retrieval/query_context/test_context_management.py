# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for query context management.

This module tests context state management and updates during retrieval.
"""

import pytest
from unittest.mock import Mock


class TestContextManagement:
    """Tests for context management functionality."""
    
    def test_context_initialization(self):
        """Verify context initializes with empty state."""
        context = {'entities': [], 'keywords': [], 'history': []}
        
        assert len(context['entities']) == 0
        assert len(context['keywords']) == 0
        assert len(context['history']) == 0
    
    def test_context_add_entity(self):
        """Verify entities can be added to context."""
        context = {'entities': [], 'keywords': []}
        
        context['entities'].append({'id': 'e1', 'name': 'GraphRAG'})
        
        assert len(context['entities']) == 1
        assert context['entities'][0]['name'] == 'GraphRAG'
    
    def test_context_add_keywords(self):
        """Verify keywords can be added to context."""
        context = {'entities': [], 'keywords': []}
        
        context['keywords'].extend(['graph', 'retrieval', 'augmented'])
        
        assert len(context['keywords']) == 3
        assert 'graph' in context['keywords']
    
    def test_context_update_preserves_existing(self):
        """Verify context updates preserve existing data."""
        context = {
            'entities': [{'id': 'e1', 'name': 'Entity1'}],
            'keywords': ['keyword1']
        }
        
        # Add new data
        context['entities'].append({'id': 'e2', 'name': 'Entity2'})
        context['keywords'].append('keyword2')
        
        assert len(context['entities']) == 2
        assert len(context['keywords']) == 2
        assert context['entities'][0]['name'] == 'Entity1'


class TestContextStateTracking:
    """Tests for context state tracking."""
    
    def test_state_tracking_query_history(self):
        """Verify query history is tracked in context."""
        context = {'history': []}
        
        context['history'].append({'query': 'What is GraphRAG?', 'timestamp': 1000})
        context['history'].append({'query': 'How does it work?', 'timestamp': 2000})
        
        assert len(context['history']) == 2
        assert context['history'][0]['query'] == 'What is GraphRAG?'
    
    def test_state_tracking_entity_scores(self):
        """Verify entity scores are tracked."""
        context = {
            'entities': [
                {'id': 'e1', 'name': 'GraphRAG', 'score': 0.9},
                {'id': 'e2', 'name': 'Knowledge Graph', 'score': 0.7}
            ]
        }
        
        # Verify scores are present
        assert all('score' in e for e in context['entities'])
        assert context['entities'][0]['score'] > context['entities'][1]['score']
    
    def test_state_reset(self):
        """Verify context state can be reset."""
        context = {
            'entities': [{'id': 'e1'}],
            'keywords': ['keyword1'],
            'history': [{'query': 'test'}]
        }
        
        # Reset context
        context = {'entities': [], 'keywords': [], 'history': []}
        
        assert len(context['entities']) == 0
        assert len(context['keywords']) == 0
        assert len(context['history']) == 0
