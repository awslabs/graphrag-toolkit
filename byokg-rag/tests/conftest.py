"""Shared pytest fixtures for byokg-rag tests.

This module provides reusable test fixtures for mocking AWS services,
graph stores, LLM clients, and test data structures.
"""

import pytest
from unittest.mock import Mock


@pytest.fixture
def mock_bedrock_generator():
    """
    Fixture providing a mock BedrockGenerator with deterministic responses.
    
    Returns a mock that simulates LLM generation without AWS API calls.
    """
    mock_gen = Mock()
    mock_gen.generate.return_value = "Mock LLM response"
    mock_gen.model_name = "mock-model"
    mock_gen.region_name = "us-west-2"
    return mock_gen


@pytest.fixture
def mock_graph_store():
    """
    Fixture providing a mock graph store with sample data.
    
    Returns a mock graph store that provides schema and node data
    without requiring a real graph database connection.
    """
    mock_store = Mock()
    mock_store.get_schema.return_value = {
        'node_types': ['Person', 'Organization', 'Location'],
        'edge_types': ['WORKS_FOR', 'LOCATED_IN']
    }
    mock_store.nodes.return_value = ['Organization', 'Portland', 'John Doe']
    return mock_store


@pytest.fixture
def sample_queries():
    """
    Fixture providing sample query strings for testing.
    
    Returns a list of representative queries covering different patterns.
    """
    return [
        "Who founded Organization?",
        "Where is Organization headquartered?",
        "What products does Organization sell?"
    ]


@pytest.fixture
def sample_graph_data():
    """
    Fixture providing sample graph structures for testing.
    
    Returns dictionaries representing nodes, edges, and paths.
    """
    return {
        'nodes': [
            {'id': 'n1', 'label': 'Person', 'name': 'John Smith'},
            {'id': 'n2', 'label': 'Organization', 'name': 'My Organization'},
            {'id': 'n3', 'label': 'Location', 'name': 'Vancouver'}
        ],
        'edges': [
            {'source': 'n1', 'target': 'n2', 'type': 'FOUNDED'},
            {'source': 'n2', 'target': 'n3', 'type': 'LOCATED_IN'}
        ],
        'paths': [
            ['n1', 'FOUNDED', 'n2', 'LOCATED_IN', 'n3']
        ]
    }


@pytest.fixture(autouse=True)
def block_aws_calls(monkeypatch):
    """
    Fixture that blocks all real AWS API calls during tests.
    
    Raises an error if any test attempts to make a real AWS call,
    ensuring tests remain isolated and fast.
    """
    def mock_boto3_client(*args, **kwargs):
        raise RuntimeError(
            "Tests must not make real AWS API calls. "
            "Use mocked clients from conftest.py fixtures."
        )
    
    monkeypatch.setattr('boto3.client', mock_boto3_client)
