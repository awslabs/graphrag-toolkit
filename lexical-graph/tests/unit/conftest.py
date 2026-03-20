# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator


@pytest.fixture
def mock_neptune_store():
    '''
    Fixture for a mock Neptune graph store.
    GraphStore is a pydantic BaseModel, so we patch validation to allow a Mock.
    '''
    with patch('graphrag_toolkit.lexical_graph.storage.graph.graph_store.GraphStore.__init__', return_value=None):
        from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
        store = MagicMock(spec=GraphStore)
        store.node_id = Mock(side_effect=lambda field: f'params.{field}')
        store.execute_query_with_retry = Mock(return_value=[])
        store.tenant_id = TenantId()
        return store


@pytest.fixture
def sample_documents():
    '''
    Fixture for sample source documents used in pipeline tests.
    '''
    docs = []
    for i in range(3):
        doc = Mock()
        doc.doc_id = f'doc_{i}'
        doc.text = f'Sample document text {i}'
        doc.metadata = {'title': f'Document {i}'}
        docs.append(doc)
    return docs


@pytest.fixture
def default_tenant():
    '''
    Fixture for default tenant ID.
    '''
    return TenantId()


@pytest.fixture
def custom_tenant():
    '''
    Fixture for custom tenant ID.
    '''
    return TenantId("acme")


@pytest.fixture
def default_id_gen(default_tenant):
    '''
    Fixture for default ID generator (backward compatible mode, no delimiter).
    '''
    return IdGenerator(tenant_id=default_tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=False)


@pytest.fixture
def default_id_gen_with_delimiter(default_tenant):
    '''
    Fixture for ID generator with delimiter enabled (collision-resistant mode).
    '''
    return IdGenerator(tenant_id=default_tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=True)


@pytest.fixture
def custom_id_gen(custom_tenant):
    '''
    Fixture for custom ID generator (backward compatible mode, no delimiter).
    '''
    return IdGenerator(tenant_id=custom_tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=False)
