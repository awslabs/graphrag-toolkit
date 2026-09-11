# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import S3DocDownloader


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


@pytest.fixture
def chunk_node():
    '''
    Factory for a chunk node belonging to a source document.

    The SOURCE relationship is what SourceDocument.source_id() reads, so a node
    built without it has no document identity.
    '''
    def _chunk_node(node_id, source_id):
        node = TextNode(text=f'text for {node_id}', id_=node_id)
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
        return node

    return _chunk_node


@pytest.fixture
def download_source_prefix():
    '''
    Factory that reads one source prefix through S3DocDownloader against a mock
    client, given a mapping of object key to the nodes that object holds.

    Every object under the prefix merges into one SourceDocument, so this is how
    a test observes what a prefix reads back as.
    '''
    def _download_source_prefix(objects):
        downloader = S3DocDownloader(
            key_prefix='p', collection_id='c', bucket_name='b', fn=lambda n: n
        )
        s3_client = Mock()
        s3_client.get_paginator.return_value.paginate.return_value = [
            {'Contents': [{'Key': key} for key in objects]}
        ]

        def download_fileobj(bucket, key, stream):
            stream.write('\n'.join(n.to_json() for n in objects[key]).encode('UTF-8'))

        s3_client.download_fileobj.side_effect = download_fileobj
        return downloader._download_doc('prefix', s3_client)

    return _download_source_prefix
# --- Ontology fixtures -------------------------------------------------------
#
# The .ttl files and the verification corpus live under tests/fixtures/ and are
# reached through these fixtures rather than through paths built in each test,
# so moving the directory is a one-line change. Declared here rather than in a
# leaf conftest because tests across indexing/extract, indexing/build and the
# model tests all read the same ontology.

@pytest.fixture
def fixtures_dir():
    '''
    Fixture for the tests/fixtures directory.
    '''
    return Path(__file__).parent.parent / 'fixtures'


@pytest.fixture
def ontology_fixtures_dir(fixtures_dir):
    '''
    Fixture for the directory holding the test ontology .ttl files.
    '''
    return fixtures_dir / 'ontologies'


@pytest.fixture
def company_ttl_path(ontology_fixtures_dir):
    '''
    Fixture for the path to the main test ontology.
    '''
    return ontology_fixtures_dir / 'company.ttl'


@pytest.fixture
def company_ontology(company_ttl_path):
    '''
    Fixture for the loaded main test ontology. Imported lazily so that tests
    which do not touch the ontology do not pay for importing rdflib.
    '''
    from graphrag_toolkit.lexical_graph.indexing.extract.ontology import Ontology
    return Ontology.from_turtle(company_ttl_path)


@pytest.fixture
def upper_snake_ttl_path(ontology_fixtures_dir):
    '''
    Fixture for the path to the UPPER_SNAKE-authored test ontology.
    '''
    return ontology_fixtures_dir / 'upper_snake_names.ttl'


@pytest.fixture
def upper_snake_ontology(upper_snake_ttl_path):
    '''
    Fixture for the loaded UPPER_SNAKE-authored test ontology, which declares
    :WORKS_FOR and :SPORTS_TEAM where company.ttl declares :worksFor and
    :SportsTeam.
    '''
    from graphrag_toolkit.lexical_graph.indexing.extract.ontology import Ontology
    return Ontology.from_turtle(upper_snake_ttl_path)


@pytest.fixture
def malformed_ttl_path(ontology_fixtures_dir):
    '''
    Fixture returning a function that resolves a malformed ontology fixture by
    name, e.g. malformed_ttl_path('subclass_cycle').
    '''
    def resolve(name):
        return ontology_fixtures_dir / 'malformed' / f'{name}.ttl'
    return resolve
