# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from llama_index.core.schema import NodeRelationship, QueryBundle, RelatedNodeInfo, TextNode
from llama_index.core.vector_stores import MetadataFilter

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.config import SourceIdWidth
from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.graph_batch_client import GraphBatchClient
from graphrag_toolkit.lexical_graph.indexing.build.source_graph_builder import SourceGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.statement_graph_builder import StatementGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.topic_graph_builder import TopicGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.source_id_collision import SourceIdClaims, SourceIdCollisionError
from graphrag_toolkit.lexical_graph.indexing.source_id_width import (
    SourceIdWidthMismatchError,
    read_stored_source_id_width,
    record_graph_source_id_width,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_based_search import ChunkBasedSearch
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import EntityContexts, SearchResultCollection
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.processors.filter_by_metadata import FilterByMetadata
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore
from graphrag_toolkit.lexical_graph.storage.graph import GraphQueryOperation, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, VALID_TO
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import instance_iri, tenant_graph_iri


def source_node(source_id='doc1', title='Original', source_hash=None, metadata=None):
    source = {'sourceId': source_id, 'metadata': {'title': title, **(metadata or {})}}
    if source_hash:
        source['sourceHash'] = source_hash
    return TextNode(metadata={'source': source})


def build_topic(graph_store):
    SourceGraphBuilder().build(source_node(metadata={'category': 'tech'}), graph_store)
    TopicGraphBuilder().build(TextNode(metadata={
        'topic': {'topicId': 't1', 'value': 'People', 'chunkIds': ['c1', 'c2']},
    }), graph_store)
    for number in (1, 2):
        ChunkGraphBuilder().build(TextNode(
            text=f'Chunk {number}',
            metadata={'chunk': {'chunkId': f'c{number}', 'metadata': {'page': number}}},
            relationships={NodeRelationship.SOURCE: RelatedNodeInfo(node_id='doc1')},
        ), graph_store)
        StatementGraphBuilder().build(TextNode(metadata={
            'statement': {
                'statementId': f's{number}', 'value': f'Statement {number}',
                'topicId': 't1', 'chunkId': f'c{number}',
            },
        }), graph_store)


def statements(graph_store, **parameters):
    return graph_store.execute_query(
        'unused native query', {'statementIds': ['s1', 's2'], **parameters},
        operation=GraphQueryOperation.GET_STATEMENTS,
    )


def test_current_builders_and_chunk_search_roundtrip(graph_store):
    build_topic(graph_store)
    retriever = ChunkBasedSearch(graph_store, VectorStore())
    assert set(retriever.chunk_based_graph_search('c1')) == {'s1', 's2'}
    assert InGraphChunkStore(graph_store).get_batch(['c1', 'c2']) == {
        'c1': 'Chunk 1', 'c2': 'Chunk 2',
    }


@pytest.mark.parametrize('batch_enabled', [False, True])
def test_batched_chunk_writes_preserve_text_and_metadata(graph_store, batch_enabled):
    build_topic(graph_store)
    node = TextNode(text='Quotes " \' and č š ž\nsecond line', metadata={
        'chunk': {'chunkId': 'c1', 'metadata': {'page': 3, '_custom': 'value'}},
    }, relationships={NodeRelationship.SOURCE: RelatedNodeInfo(node_id='doc1')})
    for _ in range(2):
        with GraphBatchClient(graph_store, batch_writes_enabled=batch_enabled, batch_write_size=1) as batch:
            ChunkGraphBuilder().build(node, batch)
            batch.apply_batch_operations()
    assert InGraphChunkStore(graph_store).get_batch(['c1']) == {'c1': node.text}
    chunks = statements(graph_store, includeChunkMetadata=True)[0]['result']['topics'][0]['chunks']
    metadata = next(chunk['metadata'] for chunk in chunks if chunk['chunkId'] == 'c1')
    assert metadata['page'] == 3
    assert metadata['_custom'] == 'value'


def test_missing_chunk_text_is_not_replaced_with_empty_text(graph_store):
    TopicGraphBuilder().build(TextNode(metadata={
        'topic': {'topicId': 't1', 'value': 'Topic', 'chunkIds': ['c1']},
    }), graph_store)
    assert InGraphChunkStore(graph_store).get_batch(['c1']) == {}
    InGraphChunkStore(graph_store).put('c1', '')
    assert InGraphChunkStore(graph_store).get_batch(['c1']) == {'c1': ''}


@pytest.mark.parametrize('include_metadata', [False, True])
def test_chunk_metadata_hint_is_honored(graph_store, include_metadata):
    build_topic(graph_store)
    chunks = statements(graph_store, includeChunkMetadata=include_metadata)[0]['result']['topics'][0]['chunks']
    assert all(bool(chunk['metadata']) == include_metadata for chunk in chunks)


def test_source_metadata_keys_and_versioning_roundtrip(graph_store):
    build_topic(graph_store)
    metadata = {'doc-type': 'a', 'doc_type': 'b', '_custom': 'c', 'id': 'external-id'}
    source = source_node(metadata=metadata)
    source.metadata['source']['versioning'] = {
        'valid_from': 10, 'valid_to': 20, 'extract_timestamp': 11,
        'build_timestamp': 12, 'id_fields': ['document', 'revision'],
    }
    SourceGraphBuilder().build(source, graph_store)
    result = statements(graph_store)[0]['result']['source']
    assert result['sourceId'] == 'doc1'
    assert metadata.items() <= result['metadata'].items()
    assert result['metadata'][VALID_FROM] == 10
    assert result['metadata'][VALID_TO] == 20
    assert result['versioning'] == source.metadata['source']['versioning']


def test_source_metadata_can_be_cleared(graph_store):
    build_topic(graph_store)
    SourceGraphBuilder().build(source_node(metadata={'category': None}), graph_store)
    assert 'category' not in statements(graph_store)[0]['result']['source']['metadata']


def test_source_hash_keeps_the_first_owner_and_allows_same_owner_updates(graph_store):
    build_topic(graph_store)
    builder = SourceGraphBuilder()
    for title, digest in [('Original', 'a' * 64), ('Collision', 'b' * 64)]:
        builder.build(source_node(title=title, source_hash=digest), graph_store)
    metadata = statements(graph_store)[0]['result']['source']['metadata']
    assert metadata['title'] == 'Original'
    assert metadata['sourceHash'] == 'a' * 64

    claims = SourceIdClaims()
    claims.add(source_node(source_hash='b' * 64))
    with pytest.raises(SourceIdCollisionError, match='already in the graph'):
        claims.verify(graph_store)

    builder.build(source_node(title='Updated', source_hash='a' * 64), graph_store)
    assert statements(graph_store)[0]['result']['source']['metadata']['title'] == 'Updated'


def test_metadata_cannot_replace_the_source_hash(graph_store):
    build_topic(graph_store)
    SourceGraphBuilder().build(source_node(
        source_hash='a' * 64, metadata={'sourceHash': 'untrusted'},
    ), graph_store)
    assert statements(graph_store)[0]['result']['source']['metadata']['sourceHash'] == 'a' * 64


def test_source_id_width_is_recorded_once_and_tenant_scoped(graph_store):
    assert read_stored_source_id_width(graph_store).written is None
    record_graph_source_id_width(graph_store, TenantId(), SourceIdWidth.FULL)
    assert read_stored_source_id_width(graph_store).recorded == SourceIdWidth.FULL
    with pytest.raises(SourceIdWidthMismatchError):
        record_graph_source_id_width(graph_store, TenantId(), SourceIdWidth.LEGACY)
    other = MultiTenantGraphStore.wrap(graph_store, TenantId('other'))
    assert read_stored_source_id_width(other).written is None
    record_graph_source_id_width(other, TenantId('other'), SourceIdWidth.LEGACY)
    assert read_stored_source_id_width(other).recorded == SourceIdWidth.LEGACY
    assert read_stored_source_id_width(graph_store).recorded == SourceIdWidth.FULL


def test_source_id_width_can_be_sampled_from_an_existing_graph(graph_store):
    SourceGraphBuilder().build(source_node(source_id='aws::5eb63bbb:d41d'), graph_store)
    assert read_stored_source_id_width(graph_store).written == SourceIdWidth.LEGACY


def test_tenants_with_the_same_ids_do_not_share_results(graph_store):
    tenant = MultiTenantGraphStore.wrap(graph_store, TenantId('acme'))
    build_topic(tenant)
    assert statements(graph_store) == []
    assert statements(tenant)[0]['result']['source']['sourceId'] == 'doc1'


def test_values_cannot_inject_sparql(graph_store):
    text = '" . } }; INSERT DATA { <urn:injected> <urn:p> <urn:o> }; #\nč š ž'
    InGraphChunkStore(graph_store).put('c/1> "', text)
    assert InGraphChunkStore(graph_store).get_batch(['c/1> "']) == {'c/1> "': text}
    assert graph_store.client.query('SELECT ?s WHERE { GRAPH ?g { <urn:injected> ?p ?s } }') == []


@pytest.mark.parametrize('category, expected_count', [('tech', 1), ('finance', 0)])
def test_standard_metadata_postprocessor_filters_native_results(graph_store, category, expected_count):
    build_topic(graph_store)
    collection = SearchResultCollection(
        results=[row['result'] for row in statements(graph_store)], entity_contexts=EntityContexts(),
    )
    config = FilterConfig(source_filters=MetadataFilter(key='category', value=category))
    filtered = FilterByMetadata(ProcessorArgs(), config).process_results(collection, QueryBundle('query'), 'test')
    assert len(filtered.results) == expected_count


@pytest.mark.parametrize('batch_enabled', [False, True])
def test_chunk_metadata_text_does_not_overwrite_content(graph_store, batch_enabled):
    node = TextNode(text='Original chunk body', metadata={
        'chunk': {'chunkId': 'c1', 'metadata': {'text': 'Metadata text', 'page': 2}},
    })
    with GraphBatchClient(graph_store, batch_writes_enabled=batch_enabled, batch_write_size=1) as batch:
        ChunkGraphBuilder().build(node, batch)
        batch.apply_batch_operations()
    assert InGraphChunkStore(graph_store).get_batch(['c1']) == {'c1': 'Original chunk body'}


@pytest.mark.parametrize('delete_first', [False, True])
def test_complement_rewrite_preserves_all_incoming_predicates(graph_store, delete_first):
    namespace = graph_store.namespace
    graph = tenant_graph_iri(str(graph_store.tenant_id), namespace)
    other_graph = tenant_graph_iri('other', namespace)
    local = instance_iri('entity', 'local', namespace)
    resolved = instance_iri('entity', 'resolved', namespace)
    graph_store.client.update(f'''{namespace.sparql_prefixes()}
        INSERT DATA {{
            GRAPH {graph} {{
                {local} a lg:Entity ; lg:id "local" .
                <urn:fact> lg:subject {local} ; lg:object {local} ; <urn:custom> {local} .
            }}
            GRAPH {other_graph} {{ <urn:fact> lg:subject {local} }}
        }}''')
    operations = [GraphQueryOperation.COPY_COMPLEMENT_RELATIONSHIPS, GraphQueryOperation.DELETE_COMPLEMENT]
    if delete_first:
        operations.reverse()
    for _ in range(2):
        for operation in operations:
            graph_store.execute_query('', {'params': [{'c_id': 'local', 'n_id': 'resolved'}]}, operation=operation)
    rows = graph_store.client.query('SELECT ?predicate ?target WHERE { <urn:fact> ?predicate ?target }',
                                    default_graph=graph[1:-1])
    assert {(row['predicate'], row['target']) for row in rows} == {
        (namespace.schema_namespace + 'subject', resolved[1:-1]),
        (namespace.schema_namespace + 'object', resolved[1:-1]),
        ('urn:custom', resolved[1:-1]),
    }
    assert graph_store.client.query('SELECT ?target WHERE { <urn:fact> ?predicate ?target }',
                                     default_graph=other_graph[1:-1]) == [{'target': local[1:-1]}]


@pytest.mark.parametrize('missing_label', ['sc', 'oc'])
def test_graph_summary_skips_null_class_labels(graph_store, missing_label):
    row = {'sc_id': 'person', 'oc_id': 'company', 'sc': 'Person', 'oc': 'Company', 'p': 'WORKS_FOR'}
    row[missing_label] = None
    graph_store.execute_query('', {'params': [row]}, operation=GraphQueryOperation.UPDATE_GRAPH_SUMMARY)
    namespace = graph_store.namespace
    graph = tenant_graph_iri(str(graph_store.tenant_id), namespace)
    rows = graph_store.client.query(f'''{namespace.sparql_prefixes()}
        SELECT ?id ?value ?count WHERE {{
            ?node a lg:SysClass ; lg:id ?id ; lg:count ?count .
            OPTIONAL {{ ?node lg:value ?value }}
        }}''', default_graph=graph[1:-1])
    assert {result['id']: result.get('value') for result in rows} == {
        'person': row['sc'], 'company': row['oc'],
    }
    assert all(result['count'] == 1 for result in rows)
