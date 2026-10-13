# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import uuid

import pytest
from llama_index.core.embeddings import MockEmbedding
from llama_index.core.llms import MockLLM
from llama_index.core.schema import Document

from graphrag_toolkit.lexical_graph import GraphRAGConfig, LexicalGraphIndex, LexicalGraphQueryEngine, TenantId
from graphrag_toolkit.lexical_graph.lexical_graph_index import BuildConfig, ExtractionConfig, IndexingConfig
from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_based_search import ChunkBasedSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.entity_based_search import EntityBasedSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.entity_network_search import EntityNetworkSearch
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import tenant_graph_iri


class ExtractionLLM(MockLLM):
    def predict(self, prompt, **kwargs):
        return '''topic: People
entities:
Alice|Person
Bob|Person
proposition: Alice works with Bob.
Alice|WORKS_WITH|Bob'''


class AnswerLLM(MockLLM):
    def predict(self, prompt, **kwargs):
        assert 'Alice works with Bob.' in kwargs['search_results']
        return 'Alice works with Bob.'


@pytest.fixture
def vector_store(request):
    connection = os.environ.get('PGVECTOR_TEST_CONNECTION')
    if not connection:
        pytest.skip('Set PGVECTOR_TEST_CONNECTION to run the ingestion/query pipeline test')
    import psycopg2
    from psycopg2 import sql
    from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes import PGIndex

    schema = f'sparql_test_{uuid.uuid4().hex}'
    with psycopg2.connect(connection) as database:
        with database.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    try:
        yield VectorStore(indexes={
            request.param: PGIndex.for_index(
                request.param, connection, schema_name=schema,
                embed_model=MockEmbedding(embed_dim=8), dimensions=8,
            ),
        })
    finally:
        with psycopg2.connect(connection) as database:
            with database.cursor() as cursor:
                cursor.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


@pytest.mark.parametrize('batch_writes', [False, True])
@pytest.mark.parametrize('retriever, vector_store', [
    (ChunkBasedSearch, 'chunk'),
    (EntityBasedSearch, 'chunk'),
    (EntityNetworkSearch, 'chunk'),
    (EntityNetworkSearch, 'topic'),
], indirect=['vector_store'])
def test_document_to_graph_vectors_and_answer(
    graph_store, vector_store, monkeypatch, tmp_path, batch_writes, retriever,
):
    for attribute, value in {
        '_extraction_llm': ExtractionLLM(),
        '_response_llm': AnswerLLM(),
        '_embed_model': MockEmbedding(embed_dim=8),
        '_embed_dimensions': 8,
        '_enable_cache': False,
        '_extraction_num_workers': 1,
        '_build_num_workers': 1,
    }.items():
        monkeypatch.setattr(GraphRAGConfig, attribute, value)
    index = LexicalGraphIndex(
        graph_store, vector_store, extraction_dir=str(tmp_path),
        indexing_config=IndexingConfig(
            extraction=ExtractionConfig(enable_proposition_extraction=False, extraction_llm=ExtractionLLM()),
            build=BuildConfig(enable_versioning=False),
        ),
    )
    document = Document(text='Alice works with Bob.', metadata={'category': 'people'})
    index.extract_and_build(
        [document], num_workers=1, batch_size=8,
        batch_writes_enabled=batch_writes, batch_write_size=1,
    )
    namespace = graph_store.namespace
    counts = graph_store.client.query(f'''{namespace.sparql_prefixes()}
        SELECT ?type (COUNT(?node) AS ?count) WHERE {{
            ?node a ?type .
        }} GROUP BY ?type''', default_graph=tenant_graph_iri(str(TenantId()), namespace)[1:-1])
    counts = {row['type'].split('#')[-1]: row['count'] for row in counts}
    assert counts.items() >= {'Source': 1, 'Chunk': 1, 'Topic': 1, 'Statement': 1, 'Fact': 1, 'Entity': 2}.items()

    engine = LexicalGraphQueryEngine.for_traversal_based_search(
        graph_store, vector_store, retrievers=[retriever],
        versioning=False, llm=AnswerLLM(), ec_keyword_provider='passthru',
        vss_top_k=1, vss_diversity_factor=0,
    )
    response = engine.query('Who does Alice work with?')
    assert str(response) == 'Alice works with Bob.'
    assert response.source_nodes
