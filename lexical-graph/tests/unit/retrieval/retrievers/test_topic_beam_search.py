# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the (non-deprecated) traversal-based TopicBeamSearch retriever."""

from unittest.mock import MagicMock

from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.retrievers.topic_beam_search import TopicBeamSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import (
    TraversalBasedBaseRetriever,
)


def _make(graph_store=None, vector_store=None, **kwargs):
    return TopicBeamSearch(
        graph_store=graph_store or MagicMock(),
        vector_store=vector_store or MagicMock(),
        **kwargs,
    )


def test_is_traversal_based_retriever():
    assert issubclass(TopicBeamSearch, TraversalBasedBaseRetriever)


def test_defaults_sourced_from_processor_args():
    r = _make()
    # neighbour defaults: same-chunk + adjacent-chunk on, entity off (sourced from ProcessorArgs)
    assert r.use_same_chunk_neighbors is True
    assert r.use_adjacent_chunk_neighbors is True   # validated win, now default-on
    assert r.use_entity_neighbors is False
    assert r.max_entity_neighbors == 100            # entity fan-out cap
    assert r.scoring_mode == 'path_weighted'
    assert (r.top_k, r.beam_width, r.max_depth) == (50, 100, 6)


def test_processor_args_override_and_constructor_override():
    from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
    # values flow from ProcessorArgs ...
    r = _make(processor_args=ProcessorArgs(use_adjacent_chunk_neighbors=False, max_entity_neighbors=25, topic_top_k=8))
    assert r.use_adjacent_chunk_neighbors is False
    assert r.max_entity_neighbors == 25
    assert r.top_k == 8
    # ... and an explicit constructor argument overrides ProcessorArgs
    r2 = _make(processor_args=ProcessorArgs(use_entity_neighbors=False), use_entity_neighbors=True)
    assert r2.use_entity_neighbors is True


def test_init_is_noop_skips_entity_context():
    # _init must not invoke keyword/entity providers (latency-sensitive path).
    r = _make()
    r.entity_contexts = MagicMock()
    r.entity_contexts.keywords = []
    r._init(QueryBundle(query_str='q'))
    r.entity_contexts.contexts.extend.assert_not_called()


def test_get_start_node_ids_returns_ids_and_caches_embeddings():
    vs = MagicMock()
    topic_index = MagicMock()
    vs.get_index.return_value = topic_index
    n1 = MagicMock(metadata={'topic': {'topicId': 't1'}}, embedding=[0.1, 0.2])
    n2 = MagicMock(metadata={'topic': {'topicId': 't2'}}, embedding=[0.3, 0.4])
    topic_index.client.query.return_value = MagicMock(nodes=[n1, n2])

    r = _make(vector_store=vs)
    qb = QueryBundle(query_str='q')
    qb.embedding = [0.5, 0.6]  # pre-set so to_embedded_query is a no-op

    ids = r.get_start_node_ids(qb)
    assert ids == ['t1', 't2']
    assert 't1' in r._topic_cache and 't2' in r._topic_cache  # stored seed embeddings cached


def test_get_neighbors_batch_builds_map():
    gs = MagicMock()
    gs.node_id.side_effect = lambda x: x
    gs.execute_query.return_value = [{'sourceId': 't1', 'neighborIds': ['t2', 't3', 't2']}]
    r = _make(graph_store=gs)
    result = r.get_neighbors_batch(['t1'])
    assert set(result['t1']) == {'t2', 't3'}  # de-duplicated


def test_expand_topics_caps_statements_per_topic():
    gs = MagicMock()
    gs.node_id.side_effect = lambda x: x
    # 5 statements for one topic; cap should limit to max_statements_per_topic
    gs.execute_query.return_value = [
        {'topicId': 't1', 'statementId': f's{i}'} for i in range(5)
    ]
    r = _make(graph_store=gs, max_statements_per_topic=3)
    statement_ids = r._expand_topics_to_statement_ids(['t1'])
    assert statement_ids == ['s0', 's1', 's2']


def test_do_graph_search_empty_seeds_returns_empty_collection():
    r = _make()
    out = r.do_graph_search(QueryBundle(query_str='q'), [])
    assert out.results == []


def test_neighbour_embeddings_fetched_from_stored_index_not_recomputed():
    # Neighbour topic embeddings are read from the stored topic index via the VectorIndex
    # abstraction (get_embeddings); nothing is recomputed on the fly.
    gs = MagicMock()
    vs = MagicMock()
    ti = MagicMock()
    vs.get_index.return_value = ti
    ti.get_embeddings.return_value = [
        {'id': 't1', 'embedding': [0.1, 0.2]},
        {'id': 't2', 'embedding': [0.3, 0.4]},
    ]
    r = _make(graph_store=gs, vector_store=vs)
    out = r._get_embeddings(['t1', 't2'])
    assert set(out.keys()) == {'t1', 't2'}
    ti.get_embeddings.assert_called_once()            # used the abstraction
    gs.execute_query.assert_not_called()              # no on-the-fly graph lookup
    ti.embed_model.get_text_embedding_batch.assert_not_called()  # no on-the-fly embedding


def test_neighbours_without_stored_embedding_are_skipped():
    # A neighbour with no stored vector in the index is skipped (not recomputed).
    gs = MagicMock()
    vs = MagicMock()
    ti = MagicMock()
    vs.get_index.return_value = ti
    ti.get_embeddings.return_value = [{'id': 't1', 'embedding': [0.1, 0.2]}]
    r = _make(graph_store=gs, vector_store=vs)
    out = r._get_embeddings(['t1', 't2'])   # t2 has no stored embedding
    assert 't1' in out and 't2' not in out
    gs.execute_query.assert_not_called()


def test_get_embeddings_handles_both_backend_id_shapes():
    # The id key differs by vector backend: OpenSearch/AOSS returns a top-level 'id';
    # Neptune Analytics returns {'embedding':..., 'topic': {'topicId':...}} with no 'id'.
    # _get_embeddings must resolve the topic id from either shape.
    vs = MagicMock()
    ti = MagicMock()
    vs.get_index.return_value = ti
    ti.get_embeddings.return_value = [
        {'id': 'aoss1', 'embedding': [0.1, 0.2]},                       # AOSS shape
        {'topic': {'topicId': 'neptune1'}, 'embedding': [0.3, 0.4]},    # Neptune Analytics shape
    ]
    r = _make(vector_store=vs)
    out = r._get_embeddings(['aoss1', 'neptune1'])
    assert set(out.keys()) == {'aoss1', 'neptune1'}  # both backends resolved


def test_get_start_node_ids_clears_cache_each_query():
    # The retriever instance is reused across queries; the per-query topic-embedding cache
    # must be reset on each call so it cannot grow without bound.
    vs = MagicMock()
    topic_index = MagicMock()
    vs.get_index.return_value = topic_index
    n1 = MagicMock(metadata={'topic': {'topicId': 't1'}}, embedding=[0.1, 0.2])
    topic_index.client.query.return_value = MagicMock(nodes=[n1])

    r = _make(vector_store=vs)
    r._topic_cache['stale'] = 'leftover-from-previous-query'
    qb = QueryBundle(query_str='q')
    qb.embedding = [0.5, 0.6]

    r.get_start_node_ids(qb)
    assert 'stale' not in r._topic_cache       # previous-query entry evicted
    assert 't1' in r._topic_cache              # fresh seed cached


def test_neighbor_query_caps_chunk_and_adjacent_neighbours():
    # Entity neighbours were already capped; chunk + adjacent-chunk neighbours must be too.
    # Use distinct caps so the chunk cap is unambiguously applied to both chunk strategies.
    gs = MagicMock()
    gs.node_id.side_effect = lambda x: x
    gs.execute_query.return_value = []
    r = _make(graph_store=gs, max_entity_neighbors=100, max_chunk_neighbors=50)
    r.use_entity_neighbors = True
    r.use_same_chunk_neighbors = True
    r.use_adjacent_chunk_neighbors = True

    r.get_neighbors_batch(['t1'])
    cypher = gs.execute_query.call_args[0][0]
    assert '[..100]' in cypher            # entity cap (pre-existing)
    assert cypher.count('[..50]') == 2    # same-chunk + adjacent-chunk caps (the fix)
