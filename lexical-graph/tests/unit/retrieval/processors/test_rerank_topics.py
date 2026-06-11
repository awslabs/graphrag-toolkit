# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the RerankTopics processor (topic-level reranking)."""

import pytest

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs, RerankTopics
from graphrag_toolkit.lexical_graph.retrieval.model import (
    SearchResultCollection, SearchResult, Topic, Statement, Source, Versioning, EntityContexts,
)
from llama_index.core.schema import QueryBundle


def _collection():
    versioning = Versioning(valid_from=0, valid_to=9999999999)
    source = Source(sourceId='doc1', metadata={}, versioning=versioning)
    # three topics; one clearly matches the query terms, two are unrelated
    topics = [
        Topic(topic='Golf tournament prize money and purse', topicId='t1', statements=[
            Statement(statementId='a', statement='The purse was 1.2 million dollars',
                      statement_str='The purse was 1.2 million dollars', score=0.0)]),
        Topic(topic='Weather and climate patterns', topicId='t2', statements=[
            Statement(statementId='b', statement='It rained heavily that week',
                      statement_str='It rained heavily that week', score=0.0)]),
        Topic(topic='Cooking recipes and ingredients', topicId='t3', statements=[
            Statement(statementId='c', statement='Add two cups of flour',
                      statement_str='Add two cups of flour', score=0.0)]),
    ]
    result = SearchResult(source=source, topics=topics, score=0.9)
    return SearchResultCollection(results=[result], entity_contexts=EntityContexts(contexts=[], keywords=[]))


@pytest.fixture
def query():
    return QueryBundle(query_str='What was the tournament purse prize money?')


def _topic_ids(collection):
    return [t.topicId for r in collection.results for t in r.topics]


def test_noop_when_topic_reranker_none(query):
    proc = RerankTopics(ProcessorArgs(topic_reranker='none', max_topics=1), FilterConfig())
    out = proc._process_results(_collection(), query)
    assert _topic_ids(out) == ['t1', 't2', 't3']  # unchanged


def test_prunes_to_max_topics(query):
    proc = RerankTopics(ProcessorArgs(topic_reranker='tfidf', max_topics=2), FilterConfig())
    out = proc._process_results(_collection(), query)
    ids = _topic_ids(out)
    assert len(ids) <= 2
    assert 't1' in ids  # the purse/prize-money topic is most relevant and must survive


def test_propagates_topic_score_to_unscored_statements(query):
    # statements start at score 0.0; after rerank they inherit the topic relevance score
    proc = RerankTopics(ProcessorArgs(topic_reranker='tfidf', max_topics=3), FilterConfig())
    out = proc._process_results(_collection(), query)
    scores = [s.score for r in out.results for t in r.topics for s in t.statements]
    assert any(sc and sc > 0.0 for sc in scores)
