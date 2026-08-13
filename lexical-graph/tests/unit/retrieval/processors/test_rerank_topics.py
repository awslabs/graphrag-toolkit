# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the RerankTopics processor (topic-level reranking)."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.config import Config

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs, RerankTopics
from graphrag_toolkit.lexical_graph.retrieval.processors import rerank_topics as mod
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


def test_processor_args_preserve_reranking_defaults():
    args = ProcessorArgs()
    assert args.reranker == 'tfidf'
    assert args.topic_reranker == 'none'
    assert args.bedrock_reranker_client_config is None


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


def test_bedrock_uses_configured_session_client_and_preserves_request(query):
    proc = RerankTopics(
        ProcessorArgs(
            topic_reranker='bedrock',
            bedrock_reranker_client_config={
                'connect_timeout': 2,
                'read_timeout': 3,
                'retries': {'total_max_attempts': 1, 'mode': 'standard'},
            },
        ),
        FilterConfig(),
    )
    session = MagicMock()
    session.region_name = 'us-west-2'
    client = session.client.return_value
    client.rerank.return_value = {
        'results': [
            {'index': 1, 'relevanceScore': 0.8},
            {'index': 0, 'relevanceScore': 0.2},
        ],
    }

    with patch.object(mod, 'GraphRAGConfig') as config:
        config.session = session
        config.bedrock_reranking_model = 'model-y'
        scores = proc._score_with_bedrock(['first', 'second'], query)

    assert scores == [0.2, 0.8]
    session.client.assert_called_once()
    service_name, = session.client.call_args.args
    client_kwargs = session.client.call_args.kwargs
    assert service_name == 'bedrock-agent-runtime'
    assert client_kwargs['region_name'] == 'us-west-2'
    assert isinstance(client_kwargs['config'], Config)
    assert client_kwargs['config'].connect_timeout == 2
    assert client_kwargs['config'].read_timeout == 3
    assert client_kwargs['config'].retries == {
        'total_max_attempts': 1,
        'mode': 'standard',
    }
    client.rerank.assert_called_once_with(
        queries=[{
            'type': 'TEXT',
            'textQuery': {'text': query.query_str},
        }],
        sources=[
            {
                'type': 'INLINE',
                'inlineDocumentSource': {
                    'type': 'TEXT',
                    'textDocument': {'text': 'first'},
                },
            },
            {
                'type': 'INLINE',
                'inlineDocumentSource': {
                    'type': 'TEXT',
                    'textDocument': {'text': 'second'},
                },
            },
        ],
        rerankingConfiguration={
            'type': 'BEDROCK_RERANKING_MODEL',
            'bedrockRerankingConfiguration': {
                'numberOfResults': 2,
                'modelConfiguration': {
                    'modelArn': (
                        'arn:aws:bedrock:us-west-2::foundation-model/model-y'
                    ),
                },
            },
        },
    )


def test_bedrock_omitted_config_does_not_pass_botocore_config(query):
    proc = RerankTopics(
        ProcessorArgs(topic_reranker='bedrock'),
        FilterConfig(),
    )
    session = MagicMock()
    session.region_name = 'us-west-2'
    session.client.return_value.rerank.return_value = {'results': []}

    with patch.object(mod, 'GraphRAGConfig') as config:
        config.session = session
        config.bedrock_reranking_model = 'model-y'
        proc._score_with_bedrock(['first'], query)

    session.client.assert_called_once_with(
        'bedrock-agent-runtime', region_name='us-west-2',
    )
