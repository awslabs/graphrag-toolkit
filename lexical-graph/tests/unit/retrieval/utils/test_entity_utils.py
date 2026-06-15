# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/utils/entity_utils."""

from unittest.mock import MagicMock, patch

from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.model import Entity, ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.utils import entity_utils
from graphrag_toolkit.lexical_graph.retrieval.utils.entity_utils import (
    _get_entity_token,
    _get_reranked_entities,
    _get_reranked_entity_tokens,
    _get_reranked_entity_tokens_tfidf,
    rerank_entities,
)


def _scored_entity(entity_id, value, classification, score):
    return ScoredEntity(
        entity=Entity(entityId=entity_id, value=value, classification=classification),
        score=score,
    )


class TestGetEntityToken:
    def test_lowercases_value_and_classification(self):
        e = _scored_entity('e1', 'Apple', 'Company', 0.5)
        assert _get_entity_token(e) == 'apple (company)'

    def test_preserves_already_lowercase(self):
        e = _scored_entity('e1', 'beta', 'noun', 0.5)
        assert _get_entity_token(e) == 'beta (noun)'


class TestGetRerankedEntityTokensTfidf:
    def test_returns_score_per_entity_token(self):
        entities = [
            _scored_entity('e1', 'apple', 'fruit', 0.9),
            _scored_entity('e2', 'orange', 'fruit', 0.8),
        ]
        with patch.object(entity_utils, 'score_values_with_tfidf') as m:
            m.return_value = {'apple (fruit)': 0.7, 'orange (fruit)': 0.3}
            result = _get_reranked_entity_tokens_tfidf(entities, ['apple'])
        assert result == {'apple (fruit)': 0.7, 'orange (fruit)': 0.3}
        passed_tokens, passed_keywords = m.call_args.args
        assert passed_tokens == ['apple (fruit)', 'orange (fruit)']
        assert passed_keywords == ['apple']


class TestGetRerankedEntityTokens:
    def test_rounds_scores_to_four_decimals(self):
        entities = [_scored_entity('e1', 'a', 'x', 0.9)]
        with patch.object(entity_utils, 'score_values_with_tfidf') as m:
            m.return_value = {'a (x)': 0.123456789}
            result = _get_reranked_entity_tokens(entities, ['kw'], reranker='tfidf')
        assert result == {'a (x)': 0.1235}


class TestGetRerankedEntities:
    def test_assigns_reranking_score_and_sorts_descending(self):
        e1 = _scored_entity('e1', 'apple', 'fruit', 0.5)
        e2 = _scored_entity('e2', 'orange', 'fruit', 0.9)
        tokens = {'apple (fruit)': 0.8, 'orange (fruit)': 0.2}

        result = _get_reranked_entities([e1, e2], tokens)

        assert result[0].entity.entityId == 'e1'
        assert result[0].reranking_score == 0.8
        assert result[1].reranking_score == 0.2

    def test_sort_tiebreaks_on_base_score(self):
        e1 = _scored_entity('e1', 'a', 'x', 0.4)
        e2 = _scored_entity('e2', 'b', 'x', 0.9)
        tokens = {'a (x)': 0.5, 'b (x)': 0.5}

        result = _get_reranked_entities([e1, e2], tokens)
        assert [r.entity.entityId for r in result] == ['e2', 'e1']

    def test_duplicate_token_only_assigns_once(self):
        e1 = _scored_entity('e1', 'apple', 'fruit', 0.5)
        e2 = _scored_entity('e1', 'apple', 'fruit', 0.5)
        tokens = {'apple (fruit)': 0.7}

        result = _get_reranked_entities([e1, e2], tokens)
        scores = [r.reranking_score for r in result]
        assert scores.count(0.7) == 1


class TestRerankEntities:
    def test_passes_query_str_with_keywords_to_token_reranker(self):
        e1 = _scored_entity('e1', 'apple', 'fruit', 0.5)
        with patch.object(entity_utils, '_get_reranked_entity_tokens') as m:
            m.return_value = {'apple (fruit)': 0.9}
            result = rerank_entities(
                [e1],
                QueryBundle(query_str='find fruit'),
                ['apple'],
                reranker='tfidf',
            )
        passed_entities, passed_keywords, passed_reranker = m.call_args.args
        assert passed_keywords == ['find fruit', 'apple']
        assert passed_reranker == 'tfidf'
        assert result[0].reranking_score == 0.9
