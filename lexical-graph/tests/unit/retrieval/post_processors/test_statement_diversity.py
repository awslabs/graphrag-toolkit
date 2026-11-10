# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from llama_index.core.schema import NodeWithScore, TextNode

from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResult
from graphrag_toolkit.lexical_graph.retrieval.post_processors.statement_diversity import (
    ALL_TEXT,
    TOPICS,
    TOPICS_AND_STATEMENTS,
    StatementDiversityPostProcessor,
)

STOP_WORDS = {'the', 'a', 'is'}


def _token(word):
    return SimpleNamespace(
        text=word,
        like_num=bool(re.fullmatch(r'\d+(\.\d+)?', word)),
        is_stop=word.lower() in STOP_WORDS,
        is_punct=bool(re.fullmatch(r'[^\w\s]+', word)),
        lemma_=word.rstrip('s') if len(word) > 3 else word,
    )


def _fake_nlp(text):
    return [_token(w) for w in text.split()]


def _install_spacy(monkeypatch, load=None):
    nlp = MagicMock(side_effect=_fake_nlp)
    fake_spacy = SimpleNamespace(load=load or MagicMock(return_value=nlp))
    monkeypatch.setitem(sys.modules, 'spacy', fake_spacy)
    return fake_spacy, nlp


def _processor(monkeypatch, **kwargs):
    _install_spacy(monkeypatch)
    return StatementDiversityPostProcessor(**kwargs)


def _nodes(*texts):
    return [NodeWithScore(node=TextNode(text=t), score=1.0) for t in texts]


def _texts(nodes):
    return [n.node.text for n in nodes]


def _similarity(processor, a, b):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    matrix = TfidfVectorizer().fit_transform(processor.preprocess_texts([a, b]))
    return cosine_similarity(matrix)[0][1]


class TestConstruction:

    def test_missing_spacy_package_raises_import_error(self, monkeypatch):
        monkeypatch.setitem(sys.modules, 'spacy', None)

        with pytest.raises(ImportError) as exc:
            StatementDiversityPostProcessor()

        assert str(exc.value) == "spacy package not found, install with 'pip install spacy'"

    def test_missing_spacy_model_raises_model_error(self, monkeypatch):
        _install_spacy(monkeypatch, load=MagicMock(side_effect=OSError))

        with pytest.raises(ModelError) as exc:
            StatementDiversityPostProcessor()

        assert str(exc.value) == (
            'Please install the spaCy model using: python -m spacy download en_core_web_sm'
        )

    def test_loads_small_english_model_with_sentencizer(self, monkeypatch):
        fake_spacy, nlp = _install_spacy(monkeypatch)

        processor = StatementDiversityPostProcessor()

        fake_spacy.load.assert_called_once_with('en_core_web_sm', disable=['ner', 'parser'])
        nlp.add_pipe.assert_called_once_with('sentencizer')
        assert processor.nlp is nlp

    def test_defaults(self, monkeypatch):
        processor = _processor(monkeypatch)

        assert processor.similarity_threshold == 0.975
        assert processor.text_fn is ALL_TEXT

    def test_custom_threshold_and_text_fn(self, monkeypatch):
        processor = _processor(monkeypatch, similarity_threshold=0.5, text_fn=TOPICS)

        assert processor.similarity_threshold == 0.5
        assert processor.text_fn is TOPICS


class TestPreprocessTexts:

    def test_normalises_numbers_and_drops_stop_words_and_punctuation(self, monkeypatch):
        processor = _processor(monkeypatch)

        result = processor.preprocess_texts(['The Rockets is 42 and 3.14 !'])

        assert result == ['rocket NUM_42 and FLOAT_3.14']

    def test_one_output_per_input(self, monkeypatch):
        processor = _processor(monkeypatch)

        assert processor.preprocess_texts(['alpha', '', 'beta']) == ['alpha', '', 'beta']


class TestDiversityFiltering:

    def test_empty_input_is_returned_unchanged(self, monkeypatch):
        processor = _processor(monkeypatch)

        assert processor.postprocess_nodes([]) == []

    def test_drops_later_duplicates_and_keeps_order(self, monkeypatch):
        processor = _processor(monkeypatch)
        nodes = _nodes('apples grow on trees', 'rivers flow downhill', 'apples grow on trees')

        result = processor.postprocess_nodes(nodes)

        assert _texts(result) == ['apples grow on trees', 'rivers flow downhill']

    def test_similarity_above_threshold_is_filtered(self, monkeypatch):
        processor = _processor(monkeypatch)
        a, b = 'alpha beta gamma delta', 'alpha beta gamma epsilon'
        processor.similarity_threshold = _similarity(processor, a, b) - 0.01

        assert _texts(processor.postprocess_nodes(_nodes(a, b))) == [a]

    def test_similarity_below_threshold_is_kept(self, monkeypatch):
        processor = _processor(monkeypatch)
        a, b = 'alpha beta gamma delta', 'alpha beta gamma epsilon'
        processor.similarity_threshold = _similarity(processor, a, b) + 0.01

        assert _texts(processor.postprocess_nodes(_nodes(a, b))) == [a, b]

    def test_similarity_equal_to_threshold_is_kept(self, monkeypatch):
        processor = _processor(monkeypatch)
        a, b = 'alpha beta gamma delta', 'alpha beta gamma epsilon'
        processor.similarity_threshold = _similarity(processor, a, b)

        assert _texts(processor.postprocess_nodes(_nodes(a, b))) == [a, b]

    def test_uses_text_fn_to_compare_nodes(self, monkeypatch):
        processor = _processor(monkeypatch, text_fn=TOPICS)
        nodes = _nodes(
            SearchResult(source='s1', topic='solar power', statements=['one']).model_dump_json(),
            SearchResult(source='s2', topic='solar power', statements=['two']).model_dump_json(),
        )

        result = processor.postprocess_nodes(nodes)

        assert result == [nodes[0]]

    def test_missing_numpy_raises_import_error(self, monkeypatch):
        processor = _processor(monkeypatch)
        monkeypatch.setitem(sys.modules, 'numpy', None)

        with pytest.raises(ImportError, match=re.escape("numpy package not found, install with 'pip install numpy'")):
            processor.postprocess_nodes(_nodes('a'))

    def test_missing_scikit_learn_raises_import_error(self, monkeypatch):
        processor = _processor(monkeypatch)
        monkeypatch.setitem(sys.modules, 'sklearn.feature_extraction.text', None)

        with pytest.raises(
            ImportError,
            match=re.escape("scikit-learn package not found, install with 'pip install scikit-learn'"),
        ):
            processor.postprocess_nodes(_nodes('a'))


class TestTextFunctions:

    def test_all_text_returns_node_text(self):
        assert ALL_TEXT(TextNode(text='hello')) == 'hello'

    def test_topics_returns_topic(self):
        node = TextNode(text=SearchResult(source='s1', topic='T', statements=['a']).model_dump_json())

        assert TOPICS(node) == 'T'

    def test_topics_and_statements_joins_topic_and_string_statements(self):
        node = TextNode(text=SearchResult(source='s1', topic='T', statements=['a', 'b']).model_dump_json())

        assert TOPICS_AND_STATEMENTS(node) == 'T\na\nb'
