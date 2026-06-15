# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/keyword_nlp_provider."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.errors import ModelError
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs


def _provider_with_spacy(monkeypatch, ents):
    doc = SimpleNamespace(ents=[SimpleNamespace(text=t) for t in ents])
    nlp = MagicMock(return_value=doc)
    fake_spacy = SimpleNamespace(load=MagicMock(return_value=nlp))
    monkeypatch.setitem(sys.modules, 'spacy', fake_spacy)
    from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_nlp_provider import (
        KeywordNLPProvider,
    )
    return KeywordNLPProvider(args=ProcessorArgs(max_keywords=10))


class TestKeywordNLPProvider:
    def test_missing_spacy_raises_import_error(self, monkeypatch):
        monkeypatch.setitem(sys.modules, 'spacy', None)
        from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_nlp_provider import (
            KeywordNLPProvider,
        )
        with pytest.raises(ImportError, match='spacy package not found'):
            KeywordNLPProvider(args=ProcessorArgs())

    def test_missing_spacy_model_raises_model_error(self, monkeypatch):
        fake_spacy = SimpleNamespace(load=MagicMock(side_effect=OSError))
        monkeypatch.setitem(sys.modules, 'spacy', fake_spacy)
        from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_nlp_provider import (
            KeywordNLPProvider,
        )
        with pytest.raises(ModelError, match='spaCy model'):
            KeywordNLPProvider(args=ProcessorArgs())

    def test_extracts_entity_text_as_keywords(self, monkeypatch):
        provider = _provider_with_spacy(monkeypatch, ['Alice', 'Bob'])
        result = provider.get_keywords(QueryBundle('Who knows Alice and Bob?'))
        assert sorted(result) == ['Alice', 'Bob']

    def test_dedups_case_insensitively_keeping_last(self, monkeypatch):
        provider = _provider_with_spacy(monkeypatch, ['Alice', 'alice', 'Bob'])
        result = provider.get_keywords(QueryBundle('q'))
        assert sorted(s.lower() for s in result) == ['alice', 'bob']
