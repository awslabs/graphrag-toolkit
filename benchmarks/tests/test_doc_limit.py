# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for the extraction document cap.

The cap has to do two things together: extract fewer documents, and lower the
count the run asserts. Doing only the first turns every capped run into a
failure on the full corpus's document count.
"""

import pytest

from benchmarks.utils.doc_limit import (
    apply_extraction_doc_limit,
    capped_expected_docs,
    extraction_doc_limit,
)

DOCS = list(range(10))


@pytest.fixture(autouse=True)
def unset_limit(monkeypatch):
    monkeypatch.delenv('BENCHMARK_EXTRACT_DOC_LIMIT', raising=False)


class TestExtractionDocLimit:

    def test_none_when_unset(self):
        assert extraction_doc_limit() is None

    def test_none_when_empty(self, monkeypatch):
        # The harness exports allowlisted-but-unset variables as empty strings.
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '')
        assert extraction_doc_limit() is None

    def test_reads_the_value(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '100')
        assert extraction_doc_limit() == 100

    @pytest.mark.parametrize('raw', ['0', '-1'])
    def test_zero_or_negative_is_no_cap(self, monkeypatch, raw):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', raw)
        assert extraction_doc_limit() is None

    def test_non_numeric_raises(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', 'a hundred')
        with pytest.raises(ValueError, match='BENCHMARK_EXTRACT_DOC_LIMIT'):
            extraction_doc_limit()


class TestApplyExtractionDocLimit:

    def test_unset_returns_every_document(self):
        assert apply_extraction_doc_limit(DOCS) == DOCS

    def test_caps_to_the_first_n(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '3')
        assert apply_extraction_doc_limit(DOCS) == [0, 1, 2]

    def test_limit_above_the_corpus_returns_every_document(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '99')
        assert apply_extraction_doc_limit(DOCS) == DOCS

    def test_empty_corpus_stays_empty(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '3')
        assert apply_extraction_doc_limit([]) == []


class TestCappedExpectedDocs:

    def test_unset_leaves_the_count_alone(self):
        assert capped_expected_docs(13501) == 13501

    def test_cap_lowers_the_count(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '100')
        assert capped_expected_docs(13501) == 100

    def test_cap_above_the_corpus_leaves_the_count_alone(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '99999')
        assert capped_expected_docs(507) == 507

    def test_no_expectation_stays_no_expectation(self, monkeypatch):
        # None means "assert only that something was extracted".
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '100')
        assert capped_expected_docs(None) is None

    def test_agrees_with_the_documents_actually_extracted(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '4')
        assert capped_expected_docs(len(DOCS)) == len(apply_extraction_doc_limit(DOCS))
