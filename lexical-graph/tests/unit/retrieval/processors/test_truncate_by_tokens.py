# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.processors.truncate_by_tokens import TruncateByTokens
from graphrag_toolkit.lexical_graph.retrieval.model import (
    SearchResultCollection, SearchResult, Topic, Statement, Source, Versioning, EntityContexts,
)
from llama_index.core.schema import QueryBundle


def _collection(num_statements=5):
    versioning = Versioning(valid_from=0, valid_to=9999999999)
    source = Source(sourceId='doc1', metadata={}, versioning=versioning)
    topic = Topic(
        topic='T', topicId='t1',
        statements=[
            Statement(statementId=str(i), statement=f'statement number {i} with several words',
                      score=1.0 / (i + 1))
            for i in range(num_statements)
        ],
    )
    result = SearchResult(source=source, topics=[topic], score=0.95)
    return SearchResultCollection(results=[result], entity_contexts=EntityContexts(contexts=[], keywords=[]))


def _count(collection):
    return sum(len(t.statements) for r in collection.results for t in r.topics)


@pytest.fixture
def query():
    return QueryBundle(query_str='q')


@pytest.mark.parametrize('mode', ['global_rank', 'per_topic_cap'])
def test_truncates_to_budget(mode, query):
    proc = TruncateByTokens(ProcessorArgs(max_context_tokens=20, token_truncation_mode=mode), FilterConfig())
    out = proc._process_results(_collection(), query)
    kept = _count(out)
    assert 0 < kept < 5  # some statements dropped to fit the small budget


def test_default_mode_is_global_rank(query):
    # token_truncation_mode defaults to 'global_rank'
    assert ProcessorArgs().token_truncation_mode == 'global_rank'
    proc = TruncateByTokens(ProcessorArgs(max_context_tokens=20), FilterConfig())
    assert proc.mode == 'global_rank'


def test_no_validation_or_encoder_when_budget_unset():
    # Without a token budget the processor is a no-op: an otherwise-invalid mode must not
    # raise, and the tokenizer is not built.
    proc = TruncateByTokens(ProcessorArgs(token_truncation_mode='nonexistent'), FilterConfig())
    assert proc._enc is None


def test_invalid_mode_raises_when_budget_set():
    import pytest
    with pytest.raises(ValueError):
        TruncateByTokens(
            ProcessorArgs(max_context_tokens=100, token_truncation_mode='nonexistent'),
            FilterConfig(),
        )


def test_global_rank_keeps_highest_scoring(query):
    # global_rank fills the budget relevance-first, so the highest-score statement
    # (statementId '0', score 1.0) must survive a tight budget.
    proc = TruncateByTokens(ProcessorArgs(max_context_tokens=20, token_truncation_mode='global_rank'), FilterConfig())
    out = proc._process_results(_collection(), query)
    kept_ids = {s.statementId for r in out.results for t in r.topics for s in t.statements}
    assert '0' in kept_ids


def test_noop_when_unset(query):
    # max_context_tokens defaults to None -> processor passes results through unchanged
    proc = TruncateByTokens(ProcessorArgs(), FilterConfig())
    out = proc._process_results(_collection(), query)
    assert _count(out) == 5


def test_always_keeps_at_least_one_statement(query):
    # Budget smaller than a single statement still keeps the first (avoids empty context)
    proc = TruncateByTokens(ProcessorArgs(max_context_tokens=1, token_truncation_mode='global_rank'), FilterConfig())
    out = proc._process_results(_collection(), query)
    assert _count(out) == 1


def test_autowired_into_pipeline_when_budget_set():
    # When max_context_tokens is set, the default pipeline swaps the count-based
    # TruncateStatements for TruncateByTokens; otherwise it is left unchanged, and an
    # explicit processors= list is always respected.
    from unittest.mock import MagicMock
    from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_based_search import ChunkBasedSearch
    from graphrag_toolkit.lexical_graph.retrieval.processors import TruncateStatements

    no_budget = ChunkBasedSearch(MagicMock(), MagicMock(), processor_args=ProcessorArgs())
    assert TruncateStatements in no_budget.processors
    assert TruncateByTokens not in no_budget.processors

    budgeted = ChunkBasedSearch(MagicMock(), MagicMock(), processor_args=ProcessorArgs(max_context_tokens=3000))
    assert TruncateByTokens in budgeted.processors
    assert TruncateStatements not in budgeted.processors

    explicit = ChunkBasedSearch(MagicMock(), MagicMock(),
                                processor_args=ProcessorArgs(max_context_tokens=3000),
                                processors=[TruncateStatements])
    assert explicit.processors == [TruncateStatements]  # caller's list untouched
