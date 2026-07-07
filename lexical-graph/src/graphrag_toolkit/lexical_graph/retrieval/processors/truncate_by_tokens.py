# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from collections import OrderedDict

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic
from llama_index.core.schema import QueryBundle


class TruncateByTokens(ProcessorBase):
    """Truncate statements to fit within a token budget using tiktoken.

    Configured via ProcessorArgs:
    - ``max_context_tokens`` (int | None): token budget. If falsy, this processor
      is a no-op (results pass through unchanged).
    - ``token_truncation_mode`` (str): one of
        - 'global_rank'   : (default) flatten all statements, sort by relevance score,
          fill the budget from the top, then re-group into source/topic structure.
          Best general-purpose mode - keeps the most relevant statements regardless of
          which topic or source they came from. Note: because selection is purely
          relevance-first, this can concentrate all surviving statements in a single
          source/topic, undoing source-diversity work done upstream (e.g. by
          get_diverse_vss_elements). Choose 'per_topic_cap' if preserving breadth matters.
        - 'per_topic_cap' : each topic gets an equal share of the budget
          (max_context_tokens / num_topics). This is the token-based analog of the
          count-based ``max_statements_per_topic`` cap, for callers who want to
          preserve per-topic breadth rather than relevance-first selection.

    Notes / caveats:
    - Token counts use tiktoken 'cl100k_base'. This is an approximation when the
      downstream generator is not an OpenAI model (e.g. Anthropic Claude); use it
      for consistent relative budgeting rather than an exact generator token count.
    - The budget counts statement text only; topic/source headers and any context
      formatting added downstream are not counted, so the rendered context sent to
      the generator will be somewhat larger than max_context_tokens (a deliberate
      under-count - treat the budget as a statement-text target, not a hard ceiling
      on the final prompt).
    - To avoid returning empty context, the first statement is always kept even if
      it alone exceeds the budget, so the result may overshoot by one large statement.
    - 'per_topic_cap' distributes a per-topic share but also enforces the overall
      ``max_context_tokens`` as a hard global ceiling, so the total no longer scales
      with the number of topics (aside from the always-keep-one-per-topic rule, which
      may overshoot by at most one statement per topic).
    """

    def __init__(self, args: ProcessorArgs, filter_config: FilterConfig):
        super().__init__(args, filter_config)
        self.max_tokens = args.max_context_tokens
        self.mode = args.token_truncation_mode
        # Only validate the mode and build the tokenizer when a budget is actually set -
        # without max_context_tokens this processor is a no-op, so it should neither reject
        # configuration it won't use nor pay the cost of loading the encoder.
        self._enc = None
        if self.max_tokens:
            if self.mode not in ('global_rank', 'per_topic_cap'):
                raise ValueError(
                    f"Invalid token_truncation_mode '{self.mode}'; "
                    "expected 'global_rank' or 'per_topic_cap'"
                )
            import tiktoken  # lazy: only needed when a token budget is configured
            self._enc = tiktoken.get_encoding('cl100k_base')

    def _count_tokens(self, text: str) -> int:
        if not text:
            return 0
        return len(self._enc.encode(text))

    @staticmethod
    def _stmt_text(stmt) -> str:
        return (stmt.statement if hasattr(stmt, 'statement') else str(stmt)) or ''

    def _process_results(self, search_results: SearchResultCollection, query: QueryBundle) -> SearchResultCollection:
        if not self.max_tokens:
            return search_results
        if self.mode == 'per_topic_cap':
            return self._per_topic_cap(search_results)
        return self._global_rank(search_results)

    def _per_topic_cap(self, search_results: SearchResultCollection) -> SearchResultCollection:
        """Give each topic an equal share of the token budget."""
        all_topics = [t for r in search_results.results for t in r.topics]
        if not all_topics:
            return search_results
        per_topic_budget = self.max_tokens // len(all_topics)
        global_remaining = self.max_tokens  # hard global ceiling enforced across all topics

        new_results = []
        for search_result in search_results.results:
            new_topics = []
            for topic in search_result.topics:
                topic_remaining = per_topic_budget
                new_statements = []
                for stmt in topic.statements:
                    cost = self._count_tokens(self._stmt_text(stmt))
                    # Stop on either the per-topic share or the global budget; keep at
                    # least one statement per topic to avoid empty context.
                    if (cost > topic_remaining or cost > global_remaining) and new_statements:
                        break
                    topic_remaining -= cost
                    global_remaining -= cost
                    new_statements.append(stmt)
                    if topic_remaining <= 0 or global_remaining <= 0:
                        break
                if new_statements:
                    new_topics.append(topic.model_copy(update={'statements': new_statements}))
                if global_remaining <= 0:
                    break
            if new_topics:
                new_results.append(search_result.model_copy(update={'topics': new_topics}))
            if global_remaining <= 0:
                break
        return search_results.with_new_results(results=new_results)

    def _global_rank(self, search_results: SearchResultCollection) -> SearchResultCollection:
        """Flatten all statements, sort by score, fill budget from top, re-group."""
        all_stmts = []
        for result in search_results.results:
            for topic in result.topics:
                for stmt in topic.statements:
                    score = stmt.score if hasattr(stmt, 'score') and stmt.score is not None else 0.0
                    all_stmts.append((score, stmt, topic, result))
        all_stmts.sort(key=lambda x: x[0], reverse=True)

        remaining = self.max_tokens
        selected = []
        for score, stmt, topic, result in all_stmts:
            cost = self._count_tokens(self._stmt_text(stmt))
            if cost > remaining and selected:
                break
            remaining -= cost
            selected.append((stmt, topic, result))
            if remaining <= 0:
                break

        return search_results.with_new_results(results=self._regroup(selected))

    @staticmethod
    def _source_key(source):
        return source.sourceId if hasattr(source, 'sourceId') else str(source)

    def _regroup(self, selected):
        """Re-group selected (stmt, topic, result) tuples into SearchResult -> Topic
        structure, preserving encounter order. The original Topic and SearchResult objects
        are copied via model_copy so fields other than statements/topics (e.g.
        SearchResult.score) are preserved rather than dropped."""
        source_map = OrderedDict()
        for stmt, topic, result in selected:
            sk = self._source_key(result.source)
            if sk not in source_map:
                source_map[sk] = {'result': result, 'topics': OrderedDict()}
            if topic.topicId not in source_map[sk]['topics']:
                source_map[sk]['topics'][topic.topicId] = {'topic': topic, 'stmts': []}
            source_map[sk]['topics'][topic.topicId]['stmts'].append(stmt)

        new_results = []
        for data in source_map.values():
            topics = [
                tdata['topic'].model_copy(update={'statements': tdata['stmts']})
                for tdata in data['topics'].values()
            ]
            new_results.append(data['result'].model_copy(update={'topics': topics}))
        return new_results
