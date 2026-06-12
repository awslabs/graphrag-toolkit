# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
RerankTopics processor.

Follows the same convention as RerankStatements: the reranking strategy is selected by a
string arg (``ProcessorArgs.topic_reranker`` -> 'tfidf' | 'bedrock' | 'none') and
dispatched here, rather than via ad-hoc flags. Where RerankStatements reranks statements
*within* a topic, this processor reranks whole topics by scoring each topic's
``name + all its statements`` against the query, keeps the top ``max_topics``, and drops the
rest before the token-budget truncation runs.

It also propagates each surviving topic's relevance score down to any statement that does not
already carry a score. This lets the statement reranker be turned off (``reranker='none'``)
so that statement selection is driven purely by topic relevance -- the ablation that answers
"if a topic ranks high by the reranker, is statement-level tfidf still needed?".
"""
import logging
import time
from typing import List, Dict

import boto3

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.retrieval.processors.processor_base import ProcessorBase
from graphrag_toolkit.lexical_graph.retrieval.processors.processor_args import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import Topic
from graphrag_toolkit.lexical_graph.utils.reranker_utils import score_values_with_tfidf
from graphrag_toolkit.lexical_graph.metadata import FilterConfig

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)


class RerankTopics(ProcessorBase):
    """Reranks and prunes whole topics by query relevance, using the strategy named in
    ``ProcessorArgs.topic_reranker``. No-op when topic_reranker is unset/'none'."""

    def __init__(self, args: ProcessorArgs, filter_config: FilterConfig):
        super().__init__(args, filter_config)

    def _topic_text(self, topic: Topic) -> str:
        stmts = ' '.join([s.statement_str or '' for s in topic.statements])
        return (f'{topic.topic}\n{stmts}')[:4000]  # cap per-doc length for the reranker

    def _score_with_tfidf(self, texts: List[str], query: QueryBundle) -> List[float]:
        scored = score_values_with_tfidf(texts, [query.query_str], len(texts))
        return [scored.get(t, 0.0) for t in texts]

    def _score_with_bedrock(self, texts: List[str], query: QueryBundle) -> List[float]:
        region = boto3.Session().region_name or getattr(GraphRAGConfig, 'aws_region', None) or 'us-east-1'
        client = boto3.client('bedrock-agent-runtime', region_name=region)
        model_arn = f'arn:aws:bedrock:{region}::foundation-model/{GraphRAGConfig.bedrock_reranking_model}'
        sources = [{'type': 'INLINE', 'inlineDocumentSource': {'type': 'TEXT', 'textDocument': {'text': t}}}
                   for t in texts]
        resp = client.rerank(
            queries=[{'type': 'TEXT', 'textQuery': {'text': query.query_str}}],
            sources=sources,
            rerankingConfiguration={
                'type': 'BEDROCK_RERANKING_MODEL',
                'bedrockRerankingConfiguration': {
                    'numberOfResults': len(texts),
                    'modelConfiguration': {'modelArn': model_arn},
                },
            },
        )
        scores = [0.0] * len(texts)
        for r in resp['results']:
            scores[r['index']] = r['relevanceScore']
        return scores

    def _process_results(self, search_results, query: QueryBundle):
        mode = (self.args.topic_reranker or 'none').lower()
        if mode == 'none':
            return search_results

        # Flatten (result_index, topic) pairs and build one document per topic.
        pairs = []
        texts = []
        for ri, sr in enumerate(search_results.results):
            for topic in sr.topics:
                pairs.append((ri, topic))
                texts.append(self._topic_text(topic))
        if not texts:
            return search_results

        start = time.time()
        if mode == 'tfidf':
            scores = self._score_with_tfidf(texts, query)
        elif mode == 'bedrock':
            scores = self._score_with_bedrock(texts, query)
        else:
            logger.warning(f'Unknown topic_reranker "{mode}", skipping topic rerank')
            return search_results
        logger.debug(f'Topic rerank ({mode}) of {len(texts)} topics: {(time.time()-start)*1000:.0f}ms')

        # Rank topics globally; keep the top max_topics.
        order = sorted(range(len(pairs)), key=lambda i: scores[i], reverse=True)
        keep = set(order[: self.args.max_topics])

        kept_topics_by_result: Dict[int, List[Topic]] = {}
        for i, (ri, topic) in enumerate(pairs):
            if i not in keep:
                continue
            # Propagate topic relevance to statements that have no score yet, so selection can
            # run on topic relevance alone when the statement reranker is disabled.
            for stmt in topic.statements:
                if stmt.score is None or stmt.score == 0.0:
                    stmt.score = scores[i]
            kept_topics_by_result.setdefault(ri, []).append(topic)

        # Rebuild results, dropping topics (and now-empty sources) that did not survive.
        new_results = []
        for ri, sr in enumerate(search_results.results):
            kept = kept_topics_by_result.get(ri)
            if not kept:
                continue
            sr.topics = kept
            new_results.append(sr)
        search_results = search_results.with_new_results(results=new_results)
        return search_results
