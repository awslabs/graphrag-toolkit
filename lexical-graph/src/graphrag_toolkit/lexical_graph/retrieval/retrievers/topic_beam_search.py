# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, List, Optional, Type

import numpy as np

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.storage.vector.vector_index import to_embedded_query
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import TraversalBasedBaseRetriever
from graphrag_toolkit.lexical_graph.retrieval.retrievers.beam_search_base import BeamSearch
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_top_k

from llama_index.core.schema import QueryBundle
from llama_index.core.vector_stores.types import VectorStoreQueryMode

logger = logging.getLogger(__name__)


class TopicBeamSearch(TraversalBasedBaseRetriever, BeamSearch):
    """Traversal-based retriever that beam-searches over the topic graph.

    Two phases:
      1. Seed: top-k topics from the topic vector index (``get_start_node_ids``).
      2. Beam: expand seeds over same-chunk topic neighbours (``do_graph_search``),
         score with the configured beam scoring mode, then expand the winning
         topics into their statements via the standard traversal helpers.

    Neighbour strategy is configurable; the default (same-chunk only) reflects the
    best-performing configuration: topics that co-occur in the same source chunk
    are more reliably related than topics linked only by shared entities across
    documents.
    """

    def __init__(self,
                 graph_store: GraphStore,
                 vector_store: VectorStore,
                 processor_args: Optional[ProcessorArgs] = None,
                 processors: Optional[List[Type[ProcessorBase]]] = None,
                 filter_config: FilterConfig = None,
                 max_depth: int = None,
                 beam_width: int = None,
                 top_k: int = None,
                 use_entity_neighbors: bool = None,
                 use_same_chunk_neighbors: bool = None,
                 use_adjacent_chunk_neighbors: bool = None,
                 max_entity_neighbors: int = None,
                 max_statements_per_topic: int = 25,
                 scoring_mode: str = 'path_weighted',
                 **kwargs):
        # Tuning is sourced from ProcessorArgs (the repo convention - mirrors how the
        # chunk-beam retriever reads chunk_beam_* from args). Explicit constructor
        # arguments, when provided, override the ProcessorArgs value.
        args = processor_args if processor_args is not None else ProcessorArgs(**kwargs)
        beam_width = beam_width if beam_width is not None else args.topic_beam_width
        max_depth = max_depth if max_depth is not None else args.topic_beam_max_depth
        BeamSearch.__init__(self, beam_width=beam_width, max_depth=max_depth, scoring_mode=scoring_mode)
        # When topic reranking is requested (and the caller hasn't supplied an explicit
        # processor list), insert RerankTopics into the default pipeline right after
        # statement reranking so it can prune topics by query relevance.
        if processors is None and (getattr(args, 'topic_reranker', 'none') or 'none').lower() != 'none':
            from graphrag_toolkit.lexical_graph.retrieval.processors import RerankStatements, RerankTopics
            from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import DEFAULT_PROCESSORS
            _procs = list(DEFAULT_PROCESSORS)
            _idx = _procs.index(RerankStatements) + 1 if RerankStatements in _procs else len(_procs)
            _procs.insert(_idx, RerankTopics)
            processors = _procs
        TraversalBasedBaseRetriever.__init__(
            self,
            graph_store=graph_store,
            vector_store=vector_store,
            processor_args=args,
            processors=processors,
            filter_config=filter_config,
            **kwargs,
        )
        pick = lambda override, default: override if override is not None else default
        self.top_k = pick(top_k, args.topic_top_k)
        self.use_entity_neighbors = pick(use_entity_neighbors, args.use_entity_neighbors)
        self.use_same_chunk_neighbors = pick(use_same_chunk_neighbors, args.use_same_chunk_neighbors)
        self.use_adjacent_chunk_neighbors = pick(use_adjacent_chunk_neighbors, args.use_adjacent_chunk_neighbors)
        self.max_entity_neighbors = pick(max_entity_neighbors, args.max_entity_neighbors)
        self.max_statements_per_topic = max_statements_per_topic
        self._topic_cache: Dict[str, np.ndarray] = {}
        self._query_embedding = None

    def _init(self, query_bundle: QueryBundle):
        # Topic beam search does not use entity contexts; skip that extraction to
        # avoid the extra keyword/entity provider round-trips.
        return

    # --- Phase 1: seed topics ---

    def get_start_node_ids(self, query_bundle: QueryBundle) -> List[str]:
        topic_index = self.vector_store.get_index('topic')
        query_bundle = to_embedded_query(query_bundle, topic_index.embed_model)
        self._query_embedding = np.array(query_bundle.embedding)

        results = topic_index.client.query(
            VectorStoreQueryMode.DEFAULT,
            query_str=query_bundle.query_str,
            query_embedding=query_bundle.embedding,
            k=self.top_k,
            filters=None,
        )
        topic_ids = []
        for node in results.nodes:
            if node and node.metadata:
                topic_id = node.metadata.get('topic', {}).get('topicId')
                if topic_id:
                    topic_ids.append(topic_id)
                    if node.embedding:
                        self._topic_cache[topic_id] = np.array(node.embedding)
        return topic_ids

    # --- BeamSearch hooks ---

    def _get_embeddings(self, ids: List[str]) -> dict:
        """Embeddings for topic ids, sourced entirely from the stored topic vectors.

        Seeds are cached from the topic VSS query; neighbour ids discovered during the
        beam are resolved by fetching their stored embeddings from the topic index by id
        (one batched lookup). This keeps every vector on the same basis as the seeds
        (the stored topic embedding = name + statements) and avoids recomputing anything
        at query time. TopicBeamSearch already requires an embedded topic index in order
        to seed, so neighbour vectors are expected to be present; any id the index does
        not return is skipped (and logged) rather than recomputed on the fly.
        """
        missing = [tid for tid in ids if tid not in self._topic_cache]
        if not missing:
            return {tid: self._topic_cache[tid] for tid in ids if tid in self._topic_cache}

        try:
            topic_index = self.vector_store.get_index('topic')
            # Fetch stored vectors by id through the VectorIndex abstraction (backend-agnostic),
            # rather than reaching into a concrete client. get_embeddings batches internally.
            for rec in topic_index.get_embeddings(missing):
                tid, emb = rec.get('id'), rec.get('embedding')
                if tid and emb is not None:
                    self._topic_cache[tid] = np.array(emb)
        except Exception as ex:
            logger.warning(f'Failed to fetch stored topic embeddings: {ex}')

        # Skip (do not expand to) any neighbour topics without a stored embedding;
        # this should not happen for a normally-built, seedable topic index.
        unresolved = [tid for tid in missing if tid not in self._topic_cache]
        if unresolved:
            logger.debug(
                f'Skipping {len(unresolved)} neighbour topic(s) with no stored embedding '
                f'in the topic index (e.g. {unresolved[:3]})'
            )
        return {tid: self._topic_cache[tid] for tid in ids if tid in self._topic_cache}

    def _get_top_k(self, query_embedding, embeddings, top_k):
        return get_top_k(query_embedding, embeddings, top_k)

    def get_neighbors(self, topic_id: str) -> List[str]:
        return self.get_neighbors_batch([topic_id]).get(topic_id, [])

    def get_neighbors_batch(self, topic_ids: List[str]) -> Dict[str, List[str]]:
        """Neighbour topics via the configured strategies (entity-overlap,
        same-chunk co-occurrence, adjacent-chunk co-occurrence)."""
        if not topic_ids:
            return {}

        parts = []
        match_clause = f"""
        MATCH (t:`__Topic__`)
        WHERE {self.graph_store.node_id('t.topicId')} IN $topicIds
        """

        if self.use_entity_neighbors:
            parts.append(f"""
            OPTIONAL MATCH (t)<-[:`__BELONGS_TO__`]-(s:`__Statement__`)<-[:`__SUPPORTS__`]-(f:`__Fact__`)<-[:`__SUBJECT__`|`__OBJECT__`]-(e:`__Entity__`)
            WITH t, COLLECT(DISTINCT e) AS entities
            UNWIND CASE WHEN size(entities) = 0 THEN [null] ELSE entities END AS entity
            OPTIONAL MATCH (entity)-[:`__SUBJECT__`|`__OBJECT__`]->(f2:`__Fact__`)-[:`__SUPPORTS__`]->(s2:`__Statement__`)-[:`__BELONGS_TO__`]->(nt:`__Topic__`)
            WHERE entity IS NOT NULL AND nt <> t
            WITH t, {self.graph_store.node_id('nt.topicId')} AS ntid, count(f2) AS strength
            ORDER BY strength DESC
            WITH t, COLLECT(DISTINCT ntid)[..{int(self.max_entity_neighbors)}] AS entity_neighbors
            """)
        else:
            parts.append("WITH t, [] AS entity_neighbors")

        if self.use_same_chunk_neighbors:
            parts.append(f"""
            OPTIONAL MATCH (t)-[:`__MENTIONED_IN__`]->(c:`__Chunk__`)<-[:`__MENTIONED_IN__`]-(ct:`__Topic__`)
            WHERE ct <> t
            WITH t, entity_neighbors, COLLECT(DISTINCT {self.graph_store.node_id('ct.topicId')}) AS chunk_neighbors
            """)
        else:
            parts.append("WITH t, entity_neighbors, [] AS chunk_neighbors")

        if self.use_adjacent_chunk_neighbors:
            parts.append(f"""
            OPTIONAL MATCH (t)-[:`__MENTIONED_IN__`]->(c:`__Chunk__`)-[:`__NEXT__`]->(adj:`__Chunk__`)<-[:`__MENTIONED_IN__`]-(at:`__Topic__`)
            WHERE at <> t
            WITH {self.graph_store.node_id('t.topicId')} AS sourceId,
                 entity_neighbors, chunk_neighbors,
                 COLLECT(DISTINCT {self.graph_store.node_id('at.topicId')}) AS adjacent_neighbors
            """)
        else:
            parts.append(f"""
            WITH {self.graph_store.node_id('t.topicId')} AS sourceId,
                 entity_neighbors, chunk_neighbors, [] AS adjacent_neighbors
            """)

        return_clause = "RETURN sourceId, entity_neighbors + chunk_neighbors + adjacent_neighbors AS neighborIds"
        cypher = match_clause + "\n".join(parts) + "\n" + return_clause
        results = self.graph_store.execute_query(cypher, {'topicIds': topic_ids})
        return {r['sourceId']: list(set(r['neighborIds'])) for r in results}

    # --- Phase 2: beam search + statement expansion ---

    def _expand_topics_to_statement_ids(self, topic_ids: List[str]) -> List[str]:
        """Statement ids for the winning topics, capped per topic."""
        if not topic_ids:
            return []
        cypher = f"""
        MATCH (s:`__Statement__`)-[:`__BELONGS_TO__`]->(t:`__Topic__`)
        WHERE {self.graph_store.node_id('t.topicId')} IN $topicIds
        RETURN {self.graph_store.node_id('t.topicId')} AS topicId,
               {self.graph_store.node_id('s.statementId')} AS statementId
        """
        rows = self.graph_store.execute_query(cypher, {'topicIds': topic_ids})
        per_topic: Dict[str, int] = {}
        statement_ids = []
        for row in rows:
            tid = row['topicId']
            count = per_topic.get(tid, 0)
            if count < self.max_statements_per_topic:
                statement_ids.append(row['statementId'])
                per_topic[tid] = count + 1
        return statement_ids

    def do_graph_search(self, query_bundle: QueryBundle, start_node_ids: List[str]) -> SearchResultCollection:
        if not start_node_ids:
            return self._to_search_results_collection([])

        beam_results = self.beam_search(self._query_embedding, start_node_ids)
        winning_topic_ids = [topic_id for topic_id, _path in beam_results]
        logger.debug(f'TopicBeamSearch: {len(start_node_ids)} seeds -> {len(winning_topic_ids)} topics')

        statement_ids = self._expand_topics_to_statement_ids(winning_topic_ids)
        statement_results = self.get_statements_by_topic_and_source(list(set(statement_ids)))
        return self._to_search_results_collection(statement_results)
