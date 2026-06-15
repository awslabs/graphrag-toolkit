# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import List, Dict

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.utils.reranker_utils import score_values_with_tfidf
from graphrag_toolkit.lexical_graph.retrieval.post_processors import SentenceReranker

from graphrag_toolkit.core.types import QueryBundle, NodeWithScore, Node

logger = logging.getLogger(__name__)

def _get_entity_token(entity):
    return f'{entity.entity.value.lower()} ({entity.entity.classification.lower()})'

def _get_reranked_entity_tokens_model(entities:List[ScoredEntity], keywords:List[str]) -> Dict[str, float]:

    reranker = SentenceReranker(model=GraphRAGConfig.reranking_model, top_n=3)
    rank_query = QueryBundle(query_str=' '.join(keywords))

    reranked_values = reranker.process(
        [
            NodeWithScore(node=Node(text=_get_entity_token(entity)), score=0.0)
            for entity in entities
        ],
        rank_query
    )

    reranked_entity_names =  {
        reranked_value.text : reranked_value.score
        for reranked_value in reranked_values
    }

    return reranked_entity_names

def _get_reranked_entity_tokens_tfidf(entities:List[ScoredEntity], keywords:List[str]) -> Dict[str, float]:
    
    entity_names = [_get_entity_token(entity) for entity in entities]
    reranked_entity_names = score_values_with_tfidf(entity_names, keywords)

    return reranked_entity_names

def _get_reranked_entity_tokens(entities:List[ScoredEntity], keywords:List[str], reranker:str) -> Dict[str, float]:

    # if reranker == 'model':
    #     results = _get_reranked_entity_tokens_model(entities, keywords) 
    # else:
    #     results = _get_reranked_entity_tokens_tfidf(entities, keywords)

    results = _get_reranked_entity_tokens_tfidf(entities, keywords)

    results = {
        k:round(v, 4) for k,v in results.items()
    }

    logger.debug(f'reranking ({reranker}): [keywords: {keywords}, reranked_entity_names: {results}]')

    return results

def _get_reranked_entities(entities:List[ScoredEntity], reranked_entity_tokens:Dict[str, float]) -> List[ScoredEntity]:

    entity_id_map = {}

    for reranked_entity_token, reranking_score in reranked_entity_tokens.items():
        for entity in entities:
            if _get_entity_token(entity) == reranked_entity_token and entity.entity.entityId not in entity_id_map:
                entity.reranking_score = reranking_score
                entity_id_map[entity.entity.entityId] = None
                

    entities.sort(key=lambda e: (-e.reranking_score, -e.score))

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f'''reranked_entities: {[
            entity.model_dump_json(exclude_unset=True, exclude_none=True, warnings=False) 
            for entity in entities
        ]}''')

    return entities

def rerank_entities(entities:List[ScoredEntity], query_bundle:QueryBundle, keywords:List[str], reranker:str) -> List[ScoredEntity]:
    all_reranked_entity_tokens = _get_reranked_entity_tokens(entities, [query_bundle.query_str] + keywords, reranker)
    return _get_reranked_entities(entities, all_reranked_entity_tokens)
