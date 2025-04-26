# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
import logging
from functools import reduce
from typing import List, Dict, Set, Any, Optional, Tuple, Iterator

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_top_k, SharedEmbeddingCache
from graphrag_toolkit.lexical_graph.retrieval.prompts import EXTRACT_KEYWORDS_PROMPT, EXTRACT_SYNONYMS_PROMPT
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_guided_base_retriever import SemanticGuidedBaseRetriever

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)

class KeywordRankingSearch(SemanticGuidedBaseRetriever):
    def __init__(
        self,
        vector_store: VectorStore,
        graph_store: GraphStore,
        embedding_cache: Optional[SharedEmbeddingCache] = None,
        keywords_prompt: str = EXTRACT_KEYWORDS_PROMPT,
        synonyms_prompt: str = EXTRACT_SYNONYMS_PROMPT,
        llm:LLMCacheType = None,
        max_keywords: int = 10,
        top_k: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(vector_store, graph_store, **kwargs)
        self.embedding_cache = embedding_cache
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache
        )
        self.max_keywords = max_keywords
        self.keywords_prompt = keywords_prompt
        self.synonyms_prompt = synonyms_prompt
        self.top_k = top_k

    def get_keywords(self, query_bundle: QueryBundle) -> Set[str]:
        """Get keywords and synonyms for the query."""
        def extract(prompt):
            response = self.llm.predict(
                PromptTemplate(template=prompt),
                text=query_bundle.query_str,
                max_keywords=self.max_keywords
            )
            return {kw.strip().lower() for kw in response.strip().split('^')}

        try:
            with concurrent.futures.ThreadPoolExecutor() as p:
                keyword_batches: Iterator[Set[str]] = p.map(
                    extract, 
                    (self.keywords_prompt, self.synonyms_prompt)
                )
                unique_keywords = reduce(lambda x, y: x.union(y), keyword_batches)
                logger.debug(f"Extracted keywords: {unique_keywords}")
                return unique_keywords
        except Exception as e:
            logger.error(f"Error extracting keywords: {e}")
            return set()

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        try:
            # 1. Get keywords
            keywords = self.get_keywords(query_bundle)
            if not keywords:
                logger.warning("No keywords extracted from query")
                return []
            
            logger.debug(f'keywords: {keywords}')

            # 2. Find statements matching any keyword
            cypher = f"""
            UNWIND $keywords AS keyword
            MATCH (e:`__Entity__`)
            WHERE toLower(e.value) = toLower(keyword)
            WITH e, keyword
            MATCH (e)-[:`__SUBJECT__`|`__OBJECT__`]->(:`__Fact__`)-[:`__SUPPORTS__`]->(statement:`__Statement__`)
            WITH statement, COLLECT(DISTINCT keyword) as matched_keywords
            RETURN {{
                statement: {{
                    statementId: {self.graph_store.node_id("statement.statementId")}
                }},
                matched_keywords: matched_keywords
            }} AS result
            """
            
            results = self.graph_store.execute_query(cypher, {'keywords': list(keywords)})
            if not results:
                logger.debug("No statements found matching keywords")
                return []
            
            logger.debug(f'results: {results}')

            # 3. Group statements by number of keyword matches
            statements_by_matches: Dict[int, List[Tuple[str, Set[str]]]] = {}
            for result in results:
                statement_id = result['result']['statement']['statementId']
                matched_keywords = set(result['result']['matched_keywords'])
                num_matches = len(matched_keywords)
                if num_matches not in statements_by_matches:
                    statements_by_matches[num_matches] = []
                statements_by_matches[num_matches].append((statement_id, matched_keywords))

            # 4. Process groups in order of most matches
            final_nodes = []
            for num_matches in sorted(statements_by_matches.keys(), reverse=True):
                group = statements_by_matches[num_matches]
                
                # If there are ties, use similarity to rank within group
                if len(group) > 1:
                    statement_ids = [sid for sid, _ in group]
                    statement_embeddings = self.embedding_cache.get_embeddings(statement_ids)
                    
                    scored_statements = get_top_k(
                        query_bundle.embedding,
                        statement_embeddings,
                        len(statement_ids)
                    )
                    
                    logger.debug(f'scored_statements: {scored_statements}')
                    
                    # Create nodes with scores and keyword information
                    keyword_map = {sid: kw for sid, kw in group}
                    for score, statement_id in scored_statements:
                        matched_keywords = keyword_map[statement_id]
                        node = TextNode(
                            text="",  # Placeholder
                            metadata={
                                'statement': {'statementId': statement_id},
                                'search_type': 'keyword_ranking',
                                'keyword_matches': list(matched_keywords),
                                'num_keyword_matches': len(matched_keywords)
                            }
                        )
                        # Normalize score using both keyword matches and similarity
                        combined_score = (num_matches / len(keywords)) * (score + 1) / 2
                        final_nodes.append(NodeWithScore(node=node, score=combined_score))
                else:
                    # Single statement in group
                    statement_id, matched_keywords = group[0]
                    node = TextNode(
                        text="",  # Placeholder
                        metadata={
                            'statement': {'statementId': statement_id},
                            'search_type': 'keyword_ranking',
                            'keyword_matches': list(matched_keywords),
                            'num_keyword_matches': len(matched_keywords)
                        }
                    )
                    score = num_matches / len(keywords)
                    final_nodes.append(NodeWithScore(node=node, score=score))

            # 5. Limit to top_k if specified
            if self.top_k:
                final_nodes.sort(key=lambda x: x.score or 0.0, reverse=True)
                final_nodes = final_nodes[:self.top_k]
                
            logger.debug(f'final_nodes: {final_nodes}')

            return final_nodes

        except Exception as e:
            logger.error(f"Error in KeywordRankingSearch: {e}")
            return []