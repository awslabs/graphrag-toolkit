# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
import logging
from functools import reduce
from typing import List, Dict, Set, Any, Optional, Tuple, Iterator

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_top_k, SharedEmbeddingCache
from graphrag_toolkit.lexical_graph.retrieval.prompts import EXTRACT_KEYWORDS_PROMPT, EXTRACT_SYNONYMS_PROMPT
from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_base_retriever import SemanticGuidedBaseRetriever

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)

class KeywordRankingSearch(SemanticGuidedBaseRetriever):
    """
    Performs keyword-based search with ranking using a combination of keyword extraction
    and graph-based retrieval techniques.

    The class utilizes language models for extracting keywords and their synonyms from
    queries, and leverages a graph store to retrieve and rank relevant statements. The
    ranking is based on keyword matches and optional similarity computations. Results
    can be limited to a specified number of top-ranked items.

    Attributes:
        embedding_cache (Optional[SharedEmbeddingCache]): A cache to store and retrieve
            embeddings for statements. If not provided, embeddings will not be used
            during ranking.
        llm (LLMCacheType): Language model cache for predictions. A default model
            is used if none is provided.
        max_keywords (int): Maximum number of keywords to extract from the query.
        keywords_prompt (str): Prompt template for extracting keywords from the query.
        synonyms_prompt (str): Prompt template for extracting synonyms for the keywords.
        top_k (int): Maximum number of ranked results to retrieve.
    """
    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        embedding_cache:Optional[SharedEmbeddingCache]=None,
        keywords_prompt:str=EXTRACT_KEYWORDS_PROMPT,
        synonyms_prompt:str=EXTRACT_SYNONYMS_PROMPT,
        llm:LLMCacheType=None,
        max_keywords:int=10,
        top_k:int=100,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes a component for managing and processing natural language queries with various
        stores and configurations. The class utilizes a vector store for search, a graph store for
        structural data, and embedding cache mechanisms. Additionally, it incorporates interaction
        with a language model for extracting keywords and synonyms, among other features. The
        configuration supports customization through various prompts, limits, and filtering options.

        Args:
            vector_store: The vector store instance used for search and retrieval tasks.
            graph_store: The graph store instance used for managing structural or relational data.
            embedding_cache: Shared embedding cache for managing precomputed embeddings to improve
                retrieval efficiency. Optional parameter.
            keywords_prompt: A string prompt utilized for extracting keywords during query processing.
                Defaults to EXTRACT_KEYWORDS_PROMPT.
            synonyms_prompt: A string prompt used for extracting synonyms based on the context.
                Defaults to EXTRACT_SYNONYMS_PROMPT.
            llm: The language model or its cache instance used for processing natural language
                queries. If none is provided, a default LLMCache instance is created.
            max_keywords: Maximum number of keywords to be extracted during processing. Defaults to 10.
            top_k: Defines the maximum number of results to retrieve from the vector store
                as part of the processing workflow. Defaults to 100.
            filter_config: Filter configuration object used to define rules for post-retrieval filtering.
                Optional parameter.
            **kwargs: Additional keyword arguments supporting further customization or configuration
                as required by inherited or related classes.

        """
        super().__init__(vector_store, graph_store, filter_config, **kwargs)
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
        """
        Extract a set of unique keywords from the provided query using multiple prompts
        processed concurrently. The method uses a language model to process the query
        string and extract keywords and their synonyms.

        Args:
            query_bundle: A QueryBundle object containing the query string to analyze.

        Returns:
            A set of unique keywords extracted from the query.

        Raises:
            No explicitly raised errors, but logs exceptions encountered during execution
            and returns an empty set.
        """
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
        """
        Retrieves nodes with scores based on keyword matches and similarity scoring.

        This method performs the following steps:
        1. Extracts keywords from the provided query bundle.
        2. Queries a graph database to find all statements matching any of the extracted keywords,
           utilizing a Cypher query.
        3. Groups the retrieved statements by the number of keyword matches.
        4. Ranks statements within each group using similarity scores, if applicable.
        5. Normalizes scores and returns the top-k ranked nodes with their scores if the `top_k`
           attribute is specified.

        The method leverages the combination of keyword-based and embedding-based scoring mechanisms
        to rank and retrieve the most relevant nodes from the graph database.

        Args:
            query_bundle (QueryBundle): An object containing query information (e.g., textual query,
                embeddings), which guides the retrieval process.

        Returns:
            List[NodeWithScore]: A list of nodes with their associated scores, ranked by relevance.
                Each node includes metadata about the matched keywords and ranking methodology.
        """
        # 1. Get keywords
        keywords = self.get_keywords(query_bundle)
        if not keywords:
            logger.warning("No keywords extracted from query")
            return []
        
        logger.debug(f'keywords: {keywords}')

        # 2. Find statements matching any keyword
        cypher = f"""
        // find statements by keywords
        UNWIND $keywords AS keyword
        MATCH (e:`__Entity__`)
        WHERE e.search_str = keyword
        WITH e, keyword
        MATCH (e)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(statement)
        WITH statement, COLLECT(DISTINCT keyword) as matched_keywords
        RETURN {{
            statement: {{
                statementId: {self.graph_store.node_id("statement.statementId")}
            }},
            matched_keywords: matched_keywords
        }} AS result
        """

        lcase_keywords = [l.lower() for l in list(keywords)]
        
        results = self.graph_store.execute_query(cypher, {'keywords': lcase_keywords})
        if not results:
            logger.debug("No statements found matching keywords")
            return []
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:   
            logger.debug(f'results: {results}')
        else:
            logger.debug(f'num results: {len(results)}')

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
                
                if logger.isEnabledFor(logging.DEBUG) and self.debug_results:   
                    logger.debug(f'scored_statements: {scored_statements}')
                else:
                    logger.debug(f'num scored_statements: {len(scored_statements)}')
                
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

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:     
            logger.debug(f'final_nodes: {final_nodes}')
        else:
            logger.debug(f'num final_nodes: {len(final_nodes)}')

        return final_nodes
