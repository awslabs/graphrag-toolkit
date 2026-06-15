# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.retrieval.prompts import EXTRACT_SUBQUERIES_PROMPT, IDENTIFY_MULTIPART_QUESTION_PROMPT
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.prompt import PromptTemplate
from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

SINGLE_QUESTION_THRESHOLD = 25

class QueryDecomposition():
    """
    Handles query decomposition for identifying and extracting subqueries from a primary query.

    This class is responsible for breaking down a complex query into simpler subqueries using
    a language model (LLM). It aims to identify multipart questions and decompose them when necessary.
    This process enables better handling and response generation for large or intricate queries.

    Attributes:
        llm (LLMCacheType): The language model used for identifying multipart
            questions and extracting subqueries. Defaults to the specified
            `llm` instance or creates a new one using `GraphRAGConfig`.
        identify_multipart_question_template (str): A template prompt
            used to identify whether a query is multipart.
        extract_subqueries_template (str): A template prompt used to extract
            subqueries from a multipart query.
        max_subqueries (int): Maximum number of subqueries that can be extracted
            from a single query. Defaults to 2.
    """
    def __init__(self,
                 args:ProcessorArgs,
                 llm:LLMCacheType=None, 
                 identify_multipart_question_template=IDENTIFY_MULTIPART_QUESTION_PROMPT,
                 extract_subqueries_template=EXTRACT_SUBQUERIES_PROMPT,
                 max_subqueries=2):
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache if not args.no_cache else not args.no_cache
        )
        self.identify_multipart_question_template = identify_multipart_question_template
        self.extract_subqueries_template = extract_subqueries_template
        self.max_subqueries = max_subqueries

    def _extract_subqueries(self, s:str) -> List[QueryBundle]:
        """
        Extracts subqueries from a given string using a language model.

        The method takes a string input and utilizes a language model (LLM) to predict
        and generate subqueries based on a predefined template. The result is returned
        as a list of QueryBundle objects, each holding one of the extracted subqueries.
        This provides structured subqueries derived from the input string for further
        processing.

        Args:
            s (str): The input string containing the query/questions to extract
                subqueries from.

        Returns:
            List[QueryBundle]: A list of QueryBundle objects, each containing
                an extracted subquery.
        """
        response = self.llm.predict(
                PromptTemplate(template=self.extract_subqueries_template),
                question=s,
                max_subqueries=self.max_subqueries
            )

        return [QueryBundle(query_str=s) for s in response.split('\n') if s]
    
    def _is_multipart_question(self, s:str):
        """
        Identifies whether a given question is a multipart question
        or not by using a language model's prediction.

        Args:
            s: The question string to be evaluated.

        Returns:
            bool: True if the response indicates the question is
            multipart, otherwise False.
        """
        response = self.llm.predict(
                PromptTemplate(template=self.identify_multipart_question_template),
                question=s
            )

        return response.lower().startswith('no')


    def decompose_query(self, query_bundle: QueryBundle) -> List[QueryBundle]:
        """
        Decomposes a query into subqueries if it is identified as a multipart question.

        This method takes a query encapsulated in a QueryBundle object and decomposes
        it into smaller subqueries if the original query exceeds the single question
        threshold and is determined to be a multipart question. If not, the original
        query is returned as a single-element list.

        Args:
            query_bundle (QueryBundle): The input query bundle containing the
                query string and any associated metadata.

        Returns:
            List[QueryBundle]: A list of QueryBundle objects representing the
                subqueries. If decomposition is not performed, the original
                query bundle is returned as a single-element list.

        Raises:
            None
        """
        subqueries = [query_bundle]
        
        original_query = query_bundle.query_str

        if len(original_query.split()) > SINGLE_QUESTION_THRESHOLD:
            if self._is_multipart_question(original_query):
                subqueries = self._extract_subqueries(original_query)

        logger.debug(f'Subqueries: {subqueries}')
                 
        return subqueries