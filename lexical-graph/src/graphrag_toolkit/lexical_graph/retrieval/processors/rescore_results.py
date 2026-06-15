# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import statistics

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult

from graphrag_toolkit.core.types import QueryBundle

class RescoreResults(ProcessorBase):
    """
    Represents a processor for rescoring search results by recalculating scores based on
    topic-level analysis. This class adjusts the overall score of each search result
    by averaging the highest statement scores within its associated topics.

    This class is designed to refine search result rankings by promoting results with
    higher relevance as indicated by topic-level scoring. It leverages an internal
    rescoring logic to compute adjusted scores efficiently.

    Attributes:
        args (ProcessorArgs): Configuration arguments for the processor, including runtime-specific details.
        filter_config (FilterConfig): Configuration for filtering criteria applied during result processing.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes the class with provided arguments and configuration for processing
        and filtering tasks.

        Args:
            args: ProcessorArgs
                The arguments required for processing tasks.
            filter_config: FilterConfig
                The configuration responsible for filtering operations.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes and rescales the scores of search results by calculating the mean score
        of topics associated with each search result. The logic uses the statements within
        each topic to compute a per-topic score and updates the overall score for each
        search result accordingly.

        Args:
            search_results (SearchResultCollection): A collection of search results to process.
            query (QueryBundle): A query bundle containing the query information and context.

        Returns:
            SearchResultCollection: The updated search results with recalculated scores.
        """
        def rescore_search_result(index:int, search_result:SearchResult):
            """
            Processes and modifies search results by rescoring them based on the highest statement
            scores within each topic of the search results.

            This class inherits from `ProcessorBase` and provides an implementation of result
            processing that updates the final score of a search result by calculating the mean
            of the highest statement scores for each topic associated with the search result.

            Methods:
                _process_results: Takes in a collection of search results and rescoring logic is
                applied using the rescore_search_result function.

                rescore_search_result: Internal helper function to compute the rescored value of
                an individual search result by determining scores based on topics and associated
                statements.

            Args:
                search_results (SearchResultCollection): A collection of original search results to be processed.
                query (QueryBundle): The query associated with the search operation.
            """
            topic_scores = [
                statistics.mean([s.score for s in topic.statements])
                for topic in search_result.topics
            ]
            
            search_result.score = max(topic_scores)
            
            return search_result
        
        return self._apply_to_search_results(search_results, rescore_search_result)
        


