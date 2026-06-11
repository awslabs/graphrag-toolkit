# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult

from graphrag_toolkit.core.types import QueryBundle

class DisaggregateResults(ProcessorBase):
    """
    Processes search results to disaggregate topics.

    The DisaggregateResults class extends the ProcessorBase class, and its primary
    purpose is to process a collection of search results by disaggregating topics
    within each search result. Each topic is evaluated individually with its
    corresponding score, allowing more granular analysis or filtering.

    Attributes:
        args (ProcessorArgs): Configuration and runtime arguments passed
            to the processor.
        filter_config (FilterConfig): Configuration details related to
            filtering criteria and logic.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes the Processor class with provided arguments and filter configuration.

        This method sets up the processor by utilizing the given configuration and
        arguments, ensuring proper initialization for further processing tasks.

        Args:
            args (ProcessorArgs): Arguments necessary for configuring the processor.
            filter_config (FilterConfig): Configuration details for filter settings.

        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes and disaggregates search results based on individual topics and their highest statement scores.

        This method analyzes each search result, iterating through the associated topics, and isolates them into
        individual search results with updated scores based on the highest statement score within the topic. The
        updated collection of search results is then returned.

        Args:
            search_results: A collection of search results to be disaggregated and processed.
            query: The query bundle that corresponds to the search results.

        Returns:
            SearchResultCollection: An updated collection of search results with disaggregated topics and recalculated
            scores.
        """
        if not self.args.disaggregate_results:
            return search_results

        disaggregated_results = []

        for search_result in search_results.results:
            for topic in search_result.topics:
                score = max([s.score for s in topic.statements])
                disaggregated_results.append(SearchResult(topics=[topic], source=search_result.source, score=score))
                
        search_results = search_results.with_new_results(results=disaggregated_results)
        
        return search_results


