# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection

from graphrag_toolkit.core.types import QueryBundle

class SortResults(ProcessorBase):
    """
    SortResults processes and sorts search results based on their score.

    This class inherits from ProcessorBase and provides functionality for sorting
    search results in descending order of their scores. It is designed to be used
    within a computational pipeline that handles search result processing.
    The class ensures that the search results are ordered by relevance as determined
    by their scores, which allows subsequent stages in the pipeline to operate on
    sorted result data.

    Attributes:
        args (ProcessorArgs): Configuration and arguments relevant for the
            processing of results.
        filter_config (FilterConfig): Configuration for filtering behavior
            during processing.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes a processor with the provided arguments and filter configuration.

        This constructor sets up the necessary parameters by accepting processor
        arguments and a filter configuration object. It ensures that the processor
        is initialized correctly with all required settings.

        Args:
            args: Configuration parameters and settings required for the processor
                to operate.
            filter_config: A configuration object containing filter specifications
                that define processing criteria.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes and sorts search results based on their score in descending order.

        This function is responsible for reordering the search results, ensuring that
        items with higher scores appear earlier in the collection. It modifies the
        `search_results` object in place and returns it after sorting.

        Args:
            search_results: A collection of search results to be sorted.
            query: A query bundle that was used to generate the search results.

        Returns:
            A `SearchResultCollection` object with the results sorted in descending
            order of score.
        """
        results = search_results.results
        search_results.results = sorted(results, key=lambda x: x.score, reverse=True)
        return search_results


