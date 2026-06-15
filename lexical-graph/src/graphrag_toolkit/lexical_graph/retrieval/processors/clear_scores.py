# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic

from graphrag_toolkit.core.types import QueryBundle

class ClearScores(ProcessorBase):
    """
    Handles the processing of clearing scores from search results.

    This class is designed to process a collection of search results and remove the
    scores associated with them. It uses the base ProcessorBase functionality to
    apply the clearing operation to each search result in the given collection.
    This can be useful in scenarios where the scores are either irrelevant or need
    to be redacted for further processing.

    Attributes:
        args (ProcessorArgs): Arguments required for initializing the processor,
            providing configuration and operational parameters.
        filter_config (FilterConfig): Configuration settings for filtering, passed
            during instantiation.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes the class with the provided arguments for processing and filter configuration.
        Ensures proper setup by invoking the parent class initializer.

        Args:
            args (ProcessorArgs): The arguments required for processing operations.
            filter_config (FilterConfig): The configuration for filtering operations.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes the given search results by applying a scoring operation through
        a specified callback function. This method clears the scores of all
        search results within the given collection, setting them to None.

        Args:
            search_results: The collection of search results to be processed.
            query: The query bundle associated with the search results.

        Returns:
            SearchResultCollection: A collection of search results with updated scores.
        """
        def clear_score(index:int, search_result:SearchResult):
            search_result.score = None
            return search_result
        
        return self._apply_to_search_results(search_results, clear_score)
