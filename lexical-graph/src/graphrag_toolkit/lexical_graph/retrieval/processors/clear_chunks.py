# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic

from graphrag_toolkit.core.types import QueryBundle

class ClearChunks(ProcessorBase):
    """
    Handles the clearing of chunks within topics in a collection of search results.

    The ClearChunks class is responsible for modifying topics by removing their
    associated chunks. This is done iteratively over a collection of search results.
    It inherits from `ProcessorBase` and utilizes its utility methods to perform
    operations on topics and search results. This processor may be used in
    situations where textual or data chunks associated with topics need to be
    removed from search results for further processing or analysis.

    Attributes:
        args (ProcessorArgs): Configuration arguments passed to the processor,
            defining its behavior and settings.
        filter_config (FilterConfig): Filtering configuration that determines
            how the processor handles filtering-related tasks.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes the instance of the class with the provided arguments and filter configuration.
        This sets up the necessary attributes and base class initialization to manage processing
        and configuration effectively for the derived use case.

        Args:
            args (ProcessorArgs): The processing arguments required for setting up the instance.
            filter_config (FilterConfig): The configuration settings for filtering tasks.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes search results by clearing the chunks in associated topics.

        This method processes a collection of search results, applying an operation to
        clear all chunks associated with the topics in each search result. It modifies
        the input search results collection and returns the processed results.

        Args:
            search_results: A collection of search results to be processed.
            query: A query bundle containing the search query details.

        Returns:
            SearchResultCollection: A processed collection of search results where the
            chunks in associated topics have been cleared.
        """
        def clear_chunks(topic:Topic):
            topic.chunks.clear()
            return topic

        def clear_search_result_chunks(index:int, search_result:SearchResult):
            return self._apply_to_topics(search_result, clear_chunks)
        
        return self._apply_to_search_results(search_results, clear_search_result_chunks)


