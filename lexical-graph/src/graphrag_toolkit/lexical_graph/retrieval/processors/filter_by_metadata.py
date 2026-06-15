# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.versioning import VALID_TO, VALID_FROM
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult

from graphrag_toolkit.core.types import QueryBundle

class FilterByMetadata(ProcessorBase):
    """
    Filters search results based on metadata.

    This class is responsible for filtering search results by examining their metadata.
    The filtering is applied to a collection of search results, retaining only those
    that meet the criteria defined in the filter configuration.

    Attributes:
        args (ProcessorArgs): Arguments required for the processing.
        filter_config (FilterConfig): Configuration that defines the metadata filtering rules.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes the class instance and sets up the basic configuration for processing.

        The constructor initializes the parent class with the provided arguments and
        filter configuration. It is responsible for setting up any necessary state
        or configurations required by the class for further processing.

        Args:
            args (ProcessorArgs): The arguments required for initializing the processor.
            filter_config (FilterConfig): The configuration settings for the filtering
                process.
        """
        super().__init__(args, filter_config)
        
    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes search results based on the provided query and applies filters to the search result metadata.

        Filters the search results by evaluating the metadata of each result using the filter configuration.
        Only results that satisfy the filter criteria are retained.

        Args:
            search_results: A collection of search results to be filtered.
            query: The query bundle associated with the search results.

        Returns:
            SearchResultCollection: A collection of filtered search results.
        """
        def filter_search_result(index:int, search_result:SearchResult):
            
            versioning_metadata = {
                VALID_FROM:search_result.source.versioning.valid_from,
                VALID_TO:search_result.source.versioning.valid_to
            }

            metadata = search_result.source.metadata | versioning_metadata

            return search_result if self.filter_config.filter_source_metadata_dictionary(metadata) else None

        return self._apply_to_search_results(search_results, filter_search_result)