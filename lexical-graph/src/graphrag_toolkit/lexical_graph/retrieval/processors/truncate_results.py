# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection

from graphrag_toolkit.core.types import QueryBundle

class TruncateResults(ProcessorBase):
    """
    TruncateResults processes search results by limiting the number of results.

    This class extends the ProcessorBase and is used to truncate the
    number of search results to a defined maximum limit specified
    in the configuration. It modifies the search results inline
    by only keeping the top results up to the configured limit.

    Attributes:
        args (ProcessorArgs): Configuration and settings for processing.
        filter_config (FilterConfig): Configuration for the filtering process.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes an instance of the Processor class. This constructor provides
        initial setup and configuration using the specified arguments and filter
        configuration.

        Args:
            args (ProcessorArgs): Arguments for configuring the processor.
            filter_config (FilterConfig): Filter configuration details used during
                initialization.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes the search results by truncating the number of results to a defined maximum.

        This method modifies a SearchResultCollection object by trimming its results
        based on the `max_search_results` attribute specified in the `args`. It ensures
        that only the top-ranked results up to this maximum limit are retained.

        Args:
            search_results: A collection of search results to process.
            query: The query information associated with the search results.

        Returns:
            A SearchResultCollection object with the results truncated to the specified
            maximum number.

        """
        search_results.results = search_results.results[:self.args.max_search_results]
        return search_results


