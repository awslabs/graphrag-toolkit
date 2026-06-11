# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult

from graphrag_toolkit.core.types import QueryBundle

class PruneResults(ProcessorBase):
    """
    Represents a processor that prunes search results based on a score threshold.

    This class inherits from ProcessorBase and processes search results by applying a pruning function.
    The pruning removes results that do not meet a predefined score threshold. It is designed for use
    cases where it is necessary to filter out low-scoring results from a search result collection.

    Attributes:
        args (ProcessorArgs): Arguments containing configuration and settings for the pruning process,
            including the results pruning threshold.
        filter_config (FilterConfig): Configuration for filtering, providing additional parameters
            or constraints that may influence the pruning logic.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes the base class for processing tasks with specified arguments and
        filter configuration.

        Args:
            args (ProcessorArgs): The arguments required for processing tasks.
            filter_config (FilterConfig): Configuration settings for filtering during
                processing.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes the search results by applying a pruning function based on the results' scores relative
        to a predefined threshold. Any search result with a score below the threshold is excluded. This
        method modifies the search results collection to retain only those results meeting the score
        criterion.

        Args:
            search_results: The collection of search results to be processed. Each result may either
                be retained or pruned based on its score relative to the pruning threshold.
            query: The query bundle associated with the search results, providing context for processing.

        Returns:
            SearchResultCollection: A new collection of search results with only those results whose
            scores meet the pruning threshold retained.
        """
        def prune_search_result(index:int, search_result:SearchResult):
            return search_result if search_result.score >= self.args.results_pruning_threshold else None

        return self._apply_to_search_results(search_results, prune_search_result)


