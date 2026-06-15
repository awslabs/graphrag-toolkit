# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic
from graphrag_toolkit.core.types import QueryBundle

class TruncateStatements(ProcessorBase):
    """
    TruncateStatements is a processor for truncating the number of statements in topics
    contained within search results.

    This class provides functionality for processing search results and limiting the number
    of statements associated with topics to a specified maximum threshold. It modifies
    topics within search results by truncating their statements based on the configured
    maximum allowed.

    Attributes:
        args (ProcessorArgs): Arguments specific to the processing operation, including
            configuration like the maximum number of statements per topic.
        filter_config (FilterConfig): Configuration object used for determining which
            content to process.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes an instance of the class with the provided arguments and
        filter configuration. Ensures proper setup of the parent class during
        initialization.

        Args:
            args (ProcessorArgs): Arguments required for processing. These
                contain configurations necessary for the initialization and
                operation of the processor.
            filter_config (FilterConfig): Configuration related to filters
                applied in the specific processing context. It determines
                filtering mechanisms and criteria.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes search results by truncating the number of statements in each topic based on the
        specified maximum statements per topic.

        This function applies truncation logic to all search results within the provided
        SearchResultCollection. Each topic's statements are limited to the maximum number defined in
        `self.args.max_statements_per_topic`.

        Args:
            search_results: A collection of search results to be processed.
            query: A query bundle that may contain context or filters for processing the
                search results.

        Returns:
            A modified SearchResultCollection where each topic has been truncated to the specified
            number of statements.
        """
        def truncate_statements(topic:Topic):
            """
            A processor that truncates the number of statements for each topic in a
            search result collection.

            This processor limits the number of statements associated with a topic
            to a predefined maximum by retaining only the first N statements, where
            N is specified by the `max_statements_per_topic` argument.
            """
            topic.statements = topic.statements[:self.args.max_statements_per_topic]
            return topic
        
        def truncate_search_result(index:int, search_result:SearchResult):
            """A processor class for truncating statements within search results.

            This class inherits from `ProcessorBase` and provides the capability
            to truncate the statements found in `SearchResultCollection` objects.
            The truncation is performed through the `_process_results` method,
            which operates on search results and query bundles, applying
            a truncation function to each indexed search result.

            Attributes:
                None
            """
            return self._apply_to_topics(search_result, truncate_statements)
        
        return self._apply_to_search_results(search_results, truncate_search_result)


