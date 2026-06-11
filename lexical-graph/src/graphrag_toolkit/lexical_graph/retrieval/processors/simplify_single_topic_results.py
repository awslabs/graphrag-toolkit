# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult

from graphrag_toolkit.core.types import QueryBundle

class SimplifySingleTopicResults(ProcessorBase):
    """
    Processor that simplifies search results by condensing single-topic results.

    This processor is designed to analyze search results and simplify cases
    where a search result contains only one topic. It modifies the search
    result structure by transferring the topic and its statements to the
    main result attributes and clearing the list of topics. This can be
    useful in scenarios where topics are nested in search results and there
    is a need to normalize them for easier processing.

    Attributes:
        args (ProcessorArgs): Configuration and arguments that dictate
            the behavior of the processor.
        filter_config (FilterConfig): Configuration that defines filtering
            settings for the processor.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes an instance of the processor class, setting up the base class with the provided
        arguments and configuration. This constructor ensures necessary setup for the processing
        pipeline.

        Args:
            args: The processor arguments providing configuration details required for setting
                up the processor instance.
            filter_config: The filter configuration specifying parameters and settings for
                filtering operations in the processor.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes and simplifies the given search results by extracting and consolidating
        topics and statements from individual search results.

        This function is intended to manipulate a collection of search results by invoking a
        helper function on each element of the collection. The helper function reduces
         the complexity in individual search result elements by simplifying topics when applicable.

        Args:
            search_results (SearchResultCollection): A collection of search results to process.
            query (QueryBundle): The related query for the search results.

        Returns:
            SearchResultCollection: The processed collection of search results where each result
            may have simplified topics and statements.
        """
        def simplify_result(index:int, search_result:SearchResult):
            """
            Processor to simplify search results by reducing them to a single topic when applicable.

            This processor iterates through the search results and examines their associated topics. If there is exactly one
            topic linked to a result, it promotes this topic to be the primary topic of the result, appending all statements
            linked to the topic into the main list of statements for that result, and clears the topic list.

            Method:
                - `_process_results`: Processes and simplifies the collection of search results based on the conditions
                  described above.
            """
            if len(search_result.topics) == 1:
                topic = search_result.topics[0]
                search_result.topic = topic.topic
                search_result.statements.extend(topic.statements)
                search_result.topics.clear()
                return search_result
            else:
                return search_result
        
        return self._apply_to_search_results(search_results, simplify_result)