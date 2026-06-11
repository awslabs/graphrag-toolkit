# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import abc
from typing import Callable

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

class ProcessorBase(object):
    """Base class for processing and managing search results.

    This class provides foundational methods for processing, filtering, logging, and handling
    search results. It is designed to be subclassed for implementing specific result processing
    logic.

    Attributes:
        args (ProcessorArgs): Configuration parameters and settings for the processor.
        filter_config (FilterConfig): Configuration for filters applied during processing.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes an instance of the class with the provided arguments.

        This constructor sets up the necessary attributes to configure the
        class instance based on the provided processing arguments and filter
        configuration.

        Args:
            args (ProcessorArgs): The processing arguments used to configure
                the instance.
            filter_config (FilterConfig): The filter configuration parameters
                used to modify processing behavior.
        """
        self.args = args
        self.filter_config = filter_config

    def _log_results(self, retriever_name:str, title:str, search_results:SearchResultCollection):
        """
        Logs debug information about intermediate search results if debugging is enabled
        for the current processor. The log contains the retriever name, processor name,
        title, and formatted search results.

        Args:
            retriever_name: Name of the retriever that produced the search results.
            title: Descriptive title for the search results.
            search_results: Collection of search results to be logged, serialized
                into JSON format with specific filters applied.
        """
        processor_name = type(self).__name__
        if processor_name in self.args.debug_results and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'''Intermediate results [{retriever_name}.{processor_name}] {title}: {search_results.model_dump_json(
                indent=2, 
                exclude_unset=True, 
                exclude_defaults=True, 
                exclude_none=True, 
                warnings=False)
            }''')

    def _apply_to_search_results(self, 
                                 search_results:SearchResultCollection, 
                                 search_result_handler:Callable[[int, SearchResult], SearchResult],
                                 **kwargs):
        """
        Applies a given handler to a collection of search results and processes them
        based on the result of the handler function. Each search result is passed
        to the handler function along with its index and any additional keyword
        arguments. The results returned by the handler are preserved only if they
        contain either topics or statements.

        Args:
            search_results (SearchResultCollection): A collection of search results to be processed.
            search_result_handler (Callable[[int, SearchResult], SearchResult]): A function that takes the index
                of a search result, the search result itself, and optional keyword arguments, and returns a
                modified search result.
            **kwargs: Additional keyword arguments that are passed to the
                `search_result_handler`.

        Returns:
            SearchResultCollection: A new collection of search results containing only
            the results that the handler function has processed and determined to be
            valid (having topics or statements).
        """
        surviving_search_results = []

        for i, search_result in enumerate(search_results.results):
            return_result = search_result_handler(i, search_result, **kwargs)
            if return_result and (return_result.topics or return_result.statements):
                surviving_search_results.append(return_result)

        return search_results.with_new_results(results=surviving_search_results)
    
    def _apply_to_topics(self, 
                         search_result:SearchResult, 
                         topic_handler:Callable[[Topic], Topic], 
                         **kwargs):
        """
        Applies a handler function to all topics in a `search_result` and updates the result
        with only topics that satisfy specific criteria. Modifications to topics are determined
        by the provided `topic_handler`.

        Args:
            search_result (SearchResult): The search result object containing the topics to process.
            topic_handler (Callable[[Topic], Topic]): A callable that processes a `Topic` object.
                The returned `Topic` determines whether it remains in the search result.
            **kwargs: Additional keyword arguments to be passed to the `topic_handler`.

        Returns:
            SearchResult: An updated `SearchResult` object containing only the topics retained
                by the `topic_handler`.
        """
        surviving_topics = []

        for topic in search_result.topics:

            return_topic = topic_handler(topic, **kwargs)

            if return_topic and return_topic.statements:
                surviving_topics.append(return_topic)

        search_result.topics = surviving_topics

        return search_result
    
    def _format_statement_context(self, source_str:str, topic_str:str, statement_str:str):
        """
        Formats a given statement in the context of a specified topic and source.

        This function combines the provided topic, statement, and source into a single
        formatted string for easier readability or context representation. The topic and
        statement are placed upfront, followed by the source.

        Args:
            source_str: Represents the origin or source of the statement.
            topic_str: Specifies the topic under which the statement falls.
            statement_str: The statement or information to be formatted.

        Returns:
            str: A formatted string combining the topic and statement.
        """
        return f'{topic_str}: {statement_str}'

    def _log_counts(self, retriever_name:str, title:str, search_results:SearchResultCollection):
        """
        Logs the counts of search results, topics, and statements.

        This method calculates and logs the counts of search results, topics, and
        statements associated with a given retriever and search results collection.

        Args:
            retriever_name (str): The name of the retriever being used to process the
                search results.
            title (str): A title or identifier for the operation being logged.
            search_results (SearchResultCollection): A collection of search results
                containing the data to be analyzed and logged.
        """
        result_count = len(search_results.results)
        topic_count = sum([len(search_result.topics) for search_result in search_results.results])
        statement_count = sum([
            
            len(topic.statements)
            for search_result in search_results.results
            for topic in search_result.topics
        ])

        logger.debug(f'[{retriever_name}.{type(self).__name__}] {title}: [results: {result_count}, topics: {topic_count}, statements: {statement_count}]')

    
    def process_results(self, search_results:SearchResultCollection, query:QueryBundle, retriever_name:str) -> SearchResultCollection:
        """
        Processes search results by applying specific modifications and logging before and after states.

        This function modifies a collection of search results based on a given query bundle. It also logs
        the counts and detailed information of search results before and after processing, tagging them
        appropriately with the retriever name and state.

        Args:
            search_results: A collection of search results to be processed.
            query: The query bundle that provides contextual information for processing search results.
            retriever_name: A string representing the name of the retriever used for generating search results.

        Returns:
            A SearchResultCollection containing the processed search results.
        """
        self._log_counts(retriever_name, 'Before', search_results)
        self._log_results(retriever_name, 'Before', search_results)
        search_results = self._process_results(search_results, query)
        self._log_counts(retriever_name, 'After', search_results)
        self._log_results(retriever_name, 'After', search_results)
        return search_results
    
    @abc.abstractmethod
    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes search results based on a query.

        This method is expected to be implemented by subclasses to define the
        specific logic for filtering, modifying, or otherwise processing a
        collection of search results according to the provided query.

        Args:
            search_results: A collection of search results that need to be
                processed.
            query: A query bundle containing the parameters or data needed to
                execute the processing.

        Returns:
            A collection of processed search results.
        """
        raise NotImplementedError
