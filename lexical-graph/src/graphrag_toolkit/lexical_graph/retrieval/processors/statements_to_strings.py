# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic

from graphrag_toolkit.core.types import QueryBundle

class StatementsToStrings(ProcessorBase):
    """
    Processes statements into strings based on the configuration provided.

    The StatementsToStrings class is a specialized processor that transforms
    statements into string representations. It provides functionality for
    processing search results and converting statements in topics to their
    string equivalents, adhering to specific filtration configurations. This
    can be useful for applications requiring formatted outputs of statements
    or further downstream processing where raw statements should be converted to strings.

    Attributes:
        args (ProcessorArgs): Processor arguments providing configurations for
            the processing operation.
        filter_config (FilterConfig): Filtering configuration to determine rules
            and conditions for processing the statements.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes a new instance of the class.

        Args:
            args (ProcessorArgs): The configuration arguments required for initial
                processing.
            filter_config (FilterConfig): The configuration settings for applying
                filtering.
        """
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes search results by converting topic statements into strings based on configuration.

        This function modifies the search results by transforming topic-related information,
        specifically converting statements into strings either by their statement attribute
        or their statement_str attribute, depending on the configuration set in `self.args.include_facts`.

        Args:
            search_results (SearchResultCollection): A collection of search results to be processed.
            query (QueryBundle): A query bundle providing context or filtering for the search results.

        Returns:
            SearchResultCollection: The processed search results with modified topic statement
            representations.
        """
        def get_statement_string(s):
            """Processor that converts statement objects to their string representations
            based on a specified attribute.

            This processor works on a collection of search results, extracting the string
            representation of each statement object within the results. The conversion is
            governed by the `include_facts` attribute specified in the processor's arguments.
            """
            return s.statement_str if self.args.include_facts else s.statement
        
        def statements_to_strings(topic:Topic):
            """
            Processes search results by converting statements in topics to string
            representations.

            This class is a subclass of ProcessorBase and is designed to handle
            search results and apply a transformation to the topics within those
            search results. The transformation involves converting all statements
            in each topic to their string representations.

            Method `_process_results` applies this process and ensures that the
            transformation is applied to the `statements` attribute of topics.

            Args:
                search_results (SearchResultCollection): The collection of search
                    results to process.
                query (QueryBundle): The query bundle associated with the search
                    results.

            Returns:
                SearchResultCollection: A collection of search results with updated
                topics where all statements have been converted to string
                representations.
            """
            topic.statements = [
                get_statement_string(statement)
                for statement in topic.statements
            ]
            return topic
    
        def search_result_statements_to_strings(index:int, search_result:SearchResult):
            """
            Processes search results by converting statements within each result into string
            representations. The transformation is applied to topics within a search result
            through the use of the method `_apply_to_topics`.

            Attributes:
                None

            """
            return self._apply_to_topics(search_result, statements_to_strings)
        
        return self._apply_to_search_results(search_results, search_result_statements_to_strings)


