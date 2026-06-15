# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Callable, Dict, Any, Optional

from graphrag_toolkit.lexical_graph.metadata import MetadataFiltersType, FilterConfig
from pydantic import BaseModel

logger = logging.getLogger(__name__)

DEFAULT_BUILD_FILTER = lambda s: False

class BuildFilters(BaseModel):
    """
    BuildFilters manages filtering logic for topics, statements, and source metadata.

    This class integrates customizable filtering functions to determine whether specific
    topics or statements should be ignored, as well as functionality to process and
    filter source metadata dictionaries. It provides a unified interface for applying
    different filtering mechanisms, enabling greater flexibility and control in filter logic.

    Attributes:
        topic_filter_fn (Callable[[str], bool]): A function used to determine whether a
            given topic should be ignored. It returns True for topics to be ignored.
        statement_filter_fn (Callable[[str], bool]): A function used to determine whether a
            given statement should be ignored. It returns True for statements to be ignored.
        source_filters (FilterConfig): A configuration object managing the filtering of
            source metadata dictionaries.
    """
    topic_filter_fn:Callable[[str], bool]
    statement_filter_fn:Callable[[str], bool]
    source_filters:FilterConfig

    def __init__(self, 
                 topic_filter_fn:Callable[[str], bool]=None, 
                 statement_filter_fn:Callable[[str], bool]=None,
                 source_filters:Optional[MetadataFiltersType]=None
        ):
        """
        Initializes a filter configuration for topics, statements, and sources. This
        constructor allows the customization or application of filtering logic for
        different aspects such as topics, statements, and source metadata within a
        specific context. Default filter functions and a source filter related
        configuration are provided if no custom logic is supplied.

        Args:
            topic_filter_fn (Callable[[str], bool], optional): The filtering function
                applied to topics. If not provided, a default topic filtering function
                (`DEFAULT_BUILD_FILTER`) is used.
            statement_filter_fn (Callable[[str], bool], optional): The filtering
                function applied to statements. If not provided, a default statement
                filtering function (`DEFAULT_BUILD_FILTER`) is used.
            source_filters (Optional[MetadataFiltersType], optional): The filtering or
                configuration object applied to source metadata. If not provided, a
                `FilterConfig` object is created with `source_filters` passed in.
        """
        super().__init__(
            topic_filter_fn = topic_filter_fn or DEFAULT_BUILD_FILTER,
            statement_filter_fn = statement_filter_fn or DEFAULT_BUILD_FILTER,
            source_filters = FilterConfig(source_filters)
        )

    def ignore_topic(self, topic:str) -> bool:
        """
        Determines whether a given topic should be ignored by applying a filter function.

        This method uses a predefined filter function to decide if a given topic should
        be ignored. If the filter function evaluates the topic as suitable for ignoring,
        it logs a corresponding debug message and returns True.

        Args:
            topic (str): The topic to be checked against the filter function.

        Returns:
            bool: True if the topic should be ignored (based on the filter function),
            False otherwise.
        """
        result = self.topic_filter_fn(topic)
        if result:
            logger.debug(f'Ignore topic: {topic}')
        return result
    
    def ignore_statement(self, statement:str) -> bool:
        """
        Determines whether a given statement should be ignored based on a filtering
        function and logs the ignored statement.

        This method evaluates a statement using a predefined filtering function. If the
        statement is deemed to require ignoring, it logs the action and returns a boolean
        result indicating whether the statement was ignored.

        Args:
            statement: The statement to evaluate against the filtering function.

        Returns:
            bool: True if the statement is to be ignored, False otherwise.
        """
        result = self.statement_filter_fn(statement)
        if result:
            logger.debug(f'Ignore statement: {statement}')
        return result
    
    def filter_source_metadata_dictionary(self, d:Dict[str, Any]) -> bool:
        """
        Filters the given source metadata dictionary through predefined source
        filters, and returns a boolean indicating if it passes the filtering
        criteria.

        Args:
            d (Dict[str, Any]): The source metadata dictionary to be filtered.

        Returns:
            bool: True if the dictionary passes the filtering criteria, False
            otherwise.
        """
        return self.source_filters.filter_source_metadata_dictionary(d)

