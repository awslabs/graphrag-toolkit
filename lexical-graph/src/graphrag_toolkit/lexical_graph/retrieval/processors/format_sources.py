# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, Callable, Union, List, Any
from string import Template

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Source

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

SourceInfoTemplateType = Union[str, Template]

def default_source_formatter_fn(source:Source):
    """
    Formats the source into a readable string representation.

    The function examines the metadata of the given `Source` object and formats
    its content into a string. If metadata exists, it processes the metadata
    values into a sorted list (in descending order by length). It constructs a
    formatted string using the longest metadata value followed by the rest of
    the values enclosed in parentheses. If metadata is empty, it defaults to
    returning the `sourceId` property of the `Source`.

    Args:
        source (Source): The source object containing metadata and sourceId.

    Returns:
        str: A formatted string representation of the source.
    """
    if source.metadata:
        source_strs = [str(v) for v in source.metadata.values()]
        source_strs.sort(key=len, reverse=True)
        if len(source_strs) > 1:
            return f"{source_strs[0]} ({', '.join(source_strs[1:])})"
        else:
            return source_strs[0]
    else:
        return source.sourceId

def source_info_template(template:SourceInfoTemplateType) -> Callable[[Dict[str, Any]], str]:
    """
    Generates a function to apply a string template to the metadata of a source.

    This function takes a template, which can be either a string or an instance of
    the Template class, and produces a function that formats a source's metadata
    according to that template. If the provided template is a string, it will be
    converted to an instance of the Template class. The resulting function can be
    used to create formatted strings by applying the template to the source's
    metadata.

    Args:
        template: A template string or a Template object that defines how the
            source's metadata should be formatted.

    Returns:
        Callable[[Dict[str, Any]], str]: A function that formats source metadata
        using the provided template.
    """
    t = template if isinstance(template, Template) else Template(template)
    def source_info_template_fn(source:Source) -> str:
        return t.safe_substitute(source.metadata)
    return source_info_template_fn

def source_info_keys(keys:List[str]) -> Callable[[Dict[str, Any]], str]:
    """
    Generates a function that retrieves the value of the first matching key from
    the metadata of a given source.

    This function factory takes a list of keys and creates a function that, when
    provided with a source object, searches through the source's metadata for those
    keys. If a key is found, it returns the associated value. If none of the keys
    are found in the metadata, it returns None.

    Args:
        keys (List[str]): A list of keys to search for in the metadata of the
            source.

    Returns:
        Callable[[Dict[str, Any]], str]: A function that, when given a source
            object, retrieves the value of the first matching key found in the
            metadata.
    """
    def source_info_keys_fn(source:Source) -> str:
        """
        Creates a function that retrieves the value of the first available key from
        a list of keys in the metadata of a given source.

        This function returns a callable that, when executed with a given source,
        iterates through the specified keys and retrieves the value associated with
        the first key that exists in the source's metadata. If none of the keys exists
        in the metadata, it returns None.

        Args:
            keys (List[str]): A list of keys to search for in the source's metadata.

        Returns:
            Callable[[Dict[str, Any]], str]: A callable function that takes a source
            object as an argument and returns the value corresponding to the first
            found key in the source's metadata, or None if no key is found.
        """
        for key in keys:
            if key in source.metadata:
                return source.metadata[key]
        return None
    return source_info_keys_fn

class FormatSources(ProcessorBase):
    """
    Handles formatting of search result sources based on the specified formatting
    logic.

    This class provides functionality to format the source field of search results
    using a customizable formatter. The formatter can be a string, list, callable,
    or template, allowing flexibility in how source formatting is achieved. It is
    primarily used to process search results and adapt the source information based
    on the provided configuration.

    Attributes:
        formatter_fn (Callable): A callable function that defines the logic for
            formatting the sources of search results. Initialized based on the
            provided formatter configuration.
    """
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        """
        Initializes a new instance of the class responsible for handling source
        formatter logic. The initialization process determines the proper formatter
        function based on the provided arguments and configurations. This function
        sets up the formatter logic, ensuring that the correct format handling for
        various input configurations is achieved.

        Args:
            args (ProcessorArgs): Object containing the processor arguments. The
                configuration for processing is determined based on the `source_formatter`
                attribute.
            filter_config (FilterConfig): Configuration object used as a filtering setup.
                This might influence or handle aspects of the data processing based on
                the provided filter logic.
        """
        super().__init__(args, filter_config)

        formatter = self.args.source_formatter or default_source_formatter_fn

        fn = None

        if isinstance(formatter, str):
            fn = source_info_template(formatter) if '$' in formatter else source_info_keys([formatter])
        elif isinstance(formatter, list):
            fn = source_info_keys(formatter)
        elif isinstance(formatter, Template):
            fn = source_info_template(formatter)
        elif isinstance(formatter, Callable):
            fn = formatter
        else:
            fn = default_source_formatter_fn

        self.formatter_fn = fn

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        """
        Processes and formats the sources in the given search results using a specified
        formatter function. The function iterates through the search results, applies
        the formatting function to each result's source attribute, and handles any
        exceptions that may occur during formatting. The modified search results are
        then returned.

        Args:
            search_results: Collection of search results to be processed.
            query: Bundle containing query information. Currently not utilized within
                this method.

        Returns:
            SearchResultCollection: The processed search results with formatted sources.
        """
        def format_source(index:int, search_result:SearchResult):
            try:
                search_result.source = self.formatter_fn(search_result.source)
            except Exception as e:
                logger.error(f'Error while formatting source: {str(e)}')
            return search_result
        
        return self._apply_to_search_results(search_results, format_source)


