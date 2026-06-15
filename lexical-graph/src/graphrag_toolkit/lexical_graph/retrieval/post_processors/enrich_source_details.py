# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from string import Template
from typing import List, Union, Dict, Any, Callable

from graphrag_toolkit.lexical_graph.retrieval.model import SearchResult

from graphrag_toolkit.core.postprocessor import PostProcessor
from graphrag_toolkit.core.types import NodeWithScore, QueryBundle

SourceInfoTemplateType = Union[str, Template]
SourceInfoAccessorType = Union[str, List[str], Template, Callable[[Dict[str, Any]], str]]

def source_info_template(template:SourceInfoTemplateType) -> Callable[[Dict[str, Any]], str]:
    """
    Generates a function for formatting source information using a provided template.

    This function returns another function that, when called with a dictionary of
    source properties, populates a string template with the values from the dictionary,
    replacing placeholders in the template. The function ensures that the template is
    evaluated safely without raising exceptions for missing placeholders.

    Args:
        template (SourceInfoTemplateType): A string template or a `Template` object
            that defines the format for formatting source information.

    Returns:
        Callable[[Dict[str, Any]], str]: A function that takes a dictionary of source
        properties as input and returns a formatted string with placeholders replaced
        by corresponding dictionary values.
    """
    t = template if isinstance(template, Template) else Template(template)
    def source_info_template_fn(source_properties:Dict[str, Any]) -> str:
        """
        Dynamically creates a string formatting function based on the provided template. This
        function is meant to substitute placeholder variables in the template with values from
        a dictionary of source properties.

        Args:
            template: A SourceInfoTemplateType object that represents the string template
                with placeholder fields for substitution.

        Returns:
            Callable[[Dict[str, Any]], str]: A function that takes a dictionary of source
                properties as input and returns a formatted string with placeholders replaced
                by corresponding dictionary values.
        """
        return t.safe_substitute(source_properties)
    return source_info_template_fn

def source_info_keys(keys:List[str]) -> Callable[[Dict[str, Any]], str]:
    """
    Generates a function that retrieves the value of the first matching key from a dictionary.

    The generated function iterates through a list of specified keys, searching for the first
    key present in a given dictionary and returning its associated value. If none of the keys
    are found in the dictionary, the function returns None.

    Args:
        keys (List[str]): A list of keys to search for in the dictionary.

    Returns:
        Callable[[Dict[str, Any]], str]: A function that takes a dictionary as input and returns
        the value of the first matching key, or None if no match is found.
    """
    def source_info_keys_fn(source_properties:Dict[str, Any]) -> str:
        """
        Generates a function that retrieves the value associated with the first matching key
        in a dictionary from a predefined list of keys.

        The returned function iterates through a list of keys and checks if any of these keys
        exists in the given dictionary. If a match is found, the corresponding value is returned.
        If none of the keys are found in the dictionary, it returns None.

        Args:
            keys (List[str]): A list of key names to look for in the given dictionary.

        Returns:
            Callable[[Dict[str, Any]], str]: A function that, when provided with a dictionary,
            returns the value associated with the first matching key in the `keys` list, or None
            if no match is found.
        """
        for key in keys:
            if key in source_properties:
                return source_properties[key]
        return None
    return source_info_keys_fn

class EnrichSourceDetails(PostProcessor):
    """
    This class is responsible for enriching source details in nodes.

    Provides functionality to modify and process the source details of nodes based
    on specified accessors, such as templates, key mappings, or callable
    functions. It is primarily designed to assist in postprocessing nodes to
    ensure their source information is properly formatted or updated before further
    usage.

    Attributes:
        source_info_accessor (SourceInfoAccessorType): A variable determining how
            the source information is accessed or modified. It can take various
            forms such as a string, list, template, or callable.
    """
    source_info_accessor:SourceInfoAccessorType=None

    def __init__(self, source_info_accessor: SourceInfoAccessorType = None):
        self.source_info_accessor = source_info_accessor

    @classmethod
    def class_name(cls) -> str:
        """
        Determines and returns the name of the class to represent the source details.

        This method is typically used to provide a standardized identifier for the
        class, which can be utilized in different parts of the application where the
        class name is required.

        Returns:
            str: The name of the class as a string.
        """
        return 'EnrichSourceDetails'
    
    def _get_source_info(self, source_metadata, source) -> str:
        """
        Retrieves source information based on metadata and configured accessor.

        This method processes the provided `source_metadata` and attempts to extract
        specific source information using the defined `source_info_accessor`. If the
        accessor is not defined, the original `source` is returned. The accessor can be
        a string pattern, a list of keys, a template, or a callable function. The
        appropriate method of extraction is determined dynamically based on the type of
        `source_info_accessor`.

        Args:
            source_metadata: Metadata associated with the source, used to determine the
                source information.
            source: Fallback source information in case no accessor is defined or the
                extraction process fails.

        Returns:
            str: Extracted source information if successful, otherwise the original
            `source` value.
        """
        accessor = self.source_info_accessor
        
        if not accessor:
            return source

        if isinstance(accessor, str):
            fn = source_info_template(accessor) if '$' in accessor else source_info_keys([accessor])
        if isinstance(accessor, list):
            fn = source_info_keys(accessor)
        if isinstance(accessor, Template):
            fn = source_info_template(accessor)
        if isinstance(accessor, Callable):
            fn = accessor

        source_info = fn(source_metadata)

        return source_info or source

    
    def process(
        self,
        nodes: list[NodeWithScore],
        query: QueryBundle,
    ) -> list[NodeWithScore]:
        """
        Postprocesses a list of nodes to retrieve and update source information and search
        result details.

        This method processes a list of nodes by validating the JSON content within each
        node's text as a SearchResult object. It retrieves source metadata and updates the
        SearchResult's source information by invoking a helper method. The processed data
        is then serialized back to JSON format and updated within each node.

        Args:
            nodes: A list of NodeWithScore objects whose text contains the JSON representation
                of a SearchResult object to be validated and updated.
            query: A QueryBundle object providing additional context for
                the nodes being postprocessed.

        Returns:
            A list of NodeWithScore objects where the SearchResult within each node has
            been updated with resolved source data.
        """
        for node in nodes:
            search_result = SearchResult.model_validate_json(node.node.text)
            source_metadata = node.metadata.get('source', {}).get('metadata', {})
            if source_metadata:
                source_info = self._get_source_info(source_metadata, search_result.source.sourceId)
                search_result.source = str(source_info)
                node.node.text = search_result.model_dump_json(exclude_none=True, exclude_defaults=True, indent=2)

        return nodes

