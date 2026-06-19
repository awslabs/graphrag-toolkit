# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import string
from typing import Any, List, Optional, Callable
import uuid

from graphrag_toolkit.lexical_graph.metadata import FilterConfig, type_name_for_key_value, format_datetime
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId

from llama_index.core.vector_stores.types import FilterCondition, FilterOperator, MetadataFilter, MetadataFilters

SEARCH_STRING_PATTERN = re.compile(r'([^\s\w]|_)+')

def new_query_var():
    return f'n{uuid.uuid4().hex}'

def search_string_from(value:str):
    """
    Removes specific patterns from a string, reduces extra spaces, and converts it to lowercase.

    This function receives a string, removes specific patterns defined by the global
    `SEARCH_STRING_PATTERN`, normalizes whitespace by replacing multiple spaces with
    a single one, and converts the resulting string to lowercase.

    Args:
        value (str): The input string to be processed.

    Returns:
        str: The processed string, with patterns removed, extra spaces reduced,
        and converted to lowercase.
    """
    value = SEARCH_STRING_PATTERN.sub(' ', value)
    while '  ' in value:
        value = value.replace('  ', ' ')
    return value.lower().strip()

def label_from(value:str):
    """
    Converts a given string into a formatted label by removing specific patterns and capitalizing words.

    This function primarily works by searching for and replacing matches of a specified pattern
    in the input string, before converting the modified string into capitalized words. Spaces
    are removed between the words to form a label-like output.

    Args:
        value (str): The input string to transform into a formatted label.

    Returns:
        str: The modified label-like output string.
    """
    if value.startswith('__') and value.endswith('__'):
        return value
    
    value = SEARCH_STRING_PATTERN.sub(' ', value)
    return string.capwords(value).replace(' ', '')

def relationship_name_from(value:str):
    """
    Generates a formatted relationship name from a given string.

    The function transforms the input string by replacing all non-alphanumeric
    characters with underscores and converts the resulting string to uppercase.

    Args:
        value (str): The input string to be processed.

    Returns:
        str: A formatted string where non-alphanumeric characters are replaced
        with underscores and all characters are in uppercase.
    """
    return ''.join([ c if c.isalnum() else '_' for c in value ]).upper()

def node_result(node_ref:str, 
                node_id:Optional[NodeId]=None, 
                properties:Optional[List[str]]=['*'], 
                key_name:Optional[str]=None):
    """
    Generates a formatted result string based on the provided node reference, node ID,
    properties, and optional key name. This can be used to specify desired details to
    fetch or represent related nodes and their attributes in a structured format.

    Args:
        node_ref (str): The reference name of the node.
        node_id (Optional[NodeId]): An optional ID for the node, which may influence
            the resulting properties based on whether the ID is property-based.
        properties (Optional[List[str]]): A list of property names to include. By default,
            includes all properties ('*'). If specific properties are provided, property-specific
            selectors will be constructed unless the provided ID conflicts.
        key_name (Optional[str]): An optional key name to override the default generated key.
            If None, the key is derived from `node_ref`.

    Returns:
        str: A formatted string combining the node reference, key, and selected properties.

    """
    key = key_name or node_ref
    
    property_selectors = []
    
    if node_id:
        if node_id.is_property_based:
            if node_id.key not in properties and '*' not in properties:
                property_selectors.append(f'.{node_id.key}')
        else:
            property_selectors.append(f'{node_id.key}: {node_id}')
    
    property_selectors.extend(['.{}'.format(p) for p in properties])
        
    return f'{key}: {node_ref}{{{", ".join(property_selectors)}}}'

def to_opencypher_operator(operator: FilterOperator) -> tuple[str, Callable[[Any], str]]:
    """
    Converts a given filter operator into its corresponding OpenCypher operator and value formatter.

    This function maps an instance of `FilterOperator` to a tuple containing the
    OpenCypher operator string and a callable for formatting values associated
    with that operator. If the given operator is not supported, the function raises
    a `ValueError`.

    Args:
        operator: The filter operator to convert. Expected to be an instance of
            `FilterOperator`.

    Returns:
        A tuple where:
        - The first element is a string representing the corresponding OpenCypher
          operator.
        - The second element is a callable, representing a function used to format
          the value for OpenCypher queries.

    Raises:
        ValueError: If the provided operator is not supported.
    """
    default_value_formatter = lambda x: x
    
    operator_map = {
        FilterOperator.EQ: ('=', default_value_formatter), 
        FilterOperator.GT: ('>', default_value_formatter), 
        FilterOperator.LT: ('<', default_value_formatter), 
        FilterOperator.NE: ('<>', default_value_formatter), 
        FilterOperator.GTE: ('>=', default_value_formatter), 
        FilterOperator.LTE: ('<=', default_value_formatter), 
        #FilterOperator.IN: ('in', default_value_formatter),  # In array (string or number)
        #FilterOperator.NIN: ('nin', default_value_formatter),  # Not in array (string or number)
        #FilterOperator.ANY: ('any', default_value_formatter),  # Contains any (array of strings)
        #FilterOperator.ALL: ('all', default_value_formatter),  # Contains all (array of strings)
        FilterOperator.TEXT_MATCH: ('CONTAINS', default_value_formatter),
        FilterOperator.TEXT_MATCH_INSENSITIVE: ('CONTAINS', lambda x: x.lower()),
        #FilterOperator.CONTAINS: ('contains', default_value_formatter),  # metadata array contains value (string or number)
        FilterOperator.IS_EMPTY: ('IS NULL', default_value_formatter),  # the field is not exist or empty (null or empty array)
    }

    if operator not in operator_map:
        raise ValueError(f'Unsupported filter operator: {operator}')
    
    return operator_map[operator]

def formatter_for_type(type_name:str) -> Callable[[Any], str]:
    """
    Determines and returns a formatting function based on the provided type name.

    This function takes a `type_name` as input and returns a corresponding
    lambda function that formats the provided value into a specific format
    associated with the type. Supported type names include 'text', 'timestamp',
    and 'number'. If the type name does not match one of the supported types,
    a `ValueError` is raised.

    Args:
        type_name: The name of the type for which the formatter is required.
            Valid values include 'text', 'timestamp', and 'number'.

    Returns:
        Callable[[Any], str]: A lambda function that formats the input value
        according to the provided `type_name`.

    Raises:
        ValueError: If an unsupported type name is provided.
    """
    if type_name == 'text':
        return lambda x: f"'{x}'"
    elif type_name == 'timestamp':
        return lambda x: f"datetime('{format_datetime(x)}')"
    elif type_name in ['number', 'int', 'float']:
        return lambda x:x
    else:
        raise ValueError(f'Unsupported type name: {type_name}')

def parse_metadata_filters_recursive(metadata_filters:MetadataFilters) -> str:
    """
    Parses a `MetadataFilters` object into an OpenCypher filter string.

    This function recursively processes `MetadataFilters` and `MetadataFilter` objects
    to construct a representation suitable for OpenCypher query language based on
    filter conditions ('AND', 'OR', 'NOT') and operator transformations. The resulting
    string can be directly incorporated into OpenCypher queries for filtering nodes
    or relationships.

    Args:
        metadata_filters (MetadataFilters): The filter structure to be parsed, which
            defines the conditions (`AND`, `OR`, `NOT`) and associated filters or nested
            filters to be converted into OpenCypher query format.

    Returns:
        str: The constructed OpenCypher filter string generated from the input metadata
        filter structure.

    Raises:
        ValueError: If the metadata filter structure contains unexpected or invalid
            types, or if an unsupported filter condition is encountered.
    """
    def to_key(key: str) -> str:
        """
        Recursively parses metadata filters and converts them into a formatted string.

        This function takes a MetadataFilters object and iteratively processes it,
        transforming its content into a specific string format for further usage.

        Args:
            metadata_filters: The MetadataFilters object that contains the filtering
                criteria to be processed.

        Returns:
            A string that represents the parsed and formatted metadata filters based
            on the provided input.

        """
        return f"source.{key}"
    
    def metadata_filter_to_opencypher_filter(f: MetadataFilter) -> str:
        """
        Parses a `MetadataFilters` object recursively into a string representation usable as an OpenCypher filter expression.

        This function processes a given `MetadataFilters` object by iterating through its filters
        and recursively transforming them into OpenCypher-compatible filter expressions. The helper
        function `metadata_filter_to_opencypher_filter` ensures that each individual filter is properly
        formatted based on its key, operator, and value.

        Args:
            metadata_filters (MetadataFilters): The metadata filters to be recursively parsed into an
                OpenCypher-compatible string representation.

        Returns:
            str: The OpenCypher-compatible filter expression string after parsing the input
                `MetadataFilters` object.
        """
        key = to_key(f.key)
        (operator, operator_formatter) = to_opencypher_operator(f.operator)

        if f.operator == FilterOperator.IS_EMPTY:
            return f"({key} {operator})"
        else:
            type_name = type_name_for_key_value(f.key, f.value)
            type_formatter = formatter_for_type(type_name)
            if f.operator == FilterOperator.TEXT_MATCH_INSENSITIVE:
                return f"({key}.toLower() {operator} {type_formatter(operator_formatter(str(f.value)))})"
            else:
                return f"({key} {operator} {type_formatter(operator_formatter(str(f.value)))})"
 
    
    condition = metadata_filters.condition.value

    filter_strs = []

    for metadata_filter in metadata_filters.filters:
        if isinstance(metadata_filter, MetadataFilter):
            if metadata_filters.condition == FilterCondition.NOT:
                raise ValueError(f'Expected MetadataFilters for FilterCondition.NOT, but found MetadataFilter')
            filter_strs.append(metadata_filter_to_opencypher_filter(metadata_filter))
        elif isinstance(metadata_filter, MetadataFilters):
            filter_strs.append(parse_metadata_filters_recursive(metadata_filter))
        else:
            raise ValueError(f'Invalid metadata filter type: {type(metadata_filter)}')
        
    if metadata_filters.condition == FilterCondition.NOT:
        return f"(NOT {' '.join(filter_strs)})"
    elif metadata_filters.condition == FilterCondition.AND or metadata_filters.condition == FilterCondition.OR:
        condition = f' {metadata_filters.condition.value.upper()} '
        return f"({condition.join(filter_strs)})"
    else:
        raise ValueError(f'Unsupported filters condition: {metadata_filters.condition}')


def filter_config_to_opencypher_filters(filter_config:FilterConfig) -> str:
    """
    Converts filter configuration into a string containing filters in OpenCypher format.

    This function takes a `FilterConfig` object as input, which may include a set of
    source filters. The representation of the filters is recursively constructed
    and returned as a string formatted for OpenCypher queries. If the filter
    configuration or its source filters are not provided, an empty string is
    returned.

    Args:
        filter_config (FilterConfig): The configuration object containing source
            filters to be converted into OpenCypher format.

    Returns:
        str: The OpenCypher formatted filters as a string. Returns an empty string
            if no filters are provided or the filter configuration is invalid.
    """
    if filter_config is None or filter_config.source_filters is None:
        return ''
    return parse_metadata_filters_recursive(filter_config.source_filters)
    