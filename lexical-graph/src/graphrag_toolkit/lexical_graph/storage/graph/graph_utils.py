# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional
import re
import string
import uuid

from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId

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
    value = SEARCH_STRING_PATTERN.sub('', value)
    while '  ' in value:
        value = value.replace('  ', ' ')
    return value.lower()

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
    