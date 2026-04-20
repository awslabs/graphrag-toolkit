# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import abc
import time
from typing import List, Dict, Any
from llama_index.core.schema import BaseNode, BaseComponent

from graphrag_toolkit.lexical_graph.metadata import SourceMetadataFormatter
from graphrag_toolkit.lexical_graph.versioning import EXTRACT_TIMESTAMP, VALID_FROM, TIMESTAMP_UPPER_BOUND
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_CLASSIFICATION
from graphrag_toolkit.lexical_graph.indexing.utils.metadata_utils import remove_collection_items_from_metadata

class NodeBuilder(BaseComponent):
    """
    NodeBuilder is an abstract base class responsible for constructing and managing
    nodes. It provides a blueprint for creating nodes with specific attributes,
    filters, and metadata formatting.

    Detailed description of the class, its purpose, and usage. The class includes
    methods for cleaning and formatting data, as well as abstract methods that
    must be implemented by subclasses to define the class name, metadata keys,
    and the node-building logic.

    Attributes:
        id_generator (IdGenerator): Generates unique identifiers for nodes.
        build_filters (BuildFilters): Filters applied when building nodes.
        source_metadata_formatter (SourceMetadataFormatter): Formats source
            metadata for nodes.
    """
    id_generator:IdGenerator
    build_filters:BuildFilters
    source_metadata_formatter:SourceMetadataFormatter

    @classmethod
    @abc.abstractmethod
    def name(cls) -> str:
        """
        Abstract base class method that defines the contract for returning a string
        identifier. This method must be implemented by all subclasses.

        Args:
            cls: The class that this method is bound to.

        Returns:
            str: A string identifying the name or purpose specific to the subclass.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def metadata_keys(cls) -> List[str]:
        """
        Defines an abstract method to retrieve metadata keys for the class. This method
        must be implemented by any subclass to provide a list of string keys related to
        metadata attributes.

        Returns:
            List[str]: A list of metadata keys represented as strings.
        """
        pass

    @abc.abstractmethod
    def build_nodes(self, nodes:List[BaseNode], **kwargs) -> List[BaseNode]:
        """
        Abstract base class for building a list of nodes.

        This class serves as a blueprint for implementing specific operations on a list
        of nodes. The `build_nodes` method must be implemented by any concrete subclass
        to provide functionality specific to the application's requirements.
        """
        pass
    
    def _clean_id(self, s):
        """
        Cleans a given string by removing all characters that are not alphanumeric.

        This method takes a string as input and removes any character that is not
        a letter or a digit, then returns the cleaned string. The functionality
        ensures that only alphanumeric characters are preserved in the output.

        Args:
            s: The input string that needs to be cleaned.

        Returns:
            str: A new string consisting only of alphanumeric characters from the input.
        """
        return ''.join(c for c in s if c.isalnum())
        
    def _format_classification(self, classification):
        """
        Formats the given classification string if it is not empty or equal to the default classification.

        Args:
            classification: The classification string to be formatted. If it is None or matches the
                default classification, an empty string will be returned.

        Returns:
            str: A formatted classification string or an empty string if classification is None or
                matches the default classification.
        """
        if not classification or classification == DEFAULT_CLASSIFICATION:
            return ''
        else:
            return f' ({classification})'
    
    def _format_fact(self, s, sc, p, o, oc):
        """
        Formats and returns a string representation of a fact.

        Args:
            s: Subject of the fact.
            sc: Context of the subject.
            p: Predicate or relationship between the subject and object.
            o: Object of the fact.
            oc: Context of the object.

        Returns:
            A string that represents the fact in the format "subject predicate object".
        """
        return f'{s} {p} {o}'
    
    def _get_build_timestamp(self, **kwargs) -> int:
        return kwargs.get('build_timestamp', int(time.time() * 1000))
    
    def _update_metadata_with_versioning_info(self, metadata:Dict[str, Any], node:BaseNode, build_timestamp:int) -> Dict[str, Any]:
        
        extract_timestamp = node.metadata.get(EXTRACT_TIMESTAMP, build_timestamp)
        valid_from_timestamp = node.metadata.get(VALID_FROM, extract_timestamp)

        metadata['source']['versioning'] = {
            'extract_timestamp': extract_timestamp,
            'build_timestamp': build_timestamp,
            'valid_from': valid_from_timestamp,
            'valid_to': TIMESTAMP_UPPER_BOUND
        }

        return metadata

    def _get_source_info_metadata(self, source_info_metadata:Dict[str, Any]):
        results = {}
        (valid_metadata, invalid_metadata) = remove_collection_items_from_metadata(source_info_metadata)
        results['metadata'] = self.source_metadata_formatter.format(valid_metadata)
        results['invalid_metadata'] = invalid_metadata
        return results

