# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from graphrag_toolkit.lexical_graph import TenantId, GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.utils.hash_utils import get_hash
from graphrag_toolkit.lexical_graph.utils.arg_utils import first_non_none

from llama_index.core.bridge.pydantic import BaseModel

class IdGenerator(BaseModel):
    """
    A class responsible for generating unique and tenant-specific identifiers.

    The `IdGenerator` class is designed to create various types of identifiers,
    such as source IDs, chunk IDs, node IDs, and tenant-specific rewritten IDs.
    It uses hashing techniques for creating compact and unique representations
    of input values. The class also integrates with a tenant system to ensure
    that identifiers are applied in the context of a particular tenant.

    Attributes:
        tenant_id (TenantId): The tenant context that is used for generating
            tenant-specific IDs and rewriting ID values.
        use_chunk_id_delimiter (bool): Whether to use delimiter in chunk ID hashing
            to prevent boundary collisions. Defaults to False for backward compatibility.
    """
    tenant_id:TenantId
    include_classification_in_entity_id:bool
    use_chunk_id_delimiter:bool

    def __init__(self, tenant_id:TenantId=None, include_classification_in_entity_id:bool=None, use_chunk_id_delimiter:bool=False):
        """
        Initialize the IdGenerator.

        Args:
            tenant_id: The tenant context for generating tenant-specific IDs.
            include_classification_in_entity_id: Whether to include classification in entity IDs.
            use_chunk_id_delimiter: Whether to use delimiter in chunk ID hashing to prevent
                boundary collisions. Defaults to False for backward compatibility with existing
                graphs. Set to True for new graphs to enable collision-resistant hashing.
        """
        super().__init__(
            tenant_id=tenant_id or TenantId(),
            include_classification_in_entity_id=first_non_none([include_classification_in_entity_id, GraphRAGConfig.include_classification_in_entity_id]),
            use_chunk_id_delimiter=use_chunk_id_delimiter
        )

    def _get_hash(self, s):
        """
        Generates an MD5 hash for a given string.

        This private method computes the MD5 hash of a provided string and returns its
        hexadecimal representation. It is used internally to generate unique hashed
        values based on string inputs.

        Args:
            s: The input string to be hashed.

        Returns:
            The hexadecimal representation of the MD5 hash of the input string.
        """
        return get_hash(s)

    def create_source_id(self, text:str, metadata_str:str):
        """
        Generates a unique source identifier by combining hashed representations of a text
        and its associated metadata.

        This function is part of an identifier creation mechanism that hashes input strings
        and formats the resulting substrings into a specific pattern for source identification.
        The creation of the source ID involves truncating the hashed values to predefined lengths
        for compactness and uniqueness.

        Args:
            text: The primary content or body for which the source identifier is generated.
            metadata_str: Additional metadata or descriptive information related to the primary content.

        Returns:
            str: A formatted string representing the unique source identifier, combining
            hashed substrings derived from the input text and metadata.

        """
        return f"aws::{self._get_hash(text)[:8]}:{self._get_hash(metadata_str)[:4]}"

    # Delimiter used to separate text and metadata in chunk ID hashing.
    # Using null byte as it cannot appear in valid UTF-8 text strings.
    _CHUNK_ID_DELIMITER = '\x00'

    def create_chunk_id(self, source_id:str, text:str, metadata_str:str):
        """
        Generates a unique chunk identifier by combining a source ID, a hash of the given text,
        and associated metadata.

        When use_chunk_id_delimiter is enabled, the text and metadata are separated by a null
        byte delimiter before hashing to prevent boundary collision issues where different
        (text, metadata_str) pairs could produce identical concatenated strings.

        For backward compatibility with existing graphs, the delimiter is disabled by default.
        Enable it for new graphs by setting use_chunk_id_delimiter=True during initialization.

        Args:
            source_id (str): The identifier of the source content.
            text (str): The primary content or text that needs to be identified.
            metadata_str (str): The metadata string used for constructing the identifier.

        Returns:
            str: A uniquely generated chunk identifier based on the given inputs.
        """
        if self.use_chunk_id_delimiter:
            # New behavior: Use delimiter to prevent boundary collisions
            hash_input = text + self._CHUNK_ID_DELIMITER + metadata_str
        else:
            # Old behavior: Direct concatenation (preserves existing chunk IDs)
            hash_input = text + metadata_str

        return f'{source_id}:{self._get_hash(hash_input)[:8]}'

    def rewrite_id_for_tenant(self, id_value:str):
        """
        Rewrites the provided ID with the tenant-specific ID format.

        This method utilizes the tenant's specific implementation of ID
        rewriting to convert the given ID to a tenant-specific format.

        Args:
            id_value: The original ID to be rewritten.

        Returns:
            str: The tenant-specific rewritten ID.
        """
        return self.tenant_id.rewrite_id(id_value)

    def create_topic_id(self, source_id:str, topic_value:str) -> str:
        return self._create_node_id('topic', source_id, topic_value)

    def create_statement_id(self, topic_id:str, statement_value:str) -> str:
        return self._create_node_id('statement', topic_id, statement_value)

    def create_fact_id(self, fact_value:str) -> str:
        return self._create_node_id('fact', fact_value)

    def create_local_entity_id(self, source_id:str, entity_value:str) -> str:
        return self._create_node_id('local-entity', entity_value, source_id)

    def create_entity_id(self, entity_value:str, entity_classification:str) -> str:
        if self.include_classification_in_entity_id:
            return self._create_node_id('entity', entity_value, entity_classification)
        else:
            return self._create_node_id('entity', entity_value)

    def _create_node_id(self, node_type:str, v1:str, v2:Optional[str]=None) -> str:
        """
        Creates a unique identifier for a specific node based on the provided parameters.

        The function generates a hashable string using the `node_type`, `v1`, and
        optionally `v2` parameters. The parameters are formatted to lower case and
        spaces are replaced with underscores for consistency. If `v2` is provided,
        it is included in the resulting identifier; otherwise, it is excluded.

        Args:
            node_type: A string representing the type of the node.
            v1: A string specifying the first variable or identifier in the node's identity.
            v2: An optional string specifying the second variable or identifier
                in the node's identity.

        Returns:
            A string containing a unique hash-based identifier for the node.
        """
        if v2:
            return self._get_hash(self.tenant_id.format_hashable(f"{node_type.lower()}::{v1.lower().replace(' ', '_')}::{v2.lower().replace(' ', '_')}"))
        else:
            return self._get_hash(self.tenant_id.format_hashable(f"{node_type.lower()}::{v1.lower().replace(' ', '_')}"))
