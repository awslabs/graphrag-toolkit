# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ETL Schema Model Module for Document Graph Operations.

This module defines the data models used for ETL (Extract, Transform, Load) operations
in the document graph system. It provides a comprehensive set of Pydantic models that
represent different aspects of the ETL process:

1. Extract: Models for configuring data extraction from various sources
2. Transform: Models for text chunking, metadata mapping, entity extraction, and normalization
3. Load: Models for defining graph nodes, relationships, and loading configurations

These models are used throughout the document graph system to define how documents
are processed, transformed, and loaded into graph databases. The models are designed
to be extensible, allowing for custom configurations through the use of Pydantic's
extra fields feature.

The top-level ETLSchema class combines all these components into a complete schema
that can be serialized to/from JSON and used to configure the entire ETL pipeline.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Optional
from pydantic import BaseModel, ConfigDict


# Extract section
class ExtractConfig(BaseModel):
    """
    Configuration for data extraction from various sources.
    
    This class defines the parameters needed to extract data from different source types,
    such as S3 buckets, local files, or APIs. It specifies where to find the data and
    how to read it.
    
    Attributes:
        source_type (str): The type of data source (e.g., "s3", "html", "csv", "api").
        bucket (Optional[str]): The S3 bucket name for S3 sources.
        prefix (Optional[str]): The S3 prefix/folder path for S3 sources.
        key (Optional[str]): The specific S3 key or file path.
        file_type (Optional[str]): The type of file to extract (e.g., "pdf", "docx").
        reader (Optional[str]): The reader to use for extraction (e.g., "pymupdf").
        delimiter (Optional[str]): The delimiter for CSV or similar files.
        encoding (Optional[str]): The character encoding of the source file.
        
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to add source-specific parameters like API tokens or headers.
    """
    source_type: str  # Allow user-defined types (e.g., "html", "csv", "api")
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    key: Optional[str] = None
    file_type: Optional[str] = None
    reader: Optional[str] = None
    delimiter: Optional[str] = None
    encoding: Optional[str] = None

    model_config = ConfigDict(extra="allow")


# Transform section
class ChunkingConfig(BaseModel):
    """
    Configuration for chunking text during document transformation.
    
    This class defines how documents should be divided into smaller, manageable chunks
    for processing and storage. Different chunking strategies can be used depending on
    the document structure and requirements.
    
    Attributes:
        strategy (str): The chunking strategy to use (e.g., "by_heading", "fixed_length", 
                       "by_customer").
        min_length (Optional[int]): The minimum length of a chunk in characters or tokens,
                                   defaulting to 100.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to add strategy-specific parameters like maximum chunk size or
        overlap settings.
    """
    strategy: str  # e.g., "by_heading", "fixed_length", "by_customer"
    min_length: Optional[int] = 100

    model_config = ConfigDict(extra="allow")


class MetadataMapping(BaseModel):
    """
    Field-level mapping of document metadata to standardized fields.
    
    This class defines how metadata fields from source documents should be mapped
    to standardized fields in the document graph. It allows for consistent metadata
    representation across different document types and sources.
    
    Attributes:
        title (Optional[str]): Field path or expression to extract the document title.
        author (Optional[str]): Field path or expression to extract the document author.
        created_date (Optional[str]): Field path or expression to extract the document creation date.
        category (Optional[str]): Field path or expression to extract the document category.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to map any custom metadata fields specific to their document types.
        Field values can be direct field names or dot-notation paths (e.g., "metadata.author").
    """
    title: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[str] = None
    category: Optional[str] = None  # e.g., for classification purposes

    model_config = ConfigDict(extra="allow")


class EntityExtractionConfig(BaseModel):
    """
    Configuration for extracting named entities from document text.
    
    This class defines how named entities (such as people, organizations, locations)
    should be extracted from document text during the transformation phase. It specifies
    the extraction method, model to use, and which entity types to extract.
    
    Attributes:
        method (str): The entity extraction method to use (e.g., "ner", "regex", "rule_based").
        model (Optional[str]): The specific model to use for extraction (e.g., "spacy_en_core_web_lg").
        extract_emails (Optional[bool]): Whether to extract email addresses, defaults to False.
        extract_dates (Optional[bool]): Whether to extract date references, defaults to False.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to specify additional entity types to extract or method-specific
        parameters like confidence thresholds.
    """
    method: str  # e.g., "ner", "regex", "rule_based"
    model: Optional[str] = None
    extract_emails: Optional[bool] = False
    extract_dates: Optional[bool] = False

    model_config = ConfigDict(extra="allow")


class NormalizeConfig(BaseModel):
    """
    Configuration for text normalization during document processing.
    
    This class defines how document text should be normalized during the transformation
    phase. Normalization can include removing headers, standardizing whitespace,
    standardizing date formats, and other text cleaning operations.
    
    Attributes:
        remove_headers (bool): Whether to remove headers from the text, defaults to True.
        collapse_whitespace (bool): Whether to normalize whitespace (removing extra spaces,
                                   newlines, etc.), defaults to True.
        standardize_dates (Optional[bool]): Whether to convert dates to a standard format,
                                          defaults to False.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to specify additional normalization operations specific to their
        document types or requirements.
    """
    remove_headers: bool = True
    collapse_whitespace: bool = True
    standardize_dates: Optional[bool] = False

    model_config = ConfigDict(extra="allow")


class TransformConfig(BaseModel):
    """
    Complete configuration for document transformation operations.
    
    This class combines all the transformation-related configurations into a single
    comprehensive configuration for the transform phase of the ETL process. It includes
    settings for chunking, metadata mapping, entity extraction, and text normalization.
    
    Attributes:
        chunking (ChunkingConfig): Configuration for how to chunk the document text.
        metadata_mapping (MetadataMapping): Configuration for mapping document metadata fields.
        entity_extraction (EntityExtractionConfig): Configuration for extracting named entities.
        normalize (NormalizeConfig): Configuration for text normalization operations.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to add custom transformation configurations not covered by the
        standard components.
    """
    chunking: ChunkingConfig
    metadata_mapping: MetadataMapping
    entity_extraction: EntityExtractionConfig
    normalize: NormalizeConfig

    model_config = ConfigDict(extra="allow")


# Load section
class NodeDefinition(BaseModel):
    """
    Definition of a graph node type and its associated fields.
    
    This class defines a type of node in the document graph and specifies which fields
    should be included in nodes of this type. It is used in the load phase to determine
    how to create nodes in the graph database.
    
    Attributes:
        type (str): The type or label of the node (e.g., "DocumentNode", "SectionNode").
        fields (List[str]): List of field names to include in nodes of this type.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to specify additional node properties like indexes or constraints.
    """
    type: str
    fields: List[str]

    model_config = ConfigDict(extra="allow")


class RelationshipDefinition(BaseModel):
    """
    Defines a relationship (edge) between two node types in the document graph.
    
    This class specifies how nodes in the graph should be connected to each other,
    defining the type of relationship and which nodes should be the source and target
    of the relationship. It is used in the load phase to create edges in the graph database.
    
    Attributes:
        type (str): The type or label of the relationship (e.g., "has_section", "references").
        source (str): The identifier for the source node of the relationship.
        target (str): The identifier for the target node of the relationship.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to specify additional relationship properties like weights or
        directional constraints.
    """
    type: str
    source: str
    target: str

    model_config = ConfigDict(extra="allow")


class LoadConfig(BaseModel):
    """
    Configuration for loading processed documents into a graph database.
    
    This class defines how processed document data should be loaded into a graph database,
    specifying the node types, their fields, and the relationships between them. It is
    used in the load phase of the ETL process to create the document graph structure.
    
    Attributes:
        document_node (NodeDefinition): Definition of the document node type and its fields.
        section_node (NodeDefinition): Definition of the section node type and its fields.
        relationships (List[RelationshipDefinition]): List of relationships between nodes
                                                     in the graph.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to specify additional loading configurations like batch sizes,
        transaction settings, or custom node types beyond the standard document and
        section nodes.
    """
    document_node: NodeDefinition
    section_node: NodeDefinition
    relationships: List[RelationshipDefinition]

    model_config = ConfigDict(extra="allow")


# Top-level schema
class ETLSchema(BaseModel):
    """
    Complete configuration for ETL (Extract, Transform, Load) processing of document graphs.
    
    This is the top-level schema class that combines all aspects of the ETL process into
    a single comprehensive configuration. It includes configurations for extracting data
    from sources, transforming the data through various operations, and loading the
    processed data into a graph database.
    
    Attributes:
        schema_id (str): A unique identifier for this schema.
        description (Optional[str]): A human-readable description of the schema's purpose.
        extract (ExtractConfig): Configuration for the extract phase.
        transform (TransformConfig): Configuration for the transform phase.
        load (LoadConfig): Configuration for the load phase.
    
    Note:
        This class allows additional custom fields through the `extra = "allow"` config,
        enabling users to add custom configurations not covered by the standard components.
        
    Example:
        ```python
        schema = ETLSchema(
            schema_id="pdf-document-schema",
            description="Schema for processing PDF documents",
            extract=ExtractConfig(source_type="s3", bucket="documents", file_type="pdf"),
            transform=TransformConfig(...),
            load=LoadConfig(...)
        )
        ```
    """
    schema_id: str
    description: Optional[str] = None
    extract: ExtractConfig
    transform: TransformConfig
    load: LoadConfig

    model_config = ConfigDict(extra="allow")
