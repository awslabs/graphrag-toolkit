# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.versioning import TIMESTAMP_UPPER_BOUND, TIMESTAMP_LOWER_BOUND

from pydantic import BaseModel, ConfigDict, Field, AliasChoices
from typing import List, Optional, Union

class Statement(BaseModel):
    """
    Represents a statement model with associated metadata and attributes.

    This class encapsulates the concept of a statement, which includes its content,
    associated facts, optional details, scoring information, and other metadata.
    It provides a structure for organizing and managing statement-related data
    within the context of the application.

    Attributes:
        statementId (Optional[str]): The unique identifier of the statement.
        statement (str): The main content of the statement.
        facts (List[str]): A list of facts associated with the statement.
        details (Optional[str]): Additional details about the statement.
        chunkId (Optional[str]): An identifier for the chunk the statement is a part of.
        score (Optional[float]): The score associated with the statement, indicating
            its relevance or confidence.
        statement_str (Optional[str]): A string representation of the statement.
    """
    model_config = ConfigDict(strict=True)

    statementId:Optional[str]=None
    statement:str
    facts:List[str]=[]
    details:Optional[str]=None
    chunkId:Optional[str]=None
    score:Optional[float]=None
    statement_str:Optional[str]=None
    retrievers:List[str]=[]

StatementType = Union[Statement, str]

class Chunk(BaseModel):
    """
    Represents a single unit or segment of data with an associated identifier, optional value,
    and score.

    This class is used to encapsulate data chunks with a unique identifier. These chunks may
    optionally include a value and a numerical score, where the score represents some measure
    of relevance or quality. This design follows strict configuration rules as defined by its
    base configuration model.

    Attributes:
        chunkId (str): The unique identifier for the data chunk.
        value (Optional[str]): The optional value or content of the data chunk. Default is None.
        score (Optional[float]): The optional score or weight assigned to the data chunk.
            Default is None.
    """
    model_config = ConfigDict(strict=True)

    chunkId:str
    value:Optional[str]=None
    metadata:dict={}
    score:Optional[float]=None

class Topic(BaseModel):
    """
    Represents a topic within a specific context.

    This class encapsulates the details of a topic, including its name, associated
    chunks, and related statements. It is used as a fundamental data structure to
    organize and manage topic information. The `model_config` attribute ensures
    that the class operates in strict mode, enforcing stricter validation and
    runtime rules when used within the application.

    Attributes:
        model_config (ConfigDict): Configuration for the model with strict mode
            enabled.
        topic (str): The main title or name of the topic.
        chunks (List[Chunk]): A collection of chunks associated with the topic.
            By default, it is an empty list.
        statements (List[StatementType]): A collection of statements related to
            the topic. By default, it is an empty list.
    """
    model_config = ConfigDict(strict=True)

    topic:str
    topicId:str
    chunks:List[Chunk]=[]
    statements:List[StatementType]=[]  

class Versioning(BaseModel):
    model_config = ConfigDict(strict=True)

    valid_from:int = TIMESTAMP_LOWER_BOUND
    valid_to:int = TIMESTAMP_UPPER_BOUND
    extract_timestamp:int = TIMESTAMP_LOWER_BOUND
    build_timestamp:int = TIMESTAMP_LOWER_BOUND
    id_fields:List[str] = []

class Source(BaseModel):
    """
    Represents a source entity with a unique identifier and associated metadata.

    This class is used to encapsulate information about a data source, including a
    unique identifier (sourceId) and optional metadata as key-value pairs. It ensures
    that data adheres to a strict schema through its configuration.

    Attributes:
        sourceId (str): A unique identifier for the source.
        metadata (Dict[str, str]): A dictionary representing additional information
            about the source, where keys and values are both strings. Defaults to an
            empty dictionary.
    """
    model_config = ConfigDict(strict=True)
    
    sourceId:str
    metadata:dict={}
    versioning:Versioning

SourceType = Union[str, Source]

class SearchResult(BaseModel):
    """
    Represents the result of a search operation.

    This class encapsulates details about the outcome of a search operation,
    including the source of the result, associated topics, an optional single
    topic, a list of statements, and an optional score indicating relevance. It
    is designed to enforce strict model configuration validation.

    Attributes:
        source (SourceType): The origin or type of the search result.
        topics (List[Topic]): List of topics related to the search result.
        topic (Optional[str]): A single optional topic associated with the search
            result.
        statements (List[StatementType]): A collection of statements returned
            from the search.
        score (Optional[float]): The optional score indicating the relevance of
            the search result.
    """
    model_config = ConfigDict(strict=True)

    source:SourceType
    topics:List[Topic]=[]
    topic:Optional[str]=None
    statements:List[StatementType]=[]
    score:Optional[float]=None

class Entity(BaseModel):
    """
    Represents an entity with specific attributes and configuration.

    This class is designed to represent an entity with an identifier, value, and
    classification. It enforces a strict configuration model to validate and
    ensure the integrity of its data. The classification attribute supports aliasing
    for flexible data input. Intended for structured data modeling, this class
    inherits from `BaseModel`, leveraging Pydantic's data validation and parsing
    capabilities.

    Attributes:
        entityId (str): Unique identifier for the entity.
        value (str): The value associated with the entity.
        classification (str): Classification or category of the entity. It supports
            aliasing through 'class' or 'classification' during data input.
    """
    model_config = ConfigDict(strict=True)

    entityId:str
    value:str
    classification:str = Field(alias=AliasChoices('class', 'classification'))

class ScoredEntity(BaseModel):
    """
    Represents an entity with an associated score.

    This class extends the BaseModel and is used to link an Entity object
    with a corresponding score, ensuring strict validation of its properties.

    Attributes:
        entity (Entity): The entity object associated with the score.
        score (float): The score value associated with the entity.
    """
    model_config = ConfigDict(strict=True)

    entity:Entity
    score:float
    reranking_score:Optional[float]=0.0

class EntityContext(BaseModel):
    model_config = ConfigDict(strict=True)

    entities: List[ScoredEntity]=[]

class EntityContexts(BaseModel):
    model_config = ConfigDict(strict=True)

    contexts: List[EntityContext]=[]
    keywords: List[str]=[]

    @property
    def context_strs(self):
        return [
            ', '.join([entity.entity.value.lower() for entity in entity_context.entities])
            for entity_context in self.contexts
        ]
    
    @property
    def keywords_str(self):
        return ', '.join(keyword for keyword in self.keywords)
    
    @property
    def all_context_strs(self):
        all_contexts = [self.keywords_str] if self.keywords else []
        all_contexts.extend(self.context_strs)
        return all_contexts


class SearchResultCollection(BaseModel):
    """
    Represents a collection of search results and associated scored entities.

    The class is designed to store and manage a list of search results and
    a list of scored entities resulting from a search operation. It provides
    methods to add search results and entities, as well as to replace
    the current set of search results with a new set.

    Attributes:
        model_config: Configuration settings for the model. The strict setting
            ensures that data types and structures are strictly enforced.
        results (List[SearchResult]): A list of search result objects.
        entities (List[ScoredEntity]): A list of scored entities associated
            with the search results.
    """
    model_config = ConfigDict(strict=True)

    results: List[SearchResult]=[]
    entity_contexts: EntityContexts

    def add_search_result(self, result:SearchResult):
        """
        Adds a search result to the existing list of results.

        This method appends the given search result to the internal list of
        results maintained by the instance. It is intended to manage the
        storage of search results during execution, ensuring they are added
        in the order they are received.

        Args:
            result: A SearchResult object representing the search result
                to be added to the list of results.
        """
        self.results.append(result)

    def with_new_results(self, results:List[SearchResult]):
        """
        Updates the current object with new search results and returns the updated object. This allows for
        fluent interface pattern by returning the instance itself after updating its state with the provided
        search results.

        Args:
            results (List[SearchResult]): A list of SearchResult objects to update the instance's current
                results data.

        Returns:
            The current instance with the updated results.
        """
        self.results = results
        return self



