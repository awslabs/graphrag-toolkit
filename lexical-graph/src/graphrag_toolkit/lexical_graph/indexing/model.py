# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, ConfigDict
from typing import List, Optional, Union, Dict, Generator, Iterable

from llama_index.core.schema import TextNode, Document, BaseNode
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo

class SourceDocument(BaseModel):
    """
    Represents a source document model used for managing and organizing nodes
    with relationships, providing methods to operate on these nodes.

    This class defines a structure to handle a collection of nodes (`refNode` and
    `nodes`) associated with a source document. It includes functionality to
    retrieve the source identifier of the document based on the defined node
    relationships.

    Attributes:
        refNode (Optional[BaseNode]): A reference node that can optionally
            associate with the document.
        nodes (List[BaseNode]): A list of nodes associated with the source
            document. Defaults to an empty list.
    """
    model_config = ConfigDict(strict=True)
    
    refNode:Optional[BaseNode]=None
    nodes:List[BaseNode]=[]

    def source_id(self):
        """
        Retrieves the source node ID from the list of nodes if it exists.

        This method accesses the first node in the `nodes` list and retrieves
        the ID of the node it is related to via the `SOURCE` relationship.
        If no nodes are present in the list, the method returns `None`.

        Returns:
            Optional[int]: The node ID of the source node if it exists.
            Returns `None` if the `nodes` list is empty.
        """
        if not self.nodes:
            return None
        return self.nodes[0].relationships[NodeRelationship.SOURCE].node_id


SourceType = Union[SourceDocument, BaseNode]

def source_documents_from_source_types(inputs: Iterable[SourceType]) -> Generator[SourceDocument, None, None]:
    """
    Generates `SourceDocument` objects from a collection of input types, processing
    each input object based on its type.

    This function iterates through a collection of `SourceType` inputs and converts
    them into `SourceDocument` objects while maintaining contextual relationships
    between the input nodes. It handles three primary types: `SourceDocument`,
    `Document`, and `TextNode`. All `TextNode` inputs are grouped into their parent
    `SourceDocument` based on their source relationships. If the input type does
    not match one of the specified types, a `ValueError` is raised.

    Args:
        inputs (Iterable[SourceType]): An iterable collection of input objects
            which can include `SourceDocument`, `Document`, or `TextNode` instances.

    Yields:
        Generator[SourceDocument, None, None]: A generator of `SourceDocument`
            objects created from the input data set.
    """
    chunks_by_source:Dict[str, SourceDocument] = {}

    for i in inputs:
        if isinstance(i, SourceDocument):
            yield i
        elif isinstance(i, Document):
            yield SourceDocument(nodes=[i])
        elif isinstance(i, TextNode):
            source_info = i.relationships[NodeRelationship.SOURCE]
            source_id = source_info.node_id
            if source_id not in chunks_by_source:
                chunks_by_source[source_id] = SourceDocument()
            chunks_by_source[source_id].nodes.append(i)
        else:
            raise ValueError(f'Unexpected source type: {type(i)}')

    for nodes in chunks_by_source.values():
        yield SourceDocument(nodes=list(nodes))


class Propositions(BaseModel):
    """
    Represents a collection of propositions with configuration for strict validation.

    This class stores a list of propositions and enforces strict validation of its data.
    It is used to model structured collections of propositional data, ensuring that
    the input adheres to the defined constraints and types.

    Attributes:
        model_config (ConfigDict): Configuration dict to enforce strict validation.
        propositions (List[str]): A list of propositions represented as strings.
    """
    model_config = ConfigDict(strict=True)
    
    propositions: List[str]

class Entity(BaseModel):
    """
    Represents an entity with its value and optional classification.

    The Entity class serves to model an object with an identifier, a specific
    value, and an optional classification. It is designed to provide structure
    for representing and managing entities in various contexts.

    Attributes:
        entityId (Optional[str]): The unique identifier for the entity. If not
            provided, it defaults to None.
        value (str): The specific value of the entity. This is a required
            attribute.
        classification (Optional[str]): Optional classification or category of
            the entity. Defaults to None.
    """
    model_config = ConfigDict(strict=True)
    
    entityId: Optional[str]=None

    value: str
    classification: Optional[str]=None

class Relation(BaseModel):
    """
    Represents a relation with specific configuration attributes.

    This class is used to define and manage a relation with a strict configuration
    model. It encapsulates attributes relevant to a given relation and ensures
    that the specified configuration is enforced.

    Attributes:
        model_config (ConfigDict): Configuration dictionary enforcing a strict
            model behavior.
        value (str): The value representing the relation.
    """
    model_config = ConfigDict(strict=True)

    value: str

EntityType = Union[Entity, str]

class Fact(BaseModel):
    """
    Represents a fact entity with associated properties describing relationships
    between subject, predicate, object, and an optional complement.

    This class is used to model facts in a structured format, where each fact is
    essentially a subject-predicate-object triplet, often supplemented with an
    optional complement. The relation between the subject and object is defined
    by the predicate. The fact is also associated with unique identifiers for
    fact tracking and external references.

    Attributes:
        factId (Optional[str]): Unique identifier for the fact. Defaults to None.
        statementId (Optional[str]): Identifier referencing the statement this
            fact is derived from. Defaults to None.
        subject (Entity): The subject entity involved in the fact.
        predicate (Relation): The relationship or action linking subject and
            object.
        object (Optional[Entity]): The object entity involved in the fact.
            Defaults to None.
        complement (Optional[str]): Additional information or modification
            related to the fact. Defaults to None.
    """
    model_config = ConfigDict(strict=True)

    factId: Optional[str]=None
    statementId: Optional[str]=None

    subject: Entity
    predicate: Relation
    object: Optional[Entity]=None
    complement: Optional[EntityType]=None

class Statement(BaseModel):
    """Represents a statement with associated details, facts, and identifiers.

    This class captures the concept of a statement, which is associated with specific
    identifiers such as topic, chunk, and statement IDs. It also holds the statement
    value, related details, and a list of associated facts. It ensures a strict configuration
    model for its behavior.

    Attributes:
        statementId (Optional[str]): Identifier for the statement. It is optional and
            can be None.
        topicId (Optional[str]): Identifier for the topic to which the statement belongs.
            It is optional and can be None.
        chunkId (Optional[str]): Identifier for the chunk related to the statement. It
            is optional and can be None.
        value (str): The textual content or main value of the statement.
        details (List[str]): A list of additional details or elaborations related to the
            statement. Defaults to an empty list.
        facts (List[Fact]): A list of Fact objects associated with the statement. Defaults
            to an empty list.
    """
    model_config = ConfigDict(strict=True)

    statementId: Optional[str]=None
    topicId: Optional[str]=None
    chunkId: Optional[str]=None

    value: str
    details: List[str]=[]
    facts: List[Fact]=[]

class Topic(BaseModel):
    """
    Represents a Topic with associated details, related chunks, entities, and statements.

    This class encapsulates information about a specific topic, including its unique
    identifier, related chunk identifiers, the topic's textual value, a list of entities
    associated with the topic, and any relevant statements. The configuration ensures
    strict model validation rules when handling Topic instances.

    Attributes:
        topicId (Optional[str]): A unique identifier for the topic. Defaults to None if not provided.
        chunkIds (List[str]): List of chunk identifiers related to the topic. Defaults to an empty list.
        value (str): The textual value or name of the topic.
        entities (List[Entity]): List of entities relevant to the topic. Defaults to an empty list.
        statements (List[Statement]): List of statements associated with the topic. Defaults to an empty list.
    """
    model_config = ConfigDict(strict=True)

    topicId : Optional[str]=None
    chunkIds: List[str]=[]

    value: str
    entities: List[Entity]=[]
    statements: List[Statement]=[]

class TopicCollection(BaseModel):
    """
    Represents a collection of topics with strict model configuration.

    This class serves as a data model for managing a collection of `Topic` objects.
    It ensures strict validation and typing enforcement, making it suitable for use
    in scenarios where structured and validated data representation is critical.

    Attributes:
        model_config (ConfigDict): Specifies the configuration for the model,
            enforcing strict validation rules.
        topics (List[Topic]): A list that contains instances of `Topic` objects.
    """
    model_config = ConfigDict(strict=True)

    topics: List[Topic]=[]  
