# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, ConfigDict
from typing import List, Optional

class Propositions(BaseModel):
    model_config = ConfigDict(strict=True)
    
    propositions: List[str]

class Entity(BaseModel):
    model_config = ConfigDict(strict=True)
    
    entity_id: Optional[str]=None

    value: str
    classification: Optional[str]=None

class Relation(BaseModel):
    model_config = ConfigDict(strict=True)

    value: str

class Fact(BaseModel):
    model_config = ConfigDict(strict=True)

    statement_id: Optional[str]=None
    fact_id: Optional[str]=None

    subject: Entity
    predicate: Relation
    object: Optional[Entity]=None
    complement: Optional[str]=None

class Statement(BaseModel):
    model_config = ConfigDict(strict=True)

    topic_id: Optional[str]=None
    statement_id: Optional[str]=None

    value: str
    details: List[str]=[]
    facts: List[Fact]=[]

class Topic(BaseModel):
    model_config = ConfigDict(strict=True)

    topic_id : Optional[str]=None

    value: str
    entities: List[Entity]=[]
    statements: List[Statement]=[]

class TopicCollection(BaseModel):
    model_config = ConfigDict(strict=True)

    topics: List[Topic]=[]  
