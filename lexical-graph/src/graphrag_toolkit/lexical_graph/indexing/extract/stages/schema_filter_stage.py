# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Sequence

from llama_index.core.schema import BaseNode, TransformComponent
from llama_index.core.bridge.pydantic import Field
from pydantic import ConfigDict

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_schema import ExtractionSchema
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection

logger = logging.getLogger(__name__)


class SchemaFilter(TransformComponent):
    """TransformComponent that filters extracted topics against a schema."""

    extraction_schema: ExtractionSchema = Field(description='Extraction schema for filtering')

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __call__(self, nodes: Sequence[BaseNode], **kwargs) -> Sequence[BaseNode]:
        if not self.extraction_schema.strict:
            return nodes

        allowed_entity_types = {n.lower() for n in self.extraction_schema.entity_type_names()}
        for config in self.extraction_schema.entity_types.values():
            for alias in config.aliases:
                allowed_entity_types.add(alias.lower())

        allowed_relationships = {r.upper() for r in self.extraction_schema.relationship_types} if self.extraction_schema.relationship_types else None

        for node in nodes:
            topics_data = node.metadata.get(TOPICS_KEY)
            if not topics_data:
                continue

            tc = TopicCollection(**topics_data)
            for topic in tc.topics:
                if allowed_entity_types:
                    topic.entities = [
                        e for e in topic.entities
                        if e.classification and e.classification.lower() in allowed_entity_types
                    ]
                if allowed_relationships:
                    for stmt in topic.statements:
                        stmt.facts = [
                            f for f in stmt.facts
                            if f.predicate.value.upper() in allowed_relationships
                        ]

            node.metadata[TOPICS_KEY] = tc.model_dump()

        return nodes


class SchemaFilterStage(ExtractionStage):
    """Stage that filters extracted topics against an ExtractionSchema."""

    def __init__(self, schema: ExtractionSchema):
        self._schema = schema

    def input_keys(self) -> List[str]:
        return [TOPICS_KEY]

    def output_keys(self) -> List[str]:
        return [TOPICS_KEY]

    def as_transform(self) -> TransformComponent:
        return SchemaFilter(extraction_schema=self._schema)

    @property
    def stage_type(self) -> str:
        return 'filter'
