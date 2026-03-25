# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Sequence

from llama_index.core.schema import BaseNode, TransformComponent
from llama_index.core.bridge.pydantic import Field

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.stages.ner_extraction_stage import PRE_EXTRACTED_ENTITIES_KEY
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection, Entity

logger = logging.getLogger(__name__)


class EntityMergeTransform(TransformComponent):
    """TransformComponent that merges NER-detected entities into TopicCollection."""

    def __call__(self, nodes: Sequence[BaseNode], **kwargs) -> Sequence[BaseNode]:
        for node in nodes:
            pre_extracted = node.metadata.get(PRE_EXTRACTED_ENTITIES_KEY, [])
            topics_data = node.metadata.get(TOPICS_KEY)

            if not pre_extracted or not topics_data:
                continue

            tc = TopicCollection(**topics_data)

            for topic in tc.topics:
                existing_names = {e.value.lower() for e in topic.entities}
                for ner_entity in pre_extracted:
                    name = ner_entity.get('value', '')
                    if name.lower() not in existing_names:
                        topic.entities.append(Entity(
                            value=name,
                            classification=ner_entity.get('classification', 'unknown'),
                        ))
                        existing_names.add(name.lower())

            node.metadata[TOPICS_KEY] = tc.model_dump()

        return nodes


class EntityMergeStage(ExtractionStage):
    """Stage that merges NER-detected entities into LLM-extracted TopicCollection."""

    def input_keys(self) -> List[str]:
        return [PRE_EXTRACTED_ENTITIES_KEY, TOPICS_KEY]

    def output_keys(self) -> List[str]:
        return [TOPICS_KEY]

    def as_transform(self) -> TransformComponent:
        return EntityMergeTransform()

    @property
    def stage_type(self) -> str:
        return 'transform'
