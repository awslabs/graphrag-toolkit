# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from difflib import SequenceMatcher
from typing import List, Optional, Sequence

from llama_index.core.schema import BaseNode, TransformComponent
from llama_index.core.bridge.pydantic import Field

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.stages.ner_extraction_stage import PRE_EXTRACTED_ENTITIES_KEY
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection, Entity

logger = logging.getLogger(__name__)


def _is_fuzzy_match(name: str, existing_names: set, threshold: float) -> bool:
    """Check if name fuzzy-matches any existing name."""
    name_lower = name.lower()
    for existing in existing_names:
        if SequenceMatcher(None, name_lower, existing).ratio() >= threshold:
            return True
    return False


class EntityMergeTransform(TransformComponent):
    """TransformComponent that merges NER-detected entities into TopicCollection."""

    fuzzy_threshold: Optional[float] = Field(
        default=None,
        description='Fuzzy matching threshold (0-1). None means exact match only.',
    )

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
                    name_lower = name.lower()

                    if self.fuzzy_threshold is not None:
                        is_dup = _is_fuzzy_match(name, existing_names, self.fuzzy_threshold)
                    else:
                        is_dup = name_lower in existing_names

                    if not is_dup:
                        topic.entities.append(Entity(
                            value=name,
                            classification=ner_entity.get('classification', 'unknown'),
                        ))
                        existing_names.add(name_lower)

            node.metadata[TOPICS_KEY] = tc.model_dump()

        return nodes


class EntityMergeStage(ExtractionStage):
    """Stage that merges NER-detected entities into LLM-extracted TopicCollection.

    Args:
        fuzzy_threshold: Similarity threshold for fuzzy deduplication (0-1).
            None (default) uses exact case-insensitive matching.
            0.85 is a good starting point for fuzzy matching.
    """

    def __init__(self, fuzzy_threshold: Optional[float] = None):
        self._fuzzy_threshold = fuzzy_threshold

    def input_keys(self) -> List[str]:
        return [PRE_EXTRACTED_ENTITIES_KEY, TOPICS_KEY]

    def output_keys(self) -> List[str]:
        return [TOPICS_KEY]

    def as_transform(self) -> TransformComponent:
        return EntityMergeTransform(fuzzy_threshold=self._fuzzy_threshold)

    @property
    def stage_type(self) -> str:
        return 'transform'
