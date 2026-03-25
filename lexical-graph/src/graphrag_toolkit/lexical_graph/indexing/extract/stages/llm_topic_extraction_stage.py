# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional

from llama_index.core.schema import TransformComponent

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor import TopicExtractor
from graphrag_toolkit.lexical_graph.indexing.extract.preferred_values import PreferredValuesProvider, default_preferred_values
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCacheType


class LLMTopicExtractionStage(ExtractionStage):
    """Stage wrapper for LLM-based topic/entity/relationship extraction."""

    def __init__(
        self,
        use_propositions: bool = True,
        prompt_template: Optional[str] = None,
        llm: Optional[LLMCacheType] = None,
        entity_classification_provider: Optional[PreferredValuesProvider] = None,
        topic_provider: Optional[PreferredValuesProvider] = None,
    ):
        self._use_propositions = use_propositions
        self._prompt_template = prompt_template
        self._llm = llm
        self._entity_classification_provider = entity_classification_provider
        self._topic_provider = topic_provider

    def input_keys(self) -> List[str]:
        if self._use_propositions:
            return [PROPOSITIONS_KEY]
        return []

    def output_keys(self) -> List[str]:
        return [TOPICS_KEY]

    def as_transform(self) -> TransformComponent:
        return TopicExtractor(
            source_metadata_field=PROPOSITIONS_KEY if self._use_propositions else None,
            prompt_template=self._prompt_template,
            llm=self._llm,
            entity_classification_provider=self._entity_classification_provider,
            topic_provider=self._topic_provider,
        )

    @property
    def stage_type(self) -> str:
        return 'llm'
