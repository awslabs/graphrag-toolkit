# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional

from llama_index.core.schema import TransformComponent

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.batch_llm_proposition_extractor_sync import BatchLLMPropositionExtractorSync
from graphrag_toolkit.lexical_graph.indexing.extract.batch_topic_extractor_sync import BatchTopicExtractorSync
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.preferred_values import PreferredValuesProvider
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCacheType


class BatchLLMPropositionStage(ExtractionStage):
    """Stage wrapper for batch LLM proposition extraction via Bedrock."""

    def __init__(self, batch_config: BatchConfig, prompt_template: Optional[str] = None, llm: Optional[LLMCacheType] = None):
        self._batch_config = batch_config
        self._prompt_template = prompt_template
        self._llm = llm

    def input_keys(self) -> List[str]:
        return []

    def output_keys(self) -> List[str]:
        return [PROPOSITIONS_KEY]

    def as_transform(self) -> TransformComponent:
        return BatchLLMPropositionExtractorSync(
            batch_config=self._batch_config,
            prompt_template=self._prompt_template,
            llm=self._llm,
        )

    @property
    def stage_type(self) -> str:
        return 'llm'


class BatchTopicExtractionStage(ExtractionStage):
    """Stage wrapper for batch topic extraction via Bedrock."""

    def __init__(
        self,
        batch_config: BatchConfig,
        use_propositions: bool = True,
        prompt_template: Optional[str] = None,
        llm: Optional[LLMCacheType] = None,
        entity_classification_provider: Optional[PreferredValuesProvider] = None,
        topic_provider: Optional[PreferredValuesProvider] = None,
    ):
        self._batch_config = batch_config
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
        return BatchTopicExtractorSync(
            batch_config=self._batch_config,
            source_metadata_field=PROPOSITIONS_KEY if self._use_propositions else None,
            prompt_template=self._prompt_template,
            llm=self._llm,
            entity_classification_provider=self._entity_classification_provider,
            topic_provider=self._topic_provider,
        )

    @property
    def stage_type(self) -> str:
        return 'llm'
