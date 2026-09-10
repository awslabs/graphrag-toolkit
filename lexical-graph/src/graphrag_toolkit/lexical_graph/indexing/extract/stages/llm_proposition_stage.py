# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional

from llama_index.core.schema import TransformComponent

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.llm_proposition_extractor import LLMPropositionExtractor
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCacheType


class LLMPropositionStage(ExtractionStage):
    """Stage wrapper for LLM-based proposition extraction."""

    def __init__(self, prompt_template: Optional[str] = None, llm: Optional[LLMCacheType] = None):
        self._prompt_template = prompt_template
        self._llm = llm

    def input_keys(self) -> List[str]:
        return []

    def output_keys(self) -> List[str]:
        return [PROPOSITIONS_KEY]

    def as_transform(self) -> TransformComponent:
        return LLMPropositionExtractor(
            prompt_template=self._prompt_template,
            llm=self._llm
        )

    @property
    def stage_type(self) -> str:
        return 'llm'
