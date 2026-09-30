# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional

from llama_index.core.schema import TransformComponent

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.proposition_extractor import PropositionExtractor
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY


class LocalPropositionStage(ExtractionStage):
    """Stage wrapper for local transformer-based proposition extraction."""

    def __init__(self, model_name: Optional[str] = None, device: Optional[str] = None):
        self._model_name = model_name
        self._device = device

    def input_keys(self) -> List[str]:
        return []

    def output_keys(self) -> List[str]:
        return [PROPOSITIONS_KEY]

    def as_transform(self) -> TransformComponent:
        kwargs = {}
        if self._model_name:
            kwargs['proposition_model_name'] = self._model_name
        if self._device:
            kwargs['device'] = self._device
        return PropositionExtractor(**kwargs)

    @property
    def stage_type(self) -> str:
        return 'local'
