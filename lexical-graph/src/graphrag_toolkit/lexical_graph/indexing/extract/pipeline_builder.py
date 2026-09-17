# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional

from llama_index.core.schema import TransformComponent

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage

logger = logging.getLogger(__name__)


class PipelineBuilder:
    """Builds a validated extraction pipeline from ExtractionStage instances.

    Validates that each stage's input_keys are satisfied by prior stages'
    output_keys (or are available as initial keys).
    """

    def __init__(self, initial_keys: Optional[List[str]] = None):
        self._stages: List[ExtractionStage] = []
        self._available_keys: set = set(initial_keys or [])

    def add(self, stage: ExtractionStage) -> 'PipelineBuilder':
        """Add a stage to the pipeline with input key validation.

        Args:
            stage: The extraction stage to add.

        Returns:
            self for method chaining.

        Raises:
            ValueError: If the stage requires input keys not yet available.
        """
        missing = set(stage.input_keys()) - self._available_keys
        if missing:
            raise ValueError(
                f"Stage {stage.__class__.__name__} requires keys {sorted(missing)} "
                f"not provided by prior stages. Available: {sorted(self._available_keys)}"
            )
        self._stages.append(stage)
        self._available_keys.update(stage.output_keys())
        return self

    def build(self) -> List[TransformComponent]:
        """Build the pipeline, returning a list of TransformComponents.

        Raises:
            ValueError: If no stages have been added.
        """
        if not self._stages:
            raise ValueError('Pipeline has no stages')
        return [stage.as_transform() for stage in self._stages]

    @property
    def stages(self) -> List[ExtractionStage]:
        """Return the current list of stages."""
        return list(self._stages)

    @property
    def available_keys(self) -> List[str]:
        """Return the currently available metadata keys."""
        return sorted(self._available_keys)
