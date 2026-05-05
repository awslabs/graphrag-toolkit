# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, List, Optional, Sequence

from llama_index.core.schema import BaseNode, TransformComponent
from llama_index.core.bridge.pydantic import Field
from pydantic import PrivateAttr

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage

logger = logging.getLogger(__name__)

PRE_EXTRACTED_ENTITIES_KEY = 'aws::graph::pre_extracted_entities'


class NERTransform(TransformComponent):
    """TransformComponent that runs NER and writes entities to metadata."""

    model_name: str = Field(default='urchade/gliner_base')
    entity_labels: List[str] = Field(default_factory=list)
    threshold: float = Field(default=0.5)
    _model: Any = PrivateAttr(default=None)

    def __call__(self, nodes: Sequence[BaseNode], **kwargs) -> Sequence[BaseNode]:
        try:
            from gliner import GLiNER
        except ImportError:
            raise ImportError(
                'GLiNER is required for NERExtractionStage. '
                'Install it with: pip install gliner'
            )

        if self._model is None:
            logger.info('Loading NER model: %s', self.model_name)
            try:
                self._model = GLiNER.from_pretrained(self.model_name)
            except Exception as e:
                raise RuntimeError(
                    f'Failed to load NER model "{self.model_name}". '
                    f'Ensure the model name is correct and you have internet access. '
                    f'Error: {e}'
                ) from e

        for node in nodes:
            text = node.get_content()
            if not text or not self.entity_labels:
                node.metadata[PRE_EXTRACTED_ENTITIES_KEY] = []
                continue

            predictions = self._model.predict_entities(text, self.entity_labels, threshold=self.threshold)
            entities = [
                {'value': p['text'], 'classification': p['label']}
                for p in predictions
            ]
            node.metadata[PRE_EXTRACTED_ENTITIES_KEY] = entities

        return nodes


class NERExtractionStage(ExtractionStage):
    """Stage wrapper for CPU-based NER extraction using GLiNER."""

    def __init__(
        self,
        model_name: str = 'urchade/gliner_base',
        entity_labels: Optional[List[str]] = None,
        threshold: float = 0.5,
    ):
        self._model_name = model_name
        self._entity_labels = entity_labels or []
        self._threshold = threshold

    def input_keys(self) -> List[str]:
        return []

    def output_keys(self) -> List[str]:
        return [PRE_EXTRACTED_ENTITIES_KEY]

    def as_transform(self) -> TransformComponent:
        return NERTransform(
            model_name=self._model_name,
            entity_labels=self._entity_labels,
            threshold=self._threshold,
        )

    @property
    def stage_type(self) -> str:
        return 'local'
