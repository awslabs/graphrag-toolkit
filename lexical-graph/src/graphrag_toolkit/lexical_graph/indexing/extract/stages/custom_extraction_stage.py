# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Callable, Dict, List, Optional, Sequence

from llama_index.core.schema import BaseNode, TransformComponent
from llama_index.core.bridge.pydantic import Field
from pydantic import ConfigDict

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.constants import (
    EXTRACTION_STATUS_KEY,
    EXTRACTION_ERROR_KEY,
    EXTRACTION_STATUS_FAILED,
)

logger = logging.getLogger(__name__)

ExtractorFn = Callable[[BaseNode], Dict[str, Any]]


class CustomExtractionTransform(TransformComponent):
    """TransformComponent that delegates per-node extraction to a user callable.

    A node is the atomic unit of success/failure: an exception from
    extractor_fn is caught and recorded on that node's metadata rather than
    raised, so one bad document never aborts the rest of the batch.
    """

    extractor_fn: ExtractorFn = Field(description='User-supplied per-node extraction callable')
    name: str = Field(default='CustomExtractionStage', description='Stage name, used in log messages')

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __call__(self, nodes: Sequence[BaseNode], **kwargs) -> Sequence[BaseNode]:
        for node in nodes:
            try:
                result = self.extractor_fn(node) or {}
                # Clear a status left by an earlier stage in a fallback chain,
                # so a recovered node is not still flagged from its failure there.
                node.metadata.pop(EXTRACTION_STATUS_KEY, None)
                node.metadata.pop(EXTRACTION_ERROR_KEY, None)
                node.metadata.update(result)
            except Exception as e:
                logger.warning(f'[{self.name}] extraction failed for node {node.node_id}: {e}')
                node.metadata[EXTRACTION_STATUS_KEY] = EXTRACTION_STATUS_FAILED
                node.metadata[EXTRACTION_ERROR_KEY] = str(e)
        return nodes


class CustomExtractionStage(ExtractionStage):
    """Stage that wraps a user-provided extraction callable.

    Enables customer-provided extraction logic (an in-house NER service, a
    deterministic rule-based extractor, a remote endpoint wrapped by the
    caller) to participate in the composable pipeline without forking the
    toolkit. The same stage serves both a single-node "real-time" call and a
    many-node "batch" call, since TransformComponent.__call__ already takes
    a sequence of nodes and per-node failure isolation holds either way.
    """

    def __init__(
        self,
        extractor_fn: ExtractorFn,
        input_keys: Optional[List[str]] = None,
        output_keys: Optional[List[str]] = None,
        name: str = 'CustomExtractionStage',
    ):
        self._extractor_fn = extractor_fn
        self._input_keys = list(input_keys or [])
        self._output_keys = list(output_keys or [])
        self._name = name

    def input_keys(self) -> List[str]:
        return list(self._input_keys)

    def output_keys(self) -> List[str]:
        return list(self._output_keys)

    def as_transform(self) -> TransformComponent:
        return CustomExtractionTransform(extractor_fn=self._extractor_fn, name=self._name)

    @property
    def stage_type(self) -> str:
        return 'custom'
