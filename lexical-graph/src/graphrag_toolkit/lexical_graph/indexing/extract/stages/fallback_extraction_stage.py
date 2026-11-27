# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, List, Sequence

from llama_index.core.schema import BaseNode, TransformComponent
from llama_index.core.bridge.pydantic import Field
from pydantic import ConfigDict

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.constants import (
    EXTRACTION_STATUS_KEY,
    EXTRACTION_ERROR_KEY,
    EXTRACTION_STATUS_FAILED,
    EXTRACTION_STATUS_QUARANTINED,
)

logger = logging.getLogger(__name__)


class FallbackExtractionTransform(TransformComponent):
    """TransformComponent that runs a chain of transforms as a fallback chain.

    Only the nodes left failed after one stage are passed to the next. A
    node failing every stage is tagged quarantined rather than raised or
    dropped, so succeeded nodes are never held up by, or corrupted by, nodes
    that end up quarantined (R13 - "no partial graph", scoped per document).

    Per-node isolation is enforced here, not assumed from the wrapped
    transform: most toolkit stages (NERExtractionStage,
    LLMTopicExtractionStage, ...) raise outright on failure and have no
    opinion about EXTRACTION_STATUS_KEY. A transform is first tried as one
    batch call, which is the efficient path when nothing fails; if that call
    raises, the same nodes are retried one at a time so the exception is
    attributed to the actual offending node(s) rather than the whole batch.

    Failures are logged the way the retrieval post-processors log theirs
    (e.g. StatementEnhancementPostProcessor): at error level, naming the
    operation and the exception, not warning/info. A node that exhausts the
    chain carries that same degrade-on-failure outcome one step further -
    it is tagged quarantined rather than silently returned unchanged,
    because unlike a post-processor a later pipeline stage needs to be able
    to tell a quarantined node from one that simply had nothing to extract.
    """

    # Not List[TransformComponent]: a fallback target may be any plain
    # TransformComponent-shaped callable (e.g. a toolkit stage that predates
    # this contract), not necessarily a TransformComponent subclass.
    transforms: List[Any] = Field(description='Ordered fallback chain')

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _run_isolated(self, transform, nodes: List[BaseNode]) -> List[BaseNode]:
        """Run transform over nodes, attributing a raised exception to the
        specific node(s) that caused it instead of the whole batch."""
        try:
            return list(transform(nodes))
        except Exception:
            pass

        processed = []
        for node in nodes:
            try:
                [result] = transform([node])
                processed.append(result)
            except Exception as e:
                logger.error(f'Error running {type(transform).__name__} for node {node.node_id}: {e}')
                node.metadata[EXTRACTION_STATUS_KEY] = EXTRACTION_STATUS_FAILED
                node.metadata[EXTRACTION_ERROR_KEY] = str(e)
                processed.append(node)
        return processed

    def __call__(self, nodes: Sequence[BaseNode], **kwargs) -> Sequence[BaseNode]:
        pending = list(nodes)
        resolved = []

        for transform in self.transforms:
            if not pending:
                break
            processed = self._run_isolated(transform, pending)
            still_failed = []
            for node in processed:
                if node.metadata.get(EXTRACTION_STATUS_KEY) == EXTRACTION_STATUS_FAILED:
                    still_failed.append(node)
                else:
                    resolved.append(node)
            pending = still_failed

        for node in pending:
            logger.error(f'Node {node.node_id} exhausted the fallback chain; extraction will not succeed for this document')
            node.metadata[EXTRACTION_STATUS_KEY] = EXTRACTION_STATUS_QUARANTINED
            resolved.append(node)

        return resolved


class FallbackExtractionStage(ExtractionStage):
    """Composes an ordered chain of ExtractionStages as a fallback chain.

    Stage 0 runs against every node; a node left failed after one stage is
    retried by the next, so only the failed subset moves on at each step.
    A node that fails every stage in the chain is quarantined rather than
    blocking or corrupting the documents that succeeded.
    """

    def __init__(self, chain: List[ExtractionStage]):
        if not chain:
            raise ValueError('FallbackExtractionStage requires at least one stage')
        self._chain = list(chain)

    def input_keys(self) -> List[str]:
        return self._chain[0].input_keys()

    def output_keys(self) -> List[str]:
        keys: List[str] = []
        for stage in self._chain:
            for key in stage.output_keys():
                if key not in keys:
                    keys.append(key)
        return keys

    def as_transform(self) -> TransformComponent:
        return FallbackExtractionTransform(transforms=[stage.as_transform() for stage in self._chain])

    @property
    def stage_type(self) -> str:
        return 'fallback'
