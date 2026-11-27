# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import List, Optional

from llama_index.core.schema import TransformComponent


class ExtractionStage(ABC):
    """Abstract base class for composable extraction pipeline stages.

    Each stage declares its input and output metadata keys, enabling
    the PipelineBuilder to validate stage compatibility at build time.
    """

    @abstractmethod
    def input_keys(self) -> List[str]:
        """Metadata keys this stage reads from."""
        ...

    @abstractmethod
    def output_keys(self) -> List[str]:
        """Metadata keys this stage writes to."""
        ...

    @abstractmethod
    def as_transform(self) -> TransformComponent:
        """Return the LlamaIndex TransformComponent for this stage."""
        ...

    @property
    def stage_type(self) -> str:
        """Stage type identifier: 'local', 'llm', 'filter', or 'transform'."""
        return 'transform'
