# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import abc
from typing import Dict, List, Optional


class ChunkStore(abc.ABC):
    """
    Backend-agnostic interface for reading and writing chunk text.

    Chunk text is stored separately from the graph so that graph traversal
    queries are not weighed down by large text properties on every
    `__Chunk__` node. Implementations decide where the text actually lives
    (the graph itself, S3, or any other object store).
    """

    @abc.abstractmethod
    def get(self, chunk_id: str) -> Optional[str]:
        """
        Return the text for a single chunk, or None if it isn't found.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, chunk_id: str, text: str) -> None:
        """
        Store the text for a single chunk.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_batch(self, chunk_ids: List[str]) -> Dict[str, str]:
        """
        Return text for the given chunk ids, keyed by chunk id. Chunk ids
        with no stored text are omitted from the result.
        """
        raise NotImplementedError
