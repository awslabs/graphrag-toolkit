# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from llama_index.core.schema import BaseNode


class BucketFiller:
    """Contiguous, count-based chunk accumulator for auto-tuning batch extraction.

    Collects document chunks in arrival order and slices them into Bedrock batch
    jobs of up to ``max_batch_size`` chunks, mirroring the fixed-``batch_size``
    path's ``node_batcher``/``split_nodes`` packing: jobs are filled to
    ``max_batch_size`` and only the last job in a round holds the remainder.

    It deliberately does NOT balance occupancy across jobs or keep a document's
    chunks within a single job: balancing under-fills jobs, and per-document
    integrity is guaranteed downstream by ``source_id``-keyed storage
    (``S3BasedDocs``), the same mechanism the fixed path relies on.

    Driven by ``ExtractionPipeline._extract_auto_tuned``.
    """

    def __init__(self, num_workers: int, max_batch_size: int):
        if num_workers < 1:
            raise ValueError(f'num_workers must be >= 1 (got {num_workers})')
        if max_batch_size < 1:
            raise ValueError(f'max_batch_size must be >= 1 (got {max_batch_size})')

        self.num_workers = num_workers
        self.max_batch_size = max_batch_size
        self.round_capacity = num_workers * max_batch_size
        self._buffer: List[BaseNode] = []

    def add_document_chunks(self, chunks: List[BaseNode]) -> None:
        """Append a document's chunks to the ordered buffer."""
        if chunks:
            self._buffer.extend(chunks)

    def pending(self) -> int:
        """Return the number of chunks currently buffered."""
        return len(self._buffer)

    def would_overshoot(self, num_chunks: int) -> bool:
        """Return True if appending ``num_chunks`` would push a non-empty buffer
        past ``round_capacity``.

        The caller flushes when this is True *before* appending, so a document
        that fits within a round is not split across rounds -- while the flushed
        round stays near-full (the buffer is already close to capacity). An empty
        buffer never overshoots, so an oversized document is still accepted and
        then drained in full rounds by the caller.
        """
        return bool(self._buffer) and (len(self._buffer) + num_chunks > self.round_capacity)

    def drain_round(self) -> List[List[BaseNode]]:
        """Slice up to one round's worth of buffered chunks into contiguous jobs.

        Takes at most ``round_capacity`` chunks off the front of the buffer and
        slices them into jobs of up to ``max_batch_size`` -- at most
        ``num_workers`` jobs, each filled to ``max_batch_size`` except the last,
        which holds the remainder. Any chunks beyond one round's capacity (only
        possible for a single document larger than ``round_capacity``) remain
        buffered for the next round. Returns an empty list when the buffer is
        empty.
        """
        if not self._buffer:
            return []

        take = self._buffer[:self.round_capacity]
        self._buffer = self._buffer[self.round_capacity:]

        return [
            take[i:i + self.max_batch_size]
            for i in range(0, len(take), self.max_batch_size)
        ]
