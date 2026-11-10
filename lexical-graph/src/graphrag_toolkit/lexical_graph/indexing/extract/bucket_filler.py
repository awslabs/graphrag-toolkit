# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from llama_index.core.schema import BaseNode


class BucketFiller:
    """Contiguous, count-based chunk accumulator for auto-tuning batch extraction.

    Collects document chunks in arrival order and slices each round evenly into
    Bedrock batch jobs. A full round is ``num_workers`` jobs of ``max_batch_size``.
    A shorter round uses as many of the workers as it can without a job falling
    below ``min_batch_size``, because a round lasts as long as its largest job.

    It does not keep a document's chunks within a single job: per-document
    integrity is guaranteed downstream by ``source_id``-keyed storage
    (``S3BasedDocs``), the same mechanism the fixed path relies on.

    Driven by ``ExtractionPipeline._extract_auto_tuned``.
    """

    def __init__(self, num_workers: int, max_batch_size: int, min_batch_size: int = 1):
        if num_workers < 1:
            raise ValueError(f'num_workers must be >= 1 (got {num_workers})')
        if max_batch_size < 1:
            raise ValueError(f'max_batch_size must be >= 1 (got {max_batch_size})')
        if min_batch_size < 1:
            raise ValueError(f'min_batch_size must be >= 1 (got {min_batch_size})')

        self.num_workers = num_workers
        self.max_batch_size = max_batch_size
        self.min_batch_size = min_batch_size
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
        """Slice up to one round's worth of buffered chunks into even, contiguous jobs.

        Takes at most ``round_capacity`` chunks off the front and splits them
        with :meth:`split`. Chunks beyond ``round_capacity`` stay buffered.
        Returns [] when the buffer is empty.
        """
        if not self._buffer:
            return []

        take = self._buffer[:self.round_capacity]
        self._buffer = self._buffer[self.round_capacity:]

        return self.split(take)

    def split(self, chunks: List[BaseNode]) -> List[List[BaseNode]]:
        """Split chunks into even, contiguous jobs.

        Uses ``min(num_workers, len(chunks) // min_batch_size)`` jobs whose sizes
        differ by at most one. Fewer than ``min_batch_size`` chunks are returned
        as one small job. Returns [] for no chunks.
        """
        if not chunks:
            return []

        num_jobs = max(1, min(self.num_workers, len(chunks) // self.min_batch_size))
        size, larger_jobs = divmod(len(chunks), num_jobs)

        jobs = []
        start = 0
        for index in range(num_jobs):
            end = start + size + (1 if index < larger_jobs else 0)
            jobs.append(chunks[start:end])
            start = end

        return jobs
