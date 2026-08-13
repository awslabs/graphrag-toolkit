# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
import logging
from typing import Dict, List, Optional

from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage.chunk.chunk_store import ChunkStore

logger = logging.getLogger(__name__)

# Codes S3 and S3-compatible endpoints use for an object that isn't there.
MISSING_KEY_CODES = ('NoSuchKey', '404')


class S3ChunkStore(ChunkStore):
    """
    ChunkStore backed by S3, keeping chunk text off the `__Chunk__` node so
    graph memory doesn't scale with text volume.

    The key is derived from the chunk id, so a read already knows every key it
    needs and never lists the bucket. Nothing is written to the graph.

    A `fallback` store, if given, answers for chunks S3 doesn't hold - that is
    how text written before a migration, still inline on the graph node, stays
    readable. Errors other than a missing key propagate rather than falling
    back, so a credentials or permissions problem doesn't look like an empty
    bucket.
    """

    def __init__(self,
                 bucket_name: str,
                 prefix: Optional[str] = None,
                 kms_key_arn: Optional[str] = None,
                 fallback: Optional[ChunkStore] = None,
                 num_threads: Optional[int] = None):
        if not bucket_name:
            raise ValueError('S3ChunkStore requires a bucket name.')

        self.bucket_name = bucket_name
        self.prefix = prefix.strip('/') if prefix else None
        self.kms_key_arn = kms_key_arn
        self.fallback = fallback
        self.num_threads = num_threads

    def _num_workers(self, num_items: int) -> int:
        """
        Threads to use for a batch of `num_items` objects.

        Never more threads than objects, so a two-chunk read does not spin up
        thirty-two of them.

        The default comes from `extraction_num_threads_per_worker`, which is an
        extraction-time setting: `get_batch` also runs on the retrieval path,
        where that number has no particular meaning. Pass `num_threads` to size
        it independently until retrieval has a concurrency setting of its own.
        """
        configured = self.num_threads or GraphRAGConfig.extraction_num_threads_per_worker

        return max(1, min(num_items, configured))

    def _key(self, chunk_id: str) -> str:
        return f'{self.prefix}/{chunk_id}.txt' if self.prefix else f'{chunk_id}.txt'

    def _get_chunk(self, chunk_id: str, s3_client) -> Optional[str]:
        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=self._key(chunk_id))
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in MISSING_KEY_CODES:
                return None
            raise
        return response['Body'].read().decode('UTF-8')

    def get_batch(self, chunk_ids: List[str]) -> Dict[str, str]:
        if not chunk_ids:
            return {}

        s3_client = GraphRAGConfig.s3
        found = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers(len(chunk_ids))) as executor:

            futures = {
                executor.submit(self._get_chunk, chunk_id, s3_client): chunk_id
                for chunk_id in chunk_ids
            }

            for future in concurrent.futures.as_completed(futures):
                text = future.result()
                # A chunk stored as empty text is a hit, so test against None.
                if text is not None:
                    found[futures[future]] = text

        missing = [chunk_id for chunk_id in chunk_ids if chunk_id not in found]

        if missing and self.fallback:
            logger.debug(f'Reading chunks not in S3 from the fallback store [num_chunks: {len(missing)}]')
            found.update(self.fallback.get_batch(missing))

        return found

    def _put_chunk(self, chunk_id: str, text: str, s3_client) -> None:
        key = self._key(chunk_id)

        logger.debug(f'Writing chunk text to S3 [bucket: {self.bucket_name}, key: {key}]')

        params = {
            'Bucket': self.bucket_name,
            'Key': key,
            'Body': text.encode('UTF-8'),
            'ContentType': 'text/plain; charset=utf-8',
        }

        if self.kms_key_arn:
            params['ServerSideEncryption'] = 'aws:kms'
            params['SSEKMSKeyId'] = self.kms_key_arn
        else:
            params['ServerSideEncryption'] = 'AES256'

        s3_client.put_object(**params)

    def put(self, chunk_id: str, text: str) -> None:
        self._put_chunk(chunk_id, text, GraphRAGConfig.s3)

    def put_batch(self, chunks: Dict[str, str]) -> None:
        """
        Write several chunks concurrently.

        S3 has no multi-object write, so a batch is still one PUT per chunk;
        what changes is that they overlap instead of running end to end.
        Measured over 5000 chunks, serial writes run 96.6 ms/chunk against
        8.0 ms/chunk in-graph, and that gap is what this closes.

        Any failed write propagates. A partial batch would otherwise leave
        chunk text in S3 with no matching graph node and nothing to say which
        chunks were missed.
        """
        if not chunks:
            return

        s3_client = GraphRAGConfig.s3

        with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers(len(chunks))) as executor:

            futures = [
                executor.submit(self._put_chunk, chunk_id, text, s3_client)
                for chunk_id, text in chunks.items()
            ]

            for future in concurrent.futures.as_completed(futures):
                future.result()
