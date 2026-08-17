# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
from urllib.parse import urlparse, parse_qs
import logging
from typing import Dict, List, Optional

from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage.chunk.chunk_store import ChunkStore

logger = logging.getLogger(__name__)

# The scheme that selects this store in a chunk-store connection string.
S3_URI_SCHEME = 's3://'


def parse_s3_connection_string(connection_string):
    parsed = urlparse(connection_string)

    bucket_name = parsed.hostname

    # `s3:///prefix` parses with no hostname and would otherwise build a store
    # against bucket None, failing later at the first call with an error that
    # says nothing about the connection string.
    if not bucket_name:
        raise ValueError(
            f'Invalid S3 connection string, no bucket name: {connection_string}. '
            'Expected s3://bucket/prefix.'
        )

    prefix = parsed.path[1:] if parsed.path else None
    if prefix:
        while prefix.endswith('/'):
            prefix = prefix[:-1]
    prefix = prefix if prefix else None

    # parse_qs always returns a list for a present key, so take the first value.
    kms_key_arns = parse_qs(parsed.query).get('kmsKeyArn') if parsed.query else None
    kms_key_arn = kms_key_arns[0] if kms_key_arns else None

    return (bucket_name, prefix, kms_key_arn)


# Codes S3 and S3-compatible endpoints use for an object that isn't there.
MISSING_KEY_CODES = ('NoSuchKey', '404')

# Cap on a single chunk download, so an unexpectedly large object can't be
# pulled fully into memory across the read thread pool.
DEFAULT_MAX_CHUNK_BYTES = 50 * 1024 * 1024


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
                 num_threads: Optional[int] = None,
                 max_chunk_bytes: int = DEFAULT_MAX_CHUNK_BYTES):
        if not bucket_name:
            raise ValueError('S3ChunkStore requires a bucket name.')

        self.bucket_name = bucket_name
        self.prefix = prefix.strip('/') if prefix else None
        self.kms_key_arn = kms_key_arn
        self.fallback = fallback
        self.num_threads = num_threads
        self.max_chunk_bytes = max_chunk_bytes

    def _num_workers(self, num_items: int) -> int:
        """
        Threads to use for a batch of `num_items` objects.

        Never more threads than objects, so a two-chunk read does not spin up
        thirty-two of them.

        The default comes from `extraction_num_threads_per_worker`, which is an
        extraction-time setting: `get_batch` also runs on the retrieval path,
        where that number has no particular meaning. Pass `num_threads` to size
        it independently.
        """
        configured = self.num_threads or GraphRAGConfig.extraction_num_threads_per_worker

        return max(1, min(num_items, configured))

    @staticmethod
    def _validate_chunk_id(chunk_id: str) -> None:
        """
        Reject chunk ids that could escape the configured prefix.

        The key is f'{prefix}/{chunk_id}.txt', so a separator in the id opens a
        new path segment and lets a crafted id touch any *.txt object in the
        bucket. Real ids are IdGenerator hashes and hold no separators.
        """
        if not chunk_id or not chunk_id.strip():
            raise ValueError('chunk_id must be a non-empty string.')
        if '/' in chunk_id or '\\' in chunk_id:
            raise ValueError(f'chunk_id must not contain a path separator: {chunk_id!r}')
        if any(ord(c) < 0x20 or ord(c) == 0x7f for c in chunk_id):
            raise ValueError(f'chunk_id must not contain control characters: {chunk_id!r}')

    def _key(self, chunk_id: str) -> str:
        self._validate_chunk_id(chunk_id)
        return f'{self.prefix}/{chunk_id}.txt' if self.prefix else f'{chunk_id}.txt'

    def _get_chunk(self, chunk_id: str, s3_client) -> Optional[str]:
        key = self._key(chunk_id)
        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in MISSING_KEY_CODES:
                return None
            raise
        # Read one byte past the cap to detect an over-limit object without
        # pulling the whole thing into memory.
        body = response['Body'].read(self.max_chunk_bytes + 1)
        if len(body) > self.max_chunk_bytes:
            raise ValueError(
                f'Chunk text at [bucket: {self.bucket_name}, key: {key}] exceeds the '
                f'maximum of {self.max_chunk_bytes} bytes.'
            )
        return body.decode('UTF-8')

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
