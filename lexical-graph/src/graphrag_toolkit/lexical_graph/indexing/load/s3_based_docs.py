# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import contextlib
import io
import json
import logging
import time
import queue
import threading
import uuid
import concurrent.futures

from collections import deque
from os.path import basename, dirname, join
from datetime import datetime
from itertools import repeat, islice
from threading import Semaphore
from typing import List, Any, Generator, Optional, Dict, Callable, Set, Tuple

from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.utils.hash_utils import get_hash
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument, SourceType, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils.id_validation import validate_id_segment

from llama_index.core.schema import TextNode, BaseComponent
from llama_index.core.bridge.pydantic import PrivateAttr

QUEUE_SIZE = 1000
BATCH_SIZE = 100

# How often the staging consumer wakes to check whether its producer is still
# alive. Uploads are slow, so this paces the liveness check, not the work.
QUEUE_POLL_SECONDS = 1.0

logger = logging.getLogger(__name__)

# Reserved name for a completion marker. The rest of the name is a digest of
# the chunk ids it covers, because one source can emit several SourceDocuments
# into a shared prefix and each needs its own marker.
COMPLETION_MARKER_PREFIX = '_COMPLETE-'

# Markers live under a reserved segment rather than beside the chunks. A chunk is
# keyed by its node id, so a shared namespace means a node id starting with the
# prefix reads back as a marker and is dropped from the document.
COMPLETION_MARKER_DIR = '_markers'

# Joins node ids before hashing them. Without a separator ['ab', 'c'] and
# ['a', 'bc'] hash alike.
_NODE_ID_DELIMITER = '\x00'


def node_ids_hash(node_ids) -> str:
    """A hash over node ids, independent of the order they arrive in."""
    return get_hash(_NODE_ID_DELIMITER.join(sorted(node_ids)))


def completion_marker_name(node_ids) -> str:
    """
    The marker name covering exactly these chunk ids.

    The whole digest, so that two markers under one prefix cannot share a name
    and leave the second overwriting the first.
    """
    return f'{COMPLETION_MARKER_PREFIX}{node_ids_hash(node_ids)}'


def completion_marker_key(root_path:str, node_ids) -> str:
    """Where the marker covering exactly these chunk ids is stored."""
    return join(root_path, COMPLETION_MARKER_DIR, completion_marker_name(node_ids))


def is_completion_marker(key:str) -> bool:
    """
    Whether an object key is a completion marker.

    Decided by the reserved segment, not the name: TextNode.from_json turns a
    marker into a node with a generated uuid and empty text rather than
    rejecting it, so a listing that keeps one gains a phantom chunk - and a
    chunk whose node id opens with the marker prefix would be dropped.
    """
    return basename(dirname(key)) == COMPLETION_MARKER_DIR


def source_document_prefix(collection_prefix:str, doc:SourceDocument, nodes) -> str:
    """
    Where a source document's objects go, with the ids that name it checked.

    Both uploaders join ids onto the collection prefix to build a key, so an id
    is a key segment and has to stay inside one.
    """
    source_id = doc.source_id()
    validate_id_segment(source_id, 'source_id')

    for n in nodes:
        validate_id_segment(n.node_id, 'node_id')

    return join(collection_prefix, source_id)


def written_nodes(doc:SourceDocument) -> List[TextNode]:
    """The nodes an uploader stores. An index key marks a vector store artefact."""
    return [n for n in doc.nodes if INDEX_KEY not in n.metadata]


# Records that a collection is staged with completion markers. Sits directly
# under the collection, where a listing delimited on '/' returns it as an object
# rather than as a source document prefix.
COLLECTION_RECORD_NAME = '_staging.json'

# Extension the chunk uploader adds to a node id to key its object.
_CHUNK_SUFFIX = '.json'


def collection_record_key(key_prefix:str, collection_id:str) -> str:
    return join(key_prefix, collection_id, COLLECTION_RECORD_NAME)


def list_collection(bucket_name:str, key_prefix:str, collection_id:str, paginator) -> Tuple[List[str], bool]:
    """
    The source document prefixes in a collection, and whether it is recorded as
    staged with completion markers.

    Both come out of the one listing, so the record costs no second call and no
    permission the listing did not already need. A collection with no record was
    staged before markers existed and is read as it stands.
    """
    collection_path = join(key_prefix, collection_id, '')
    record_key = collection_record_key(key_prefix, collection_id)

    source_doc_prefixes = []
    recorded = False

    for page in paginator.paginate(Bucket=bucket_name, Prefix=collection_path, Delimiter='/'):
        source_doc_prefixes.extend(
            source_doc_obj['Prefix'] for source_doc_obj in page.get('CommonPrefixes', [])
        )
        recorded = recorded or any(
            obj['Key'] == record_key for obj in page.get('Contents', [])
        )

    return source_doc_prefixes, recorded


def marker_chunk_ids(marker_keys, bucket_name:str, s3_client) -> Set[str]:
    """The chunk ids these markers cover."""
    chunk_ids = set()
    for marker_key in marker_keys:
        with io.BytesIO() as io_stream:
            s3_client.download_fileobj(bucket_name, marker_key, io_stream)
            io_stream.seek(0)
            marker = json.loads(io_stream.read().decode('UTF-8'))
        chunk_ids.update(marker.get('chunk_ids', []))
    return chunk_ids


def partition_marker_keys(keys) -> Tuple[List[str], List[str]]:
    """Split a source document prefix's keys into content and markers."""
    markers = [key for key in keys if is_completion_marker(key)]
    content = [key for key in keys if not is_completion_marker(key)]
    return content, markers


def chunk_id_from_key(chunk_key:str, source_doc_prefix:str) -> str:
    """
    The node id a chunk object is keyed on. Taken by stripping what the writer
    added, because a node id can hold a '.', so an id ending in '.json' keys an
    object ending '.json.json'.
    """
    name = chunk_key[len(source_doc_prefix):] if chunk_key.startswith(source_doc_prefix) else chunk_key
    return name[:-len(_CHUNK_SUFFIX)] if name.endswith(_CHUNK_SUFFIX) else name


def is_complete(node_ids, marker_keys, bucket_name:str, s3_client) -> bool:
    """
    Whether the markers under a prefix account for exactly the nodes found there.

    A truncated document leaves nodes no marker covers, and one whose objects
    were partly removed leaves a marker covering nodes that are gone.

    A marker covers one SourceDocument. Extraction emits a source as more than
    one SourceDocument when its chunks span rounds, and each part is marked as
    it lands, so a source whose later parts were never stored reads as complete.
    Telling those apart needs the extraction pipeline to say when a source is
    finished, which it does not currently record.
    """
    return set(node_ids) == marker_chunk_ids(marker_keys, bucket_name, s3_client)

class EncryptedPut:
    """
    How these uploaders encrypt what they store.

    A caller-supplied KMS key selects aws:kms, otherwise S3 managed keys.
    """

    # Supplied by the host class.
    bucket_name:str
    s3_encryption_key_id:Optional[str]

    def _put(self, key:str, body:str, content_type:str, s3_client):
        encryption = (
            {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': self.s3_encryption_key_id}
            if self.s3_encryption_key_id
            else {'ServerSideEncryption': 'AES256'}
        )

        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body.encode('UTF-8'),
            ContentType=content_type,
            **encryption
        )

    def record_collection(self, key_prefix:str, collection_id:str, s3_client):
        """
        Record that this collection is staged with completion markers.

        Only an empty collection is recorded, so staging into one written before
        markers existed leaves its documents reading as they do today rather
        than turning incomplete. Written before the first document, so any
        collection holding an object holds its record too. A failure raises:
        without the record every later read of this collection trusts what it
        finds.
        """
        collection_path = join(key_prefix, collection_id, '')

        existing = s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=collection_path, MaxKeys=1
        )

        if existing.get('KeyCount'):
            logger.debug(f'Not recording a collection that already holds objects [collection_path: {collection_path}]')
            return

        key = collection_record_key(key_prefix, collection_id)
        self._put(key, json.dumps({'completion_markers': True}, indent=4), 'application/json', s3_client)

    def _write_completion_marker(self, root_path:str, nodes:List[TextNode], s3_client):
        """
        Record that this document is complete.

        Written last, so its presence separates a whole document from a
        truncated prefix. A failed write is logged rather than raised: no
        marker costs a re-stage, where raising would lose a stream the chunks
        themselves survived.
        """
        node_ids = sorted(n.node_id for n in nodes)
        marker = {
            'chunk_ids': node_ids,
            'count': len(node_ids),
            'content_hash': node_ids_hash(node_ids),
        }

        key = completion_marker_key(root_path, node_ids)
        logger.debug(f'Writing completion marker to S3 [bucket: {self.bucket_name}, key: {key}]')

        try:
            self._put(key, json.dumps(marker, indent=4), 'application/json', s3_client)
        except Exception as e:
            logger.error(f'Error writing completion marker [key: {key}]: {str(e)}')


class _UploadFailed:
    """
    Stands in for a document that did not upload.

    The consumer counts one item per submitted document, so a failure has to
    occupy a slot rather than being dropped.
    """

    def __init__(self, cause:BaseException):
        self.cause = cause

class ConfiguredThreadCount:
    """
    Sizes a thread pool from a caller-supplied count, falling back to the config
    for a component built outside S3BasedDocs.
    """

    # Declared on the mixin and collected by BaseComponent's pydantic field
    # machinery across the MRO, so each subclass carries num_threads as a field.
    num_threads:Optional[int]=None

    def _num_threads(self):
        if self.num_threads is None:
            return GraphRAGConfig.extraction_num_threads_per_worker
        return self.num_threads


def to_batches(xs, n):
    n = max(1, n)
    return [xs[i:i+n] for i in range(0, len(xs), n)]

class S3DocDownloader(ConfiguredThreadCount, BaseComponent):

    key_prefix:str
    collection_id:str
    bucket_name:str
    fn:Callable[[TextNode], TextNode]

    def _download_doc(self, doc_key, s3_client, strict=False):

        paginator = s3_client.get_paginator('list_objects_v2')

        node_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=doc_key)

        node_keys, marker_keys = partition_marker_keys([
            node_obj['Key']
            for node_page in node_pages
            for node_obj in node_page.get('Contents', [])
        ])

        # Every object under the prefix merges into one SourceDocument, and a
        # run that packs a source's chunks differently writes a new object
        # beside the old one, so a node id can arrive twice. Keys sort
        # lexicographically, so the first copy wins rather than the newest.
        nodes = []
        seen_node_ids = set()

        for node_key in node_keys:
        
            with io.BytesIO() as io_stream:
                s3_client.download_fileobj(self.bucket_name, node_key, io_stream)        
                io_stream.seek(0)
                data = io_stream.readline().decode('UTF-8')
                while data:
                    node = TextNode.from_json(data)
                    if node.node_id not in seen_node_ids:
                        seen_node_ids.add(node.node_id)
                        nodes.append(self.fn(node))
                    data = io_stream.readline().decode('UTF-8')

        if strict and not is_complete(seen_node_ids, marker_keys, self.bucket_name, s3_client):
            logger.warning(f'Skipping an incomplete source document [prefix: {doc_key}, num_nodes: {len(nodes)}]')
            return None

        return SourceDocument(nodes=nodes)
    
    def download(self):

        s3_client = GraphRAGConfig.s3

        collection_path = join(self.key_prefix,  self.collection_id, '')

        paginator = s3_client.get_paginator('list_objects_v2')

        source_doc_prefixes, strict = list_collection(
            self.bucket_name, self.key_prefix, self.collection_id, paginator
        )

        source_doc_prefixes_batches = to_batches(source_doc_prefixes, BATCH_SIZE)

        logger.debug(f'Started getting source documents from S3 [bucket: {self.bucket_name}, collection_path: {collection_path}, num_prefixes: {len(source_doc_prefixes)}]')

        with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_threads()) as executor:

            for source_doc_prefixes_batch in source_doc_prefixes_batches:

                docs = []

                keys = [
                    source_doc_prefix
                    for source_doc_prefix in source_doc_prefixes_batch
                ]
                
                docs.extend(list(executor.map(
                    self._download_doc,
                    keys,
                    repeat(s3_client),
                    repeat(strict)
                )))

                for doc in docs:
                    if doc is None:
                        continue
                    logger.debug(f'Yielding source document [source: {doc.source_id()}, num_nodes: {len(doc.nodes)}]')
                    yield doc

class S3DocUploader(ConfiguredThreadCount, EncryptedPut, BaseComponent):

    bucket_name:str
    collection_prefix:str
    s3_encryption_key_id:Optional[str]=None
    deterministic_document_key:bool=False
    _semaphore:Semaphore = PrivateAttr(default=None)
    _queue:queue.Queue = PrivateAttr(default=None)
    
    def _doc_suffix(self, doc:SourceDocument) -> str:
        """
        What separates one object from another under a source document's prefix.

        A random suffix differs on every attempt, so a retry writes a second
        object rather than replacing the first. Hashing the ids of the nodes
        written instead makes a retry overwrite, while keeping the separate
        SourceDocuments an auto-tuned run emits for one source apart.
        """
        if not self.deterministic_document_key:
            return uuid.uuid4().hex[:5]

        return node_ids_hash(n.node_id for n in written_nodes(doc))[:5]

    def _upload_doc(self, root_path:str, doc:SourceDocument, s3_client):

        doc_output_path = join(root_path, f'{doc.source_id()}-{self._doc_suffix(doc)}.jsonl')

        logger.debug(f'Writing source document as JSONL to S3: [bucket: {self.bucket_name}, key: {doc_output_path}]')
        
        try:

            nodes = written_nodes(doc)

            s = '\n'.join([
                json.dumps(n.to_dict())
                for n in nodes
            ])

            if nodes:
                self._put(doc_output_path, s, 'text/plain', s3_client)
                self._write_completion_marker(root_path, nodes, s3_client)

            return doc

        except Exception as e:
            logger.error(f'Error while writing source document to S3: {str(e)}')
            raise

    def _get_callback_fn(self, queue:queue.Queue):
        def _task_complete_callback(future):
            # The consumer counts one item per submitted document, so a failure
            # has to put something too. Putting nothing leaves it polling for a
            # count it can never reach.
            try:
                queue.put(future.result(timeout=1.0))
            except BaseException as e:
                # BaseException, matching _doc_publisher: an interrupt in a worker
                # strands the consumer as surely as a ClientError does.
                logger.error(f'Error uploading source document: {str(e)}')
                queue.put(_UploadFailed(e))
            finally:
                self._semaphore.release()
        return _task_complete_callback
    
    def _submit_proxy(self, function, executor, queue:queue.Queue, *args, **kwargs) -> bool:
        """Submit one upload. Returns whether the consumer should expect an item."""
        self._semaphore.acquire()
        try:
            future = executor.submit(function, *args, **kwargs)
        except Exception as e:
            logger.exception(f'Error in submit proxy: {str(e)}')
            self._semaphore.release()
            return False

        future.add_done_callback(self._get_callback_fn(queue))
        return True
    
    def _doc_publisher(self, queue:queue.Queue, source_documents:List[SourceDocument]=[]):

        count = 0

        try:
            s3_client = GraphRAGConfig.s3

            with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_threads()) as executor:

                for source_document in source_documents:

                    if not source_document.nodes:
                        continue
                    
                    root_path = source_document_prefix(
                        self.collection_prefix, source_document, written_nodes(source_document)
                    )
                   
                    if self._submit_proxy(self._upload_doc, executor, queue, root_path, source_document, s3_client):
                        count += 1

        except BaseException as e:
            logger.exception(f'Error in doc publisher: {str(e)}')
            raise

        finally:
            self._queue.put(count)

    def _upload_batch(self, source_docs_batch:List[SourceDocument]):

        producer_errors:List[BaseException] = []

        def publish():
            try:
                self._doc_publisher(source_documents=source_docs_batch, queue=self._queue)
            except BaseException as e:
                producer_errors.append(e)

        thread = threading.Thread(target=publish, daemon=True)
        thread.start()

        count = 0
        target_count = None

        logger.debug(f'About to start polling queue [count: {count}, target_count: {target_count}]')

        failures:List[BaseException] = []

        while target_count is None or count < target_count:
            try:
                item = self._queue.get(timeout=QUEUE_POLL_SECONDS)
                if isinstance(item, int):
                    target_count = item
                else:
                    count += 1
                    if isinstance(item, _UploadFailed):
                        failures.append(item.cause)
                    else:
                        yield item
                self._queue.task_done()
            except queue.Empty:
                # A producer that died before putting its count would otherwise
                # keep the consumer here for the rest of the run. Reaching here
                # means the batch is short, so it is a failure rather than an end.
                if not thread.is_alive():
                    if not producer_errors:
                        failures.append(RuntimeError(
                            f'Upload producer stopped before reporting its document count '
                            f'[count: {count}, target_count: {target_count}]'
                        ))
                    break
            
        logger.debug(f'Waiting on queue to empty [count: {count}, target_count: {target_count}]')

        thread.join()

        if producer_errors:
            logger.error(
                f'Upload producer failed after submitting {count} source documents '
                f'[batch_size: {len(source_docs_batch)}, failed_documents: {len(failures)}]'
            )
            raise producer_errors[0]

        if failures:
            # The first is raised rather than aggregated: 3.10 has no ExceptionGroup.
            if len(failures) > 1:
                logger.error(
                    f'{len(failures)} source documents failed to upload, raising the first '
                    f'[batch_size: {len(source_docs_batch)}]'
                )
            raise failures[0]

    def upload(self, source_documents: List[SourceDocument]):

        if not self._queue:
            self._queue = queue.Queue(QUEUE_SIZE)

        if not self._semaphore:
            self._semaphore = Semaphore(BATCH_SIZE)

        logger.debug('Starting upload...')

        total = 0
        source_docs_batch = []
        
        for source_doc in source_documents:
            
            source_docs_batch.append(source_doc)
            
            if len(source_docs_batch) == 1000:

                for return_value in self._upload_batch(source_docs_batch):
                    total += 1
                    yield return_value
            
                source_docs_batch = []
                logger.debug(f'Total uploaded: {total}')
                
        if source_docs_batch:

            for return_value in self._upload_batch(source_docs_batch):
                    total += 1
                    yield return_value
        
            source_docs_batch = []
            logger.debug(f'Total uploaded: {total}')

class S3ChunkDownloader(ConfiguredThreadCount, BaseComponent):

    key_prefix:str
    collection_id:str
    bucket_name:str
    fn:Callable[[TextNode], TextNode]

    def _download_chunk(self, chunk_key, s3_client):

        with io.BytesIO() as io_stream:
            s3_client.download_fileobj(self.bucket_name, chunk_key, io_stream)        
            io_stream.seek(0)
            data = io_stream.read().decode('UTF-8')
            return self.fn(TextNode.from_json(data))

    def download(self):
        """
        Yield one SourceDocument per source-document prefix in the collection.

        Two thread pools stay open across the yields, so they shut down when the
        generator is closed rather than when the loop ends. Consume it fully, or
        close it - `with contextlib.closing(...)` or an explicit `.close()` - if
        you might stop early. Abandoning it without closing leaves the pools for
        the garbage collector to reclaim.
        """
        s3_client = GraphRAGConfig.s3

        collection_path = join(self.key_prefix,  self.collection_id, '')

        paginator = s3_client.get_paginator('list_objects_v2')

        source_doc_prefixes, strict = list_collection(
            self.bucket_name, self.key_prefix, self.collection_id, paginator
        )

        logger.debug(f'Started getting source documents from S3 [bucket: {self.bucket_name}, collection_path: {collection_path}, num_prefixes: {len(source_doc_prefixes)}]')

        num_threads = self._num_threads()

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as download_executor, \
             concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as list_executor:

            def _list_chunk_keys(source_doc_prefix):
                # Listing and markers only: no chunk downloads dispatched here, so
                # peak resident chunk data stays one document's worth rather than
                # the whole window's, and completeness is settled by prefetch
                # rather than by a blocking GET per document on the way out.
                chunk_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=source_doc_prefix)
                chunk_keys, marker_keys = partition_marker_keys([
                    chunk_obj['Key']
                    for chunk_page in chunk_pages
                    for chunk_obj in chunk_page.get('Contents', [])
                ])

                if not strict:
                    return chunk_keys, True

                chunk_ids = [
                    chunk_id_from_key(chunk_key, source_doc_prefix)
                    for chunk_key in chunk_keys
                ]

                return chunk_keys, is_complete(chunk_ids, marker_keys, self.bucket_name, s3_client)

            # Bounded sliding window: at most num_threads listings prefetch ahead,
            # so listing overlaps downloading without reading the whole collection.
            remaining_prefixes = iter(source_doc_prefixes)
            in_flight = deque(
                (source_doc_prefix, list_executor.submit(_list_chunk_keys, source_doc_prefix))
                for source_doc_prefix in islice(remaining_prefixes, num_threads)
            )

            while in_flight:
                source_doc_prefix, listing = in_flight.popleft()
                chunk_keys, complete = listing.result()

                next_prefix = next(remaining_prefixes, None)
                if next_prefix is not None:
                    in_flight.append(
                        (next_prefix, list_executor.submit(_list_chunk_keys, next_prefix))
                    )

                if not complete:
                    logger.warning(f'Skipping an incomplete source document [prefix: {source_doc_prefix}, num_chunks: {len(chunk_keys)}]')
                    continue

                # Download the current document's chunks only, then yield, so at
                # most one document's chunk data is resident at a time and an
                # abandoned generator dispatches no downloads for unconsumed docs.
                nodes = list(download_executor.map(
                    self._download_chunk,
                    chunk_keys,
                    repeat(s3_client),
                ))

                logger.debug(f'Yielding source document [source: {source_doc_prefix}, num_nodes: {len(nodes)}]')

                yield SourceDocument(nodes=nodes)

class S3ChunkUploader(ConfiguredThreadCount, EncryptedPut, BaseComponent):

    bucket_name:str
    collection_prefix:str
    s3_encryption_key_id:Optional[str]=None

    def _upload_chunk(self, root_path:str, n:TextNode, s3_client):
        chunk_output_path = join(root_path, f'{n.node_id}{_CHUNK_SUFFIX}')
                    
        logger.debug(f'Writing chunk to S3: [bucket: {self.bucket_name}, key: {chunk_output_path}]')

        self._put(
            chunk_output_path, json.dumps(n.to_dict(), indent=4), 'application/json', s3_client
        )

    def _drain(self, futures) -> bool:
        """Wait on a document's uploads, reporting whether all of them landed."""
        succeeded = True
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f'Error uploading chunk: {str(e)}')
                succeeded = False
        return succeeded

    def upload(self, source_documents: List[SourceDocument]):
        """
        Upload each document's chunks, yielding a document once its own uploads
        have been attempted.

        Chunks for several documents are in flight at once; waiting for one
        document first capped them at that document's chunk count rather than at
        the pool. Documents are yielded in order. A failed chunk is logged, not
        raised, so a yielded document is not proof every chunk reached S3.
        """
        s3_client = GraphRAGConfig.s3
        num_threads = self._num_threads()

        # Two documents per thread keeps the pool busy while the oldest drains.
        max_inflight = num_threads * 2

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:

            pending = deque()
            inflight = 0

            def release_oldest():
                nonlocal inflight
                (oldest, root_path, nodes, oldest_futures) = pending.popleft()
                if self._drain(oldest_futures) and nodes:
                    self._write_completion_marker(root_path, nodes, s3_client)
                inflight -= len(oldest_futures)
                return oldest

            for source_document in source_documents:

                nodes = written_nodes(source_document)

                if nodes:
                    root_path = source_document_prefix(self.collection_prefix, source_document, nodes)
                    logger.debug(f'Writing source document to S3 [bucket: {self.bucket_name}, prefix: {root_path}]')

                    futures = [
                        executor.submit(self._upload_chunk, root_path, n, s3_client)
                        for n in nodes
                    ]
                else:
                    # No prefix: one holding only a marker reads back as a
                    # document with no nodes, whose source_id() is None, and a
                    # re-stage cannot build a path from None. Still queued, so
                    # it keeps its place in the order upload() promises.
                    root_path = None
                    futures = []
                    logger.debug(f'Nothing to write for source document [source: {source_document.source_id()}]')

                pending.append((source_document, root_path, nodes, futures))
                inflight += len(futures)

                while inflight > max_inflight:
                    yield release_oldest()

            while pending:
                yield release_oldest()



class S3BasedDocs(NodeHandler):

    region:str
    bucket_name:str
    key_prefix:str
    collection_id:str
    s3_encryption_key_id:Optional[str]=None
    metadata_keys:Optional[List[str]]=None
    for_jsonl:Optional[bool]=False
    num_threads:Optional[int]=None
    deterministic_document_key:bool=False

    _uploader:Any = PrivateAttr(default=None)
    _downloader:Any = PrivateAttr(default=None)

    def __init__(self, 
                 region:str, 
                 bucket_name:str, 
                 key_prefix:str, 
                 collection_id:Optional[str]=None,
                 s3_encryption_key_id:Optional[str]=None, 
                 metadata_keys:Optional[List[str]]=None,
                 for_jsonl:Optional[bool]=False,
                 num_threads:Optional[int]=None,
                 deterministic_document_key:bool=False):

        # __init__ runs where GraphRAGConfig was configured; accept() runs in a
        # spawned worker that inherits no parent memory and reads back the
        # default. Carried as a field so it pickles with the handler.
        if num_threads is None:
            num_threads = GraphRAGConfig.extraction_num_threads_per_worker

        super().__init__(
            region=region,
            bucket_name=bucket_name,
            key_prefix=key_prefix,
            collection_id=collection_id or datetime.now().strftime('%Y%m%d-%H%M%S'),
            s3_encryption_key_id=s3_encryption_key_id,
            metadata_keys=metadata_keys,
            for_jsonl=for_jsonl,
            num_threads=num_threads,
            deterministic_document_key=deterministic_document_key
        )

    def docs(self):
        return self
    
    def _filter_metadata(self, node:TextNode) -> TextNode:
        """
        Filters the metadata within a TextNode object and its associated relationships to retain only
        specific keys. Deletes metadata keys that are neither in the allowed set of keys
        (PROPOSITIONS_KEY, TOPICS_KEY, INDEX_KEY) nor in the user-specified metadata keys (metadata_keys).

        Args:
            node (TextNode): The TextNode whose metadata and relationships' metadata will be filtered.

        Returns:
            TextNode: The filtered TextNode with irrelevant metadata keys removed.
        """
        def filter(metadata:Dict):
            """
            Handles operations on a TextNode object by filtering its metadata based on
            specified criteria. Utilizes predefined constants and optional metadata keys
            to clean the metadata attached to the given TextNode.

            This class inherits from NodeHandler, specializing its behavior to interact
            with the S3-based document storage or related metadata.

            Attributes:
                metadata_keys (Optional[List[str]]): A list of metadata keys that are allowed
                    to remain in the metadata dictionary. If None, filtering is based only on
                    predefined constants.
            """
            keys_to_delete = []
            for key in metadata.keys():
                if key not in [PROPOSITIONS_KEY, TOPICS_KEY, INDEX_KEY]:
                    if self.metadata_keys is not None and key not in self.metadata_keys:
                        keys_to_delete.append(key)
            for key in keys_to_delete:
                del metadata[key]

        filter(node.metadata)

        for _, relationship_info in node.relationships.items():
            if relationship_info.metadata:
                filter(relationship_info.metadata)

        return node
    
    def __iter__(self):

        if not self._downloader:
        
            if self.for_jsonl:
                self._downloader = S3DocDownloader(
                    key_prefix=self.key_prefix, 
                    collection_id=self.collection_id, 
                    bucket_name=self.bucket_name, 
                    fn=self._filter_metadata,
                    num_threads=self.num_threads
                )
            else:
                self._downloader = S3ChunkDownloader(
                    key_prefix=self.key_prefix, 
                    collection_id=self.collection_id, 
                    bucket_name=self.bucket_name, 
                    fn=self._filter_metadata,
                    num_threads=self.num_threads
                )

        path = join(self.key_prefix,  self.collection_id, '')

        logger.debug(f"Started getting source documents from S3 [bucket: {self.bucket_name}, prefix: {path}]")

        start = time.time()
        doc_count = 0

        # download() holds thread pools open across its yields, so closing it is
        # what shuts them down. Wrapping it here means a consumer that stops
        # early - `for doc in docs: break` - unwinds the pools through this
        # generator's own GeneratorExit, rather than waiting on the garbage
        # collector to finalize an abandoned generator.
        with contextlib.closing(self._downloader.download()) as docs:
            for doc in docs:
                doc_count += 1
                yield(doc)

        end = time.time()

        logger.debug(f"Finished getting {doc_count} source documents from S3 [bucket: {self.bucket_name}, prefix: {path}] ({end - start} seconds)")

    def __call__(self, nodes: List[SourceType], **kwargs: Any) -> List[SourceDocument]:
        return [n for n in self.accept(source_documents_from_source_types(nodes), **kwargs)]
    
    def accept(self, source_documents: List[SourceDocument], **kwargs: Any) -> Generator[SourceDocument, None, None]:
        
        collection_prefix = join(self.key_prefix, self.collection_id)

        start = time.time()
        logger.debug(f'Started writing source documents to S3 [bucket: {self.bucket_name}, prefix: {collection_prefix}]')

        doc_count = 0

        if not self._uploader:

            if self.for_jsonl:
                uploader = S3DocUploader(
                    bucket_name=self.bucket_name,
                    collection_prefix=collection_prefix,
                    s3_encryption_key_id=self.s3_encryption_key_id,
                    num_threads=self.num_threads,
                    deterministic_document_key=self.deterministic_document_key
                )

            else:
                uploader = S3ChunkUploader(
                    bucket_name=self.bucket_name,
                    collection_prefix=collection_prefix,
                    s3_encryption_key_id=self.s3_encryption_key_id,
                    num_threads=self.num_threads
                )

            # Recorded before the uploader is kept, so a failure leaves the next
            # call to try again rather than staging on unrecorded.
            uploader.record_collection(self.key_prefix, self.collection_id, GraphRAGConfig.s3)
            self._uploader = uploader

        for doc in self._uploader.upload(source_documents):
            doc_count += 1
            yield doc

        end = time.time()
        logger.debug(f'Finished writing {doc_count} source documents to S3 [bucket: {self.bucket_name}, prefix: {collection_prefix}] ({end - start} seconds)')
