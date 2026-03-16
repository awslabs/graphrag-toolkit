# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import io
import json
import logging
import time
import queue
import threading
import uuid
import concurrent.futures

from os.path import join
from datetime import datetime
from itertools import repeat
from threading import Semaphore
from typing import List, Any, Generator, Optional, Dict, Callable

from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument, SourceType, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph import GraphRAGConfig

from llama_index.core.schema import TextNode, BaseComponent
from llama_index.core.bridge.pydantic import PrivateAttr

QUEUE_SIZE = 1000
BATCH_SIZE = 100

logger = logging.getLogger(__name__)

def to_batches(xs, n):
    n = max(1, n)
    return [xs[i:i+n] for i in range(0, len(xs), n)]

class S3DocDownloader(BaseComponent):

    key_prefix:str
    collection_id:str
    bucket_name:str
    fn:Callable[[TextNode], TextNode]

    def _download_doc(self, doc_key, s3_client):

        paginator = s3_client.get_paginator('list_objects_v2')

        node_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=doc_key)
                
        node_keys = [
            node_obj['Key']
            for node_page in node_pages
            for node_obj in node_page['Contents'] 
        ]

        nodes = []

        for node_key in node_keys:
        
            with io.BytesIO() as io_stream:
                s3_client.download_fileobj(self.bucket_name, node_key, io_stream)        
                io_stream.seek(0)
                data = io_stream.readline().decode('UTF-8')
                while data:
                    nodes.append(self.fn(TextNode.from_json(data)))
                    data = io_stream.readline().decode('UTF-8')

        return SourceDocument(nodes=nodes)
    
    def download(self):

        s3_client = GraphRAGConfig.s3

        collection_path = join(self.key_prefix,  self.collection_id, '')

        paginator = s3_client.get_paginator('list_objects_v2')
        source_doc_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=collection_path, Delimiter='/')

        source_doc_prefixes = [ 
            source_doc_obj['Prefix'] 
            for source_doc_page in source_doc_pages 
            for source_doc_obj in source_doc_page['CommonPrefixes']           
        ]

        source_doc_prefixes_batches = to_batches(source_doc_prefixes, BATCH_SIZE)

        logger.debug(f'Started getting source documents from S3 [bucket: {self.bucket_name}, collection_path: {collection_path}, num_prefixes: {len(source_doc_prefixes)}]')

        with concurrent.futures.ThreadPoolExecutor(max_workers=GraphRAGConfig.extraction_num_threads_per_worker) as executor:

            for source_doc_prefixes_batch in source_doc_prefixes_batches:

                docs = []

                keys = [
                    source_doc_prefix
                    for source_doc_prefix in source_doc_prefixes_batch
                ]
                
                docs.extend(list(executor.map(
                    self._download_doc,
                    keys,
                    repeat(s3_client)
                )))

                for doc in docs:
                    logger.debug(f'Yielding source document [source: {doc.source_id()}, num_nodes: {len(doc.nodes)}]')
                    yield doc

class S3DocUploader(BaseComponent):

    bucket_name:str
    collection_prefix:str
    s3_encryption_key_id:Optional[str]=None
    _semaphore:Semaphore = PrivateAttr(default=None)
    _queue:queue.Queue = PrivateAttr(default=None)
    
    def _upload_doc(self, root_path:str, doc:SourceDocument, s3_client):

        doc_output_path = join(root_path, f'{doc.source_id()}-{uuid.uuid4().hex[:5]}.jsonl')

        logger.debug(f'Writing source document as JSONL to S3: [bucket: {self.bucket_name}, key: {doc_output_path}]')
        
        try:

            s = '\n'.join([
                json.dumps(n.to_dict())
                for n in doc.nodes 
                if not [key for key in [INDEX_KEY] if key in n.metadata]
            ]) 

            if self.s3_encryption_key_id:
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=doc_output_path,
                    Body=(bytes(s.encode('UTF-8'))),
                    ContentType='text/plain',
                    ServerSideEncryption='aws:kms',
                    SSEKMSKeyId=self.s3_encryption_key_id
                )
            else:
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=doc_output_path,
                    Body=(bytes(s.encode('UTF-8'))),
                    ContentType='text/plain',
                    ServerSideEncryption='AES256'
                )

            return doc
            
        except Exception as e:
            logger.error(f'Error while writing source document to S3: {str(e)}')

    def _task_complete_callback(self, future):
        self._semaphore.release()

    def _get_callback_fn(self, queue:queue.Queue):
        def _task_complete_callback(future):
            try:
                doc = future.result(timeout=1.0)
                queue.put(doc)
            except Exception as e:
                logger.error(f'Error getting result from future: {str(e)}')
            self._semaphore.release()
        return _task_complete_callback
    
    def _submit_proxy(self, function, executor, queue:queue.Queue, *args, **kwargs):
        try:
            self._semaphore.acquire()
            future = executor.submit(function, *args, **kwargs)
            future.add_done_callback(self._get_callback_fn(queue))
        except Exception as e:
            logger.exception(f'Error in submit proxy: {str(e)}')
    
    def _doc_publisher(self, queue:queue.Queue, source_documents:List[SourceDocument]=[]):

        s3_client = GraphRAGConfig.s3
        
        try:

            with concurrent.futures.ThreadPoolExecutor(max_workers=GraphRAGConfig.extraction_num_threads_per_worker) as executor:

                count = 0

                for source_document in source_documents:

                    if not source_document.nodes:
                        continue
                    
                    root_path = join(self.collection_prefix, source_document.source_id())
                   
                    self._submit_proxy(self._upload_doc, executor, queue, root_path, source_document, s3_client)

                    count += 1
                
            self._queue.put(count)

        except Exception as e:
            logger.exception(f'Error in doc publisher: {str(e)}')

    def _upload_batch(self, source_docs_batch:List[SourceDocument]):

        thread = threading.Thread(target=self._doc_publisher, daemon=True, kwargs={'source_documents': source_docs_batch, 'queue': self._queue})
        thread.start()

        count = 0
        target_count = None

        logger.debug(f'About to start polling queue [count: {count}, target_count: {target_count}]')

        while target_count is None or count < target_count:
            try:
                item = self._queue.get(timeout=60.0)
                if isinstance(item, int):
                    target_count = item
                else:
                    count += 1
                    yield item
                self._queue.task_done()
            except queue.Empty as e:
                continue
            
        logger.debug(f'Waiting on queue to empty [count: {count}, target_count: {target_count}]')

        thread.join()

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

class S3ChunkDownloader(BaseComponent):

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

        s3_client = GraphRAGConfig.s3

        collection_path = join(self.key_prefix,  self.collection_id, '')

        paginator = s3_client.get_paginator('list_objects_v2')
        source_doc_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=collection_path, Delimiter='/')

        source_doc_prefixes = [ 
            source_doc_obj['Prefix'] 
            for source_doc_page in source_doc_pages 
            for source_doc_obj in source_doc_page['CommonPrefixes']
             
        ]

        logger.debug(f'Started getting source documents from S3 [bucket: {self.bucket_name}, collection_path: {collection_path}, num_prefixes: {len(source_doc_prefixes)}]')

        with concurrent.futures.ThreadPoolExecutor(max_workers=GraphRAGConfig.extraction_num_threads_per_worker) as executor:

            for source_doc_prefix in source_doc_prefixes:
                
                chunk_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=source_doc_prefix)
                
                chunk_keys = [
                    chunk_obj['Key']
                    for chunk_page in chunk_pages
                    for chunk_obj in chunk_page['Contents'] 
                ]

                nodes = list(executor.map(
                    self._download_chunk,
                    chunk_keys,
                    repeat(s3_client)
                ))

                logger.debug(f'Yielding source document [source: {source_doc_prefix}, num_nodes: {len(nodes)}]')
            
                yield SourceDocument(nodes=nodes)

class S3ChunkUploader(BaseComponent):

    bucket_name:str
    collection_prefix:str
    s3_encryption_key_id:Optional[str]=None

    def _upload_chunk(self, root_path:str, n:TextNode, s3_client):
        chunk_output_path = join(root_path, f'{n.node_id}.json')
                    
        logger.debug(f'Writing chunk to S3: [bucket: {self.bucket_name}, key: {chunk_output_path}]')

        if self.s3_encryption_key_id:
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=chunk_output_path,
                Body=(bytes(json.dumps(n.to_dict(), indent=4).encode('UTF-8'))),
                ContentType='application/json',
                ServerSideEncryption='aws:kms',
                SSEKMSKeyId=self.s3_encryption_key_id
            )
        else:
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=chunk_output_path,
                Body=(bytes(json.dumps(n.to_dict(), indent=4).encode('UTF-8'))),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )

    def upload(self, source_documents: List[SourceDocument]):

        s3_client = GraphRAGConfig.s3
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=GraphRAGConfig.extraction_num_threads_per_worker) as executor:

            for source_document in source_documents:
        
                root_path =  join(self.collection_prefix, source_document.source_id())
                logger.debug(f'Writing source document to S3 [bucket: {self.bucket_name}, prefix: {root_path}]')

                futures = [
                    executor.submit(self._upload_chunk, root_path, n, s3_client)
                    for n in source_document.nodes 
                    if not [key for key in [INDEX_KEY] if key in n.metadata]
                ]

                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'Error uploading chunk: {str(e)}')

                yield source_document



class S3BasedDocs(NodeHandler):

    region:str
    bucket_name:str
    key_prefix:str
    collection_id:str
    s3_encryption_key_id:Optional[str]=None
    metadata_keys:Optional[List[str]]=None
    for_jsonl:Optional[bool]=False

    _uploader:Any = PrivateAttr(default=None)
    _downloader:Any = PrivateAttr(default=None)

    def __init__(self, 
                 region:str, 
                 bucket_name:str, 
                 key_prefix:str, 
                 collection_id:Optional[str]=None,
                 s3_encryption_key_id:Optional[str]=None, 
                 metadata_keys:Optional[List[str]]=None,
                 for_jsonl:Optional[bool]=False):
        
        super().__init__(
            region=region,
            bucket_name=bucket_name,
            key_prefix=key_prefix,
            collection_id=collection_id or datetime.now().strftime('%Y%m%d-%H%M%S'),
            s3_encryption_key_id=s3_encryption_key_id,
            metadata_keys=metadata_keys,
            for_jsonl=for_jsonl
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
                    fn=self._filter_metadata
                )
            else:
                self._downloader = S3ChunkDownloader(
                    key_prefix=self.key_prefix, 
                    collection_id=self.collection_id, 
                    bucket_name=self.bucket_name, 
                    fn=self._filter_metadata
                )

        path = join(self.key_prefix,  self.collection_id, '')

        logger.debug(f"Started getting source documents from S3 [bucket: {self.bucket_name}, prefix: {path}]")

        start = time.time()
        doc_count = 0

        for doc in self._downloader.download():
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
                self._uploader = S3DocUploader(
                    bucket_name=self.bucket_name, 
                    collection_prefix=collection_prefix,
                    s3_encryption_key_id=self.s3_encryption_key_id
                )
                
            else:
                self._uploader = S3ChunkUploader(
                    bucket_name=self.bucket_name, 
                    collection_prefix=collection_prefix,
                    s3_encryption_key_id=self.s3_encryption_key_id
                )
        
        for doc in self._uploader.upload(source_documents):
            doc_count += 1
            yield doc

        end = time.time()
        logger.debug(f'Finished writing {doc_count} source documents to S3 [bucket: {self.bucket_name}, prefix: {collection_prefix}] ({end - start} seconds)')
