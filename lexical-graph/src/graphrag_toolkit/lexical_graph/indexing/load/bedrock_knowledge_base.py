# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import os
import io
import uuid
import logging
import shutil
import copy
import base64
from pathlib import Path
from typing import Callable, Dict, Any
from os.path import join
from urllib.parse import urlparse

from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.load.file_based_chunks import FileBasedChunks
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter
from graphrag_toolkit.lexical_graph import GraphRAGConfig

from llama_index.core.schema import TextNode, Document
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo

logger = logging.getLogger(__name__)

class TempFile():
    def __init__(self, filepath):
        self.filepath = filepath
        
    def __enter__(self):
        self.file = open(self.filepath)
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.file.close()
        os.remove(self.filepath)
        
    def readline(self):
        return self.file.readline()
    
class TempDir():
    def __init__(self, dir_path):
        self.dir_path = dir_path
        
    def __enter__(self):
        if not os.path.exists(self.dir_path):
            os.makedirs(self.dir_path)
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        if os.path.exists(self.dir_path):
            shutil.rmtree(self.dir_path)

class BedrockKnowledgeBaseExport():

    def __init__(self, 
                 region:str, 
                 bucket_name:str, 
                 key_prefix:str, 
                 limit:int=-1, 
                 output_dir:str='output', 
                 metadata_fn:Callable[[str], Dict[str, Any]]=None,
                 include_embeddings:bool=True,
                 include_source_doc:bool=False,
                 tenant_id:str=None,
                 **kwargs):
        
        self.bucket_name=bucket_name
        self.key_prefix=key_prefix
        self.region=region
        self.limit=limit
        self.output_dir = output_dir
        self.s3_client = GraphRAGConfig.s3
        self.id_rewriter = IdRewriter(id_generator=IdGenerator(tenant_id=tenant_id))
        self.metadata_fn=metadata_fn
        self.include_embeddings = include_embeddings
        self.include_source_doc = include_source_doc
        
    def _kb_chunks(self, kb_export_dir):

        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.key_prefix)

        keys = [
            obj['Key']
            for page in pages
            for obj in page['Contents'] 
        ]
        
        for key in keys:
        
            logger.info(f'Loading Amazon Bedrock Knowledge Base export file [bucket: {self.bucket_name}, key: {key}, region: {self.region}]')

            temp_filepath = join(kb_export_dir, f'{uuid.uuid4().hex}.json')
            self.s3_client.download_file(self.bucket_name, key, temp_filepath)

            with TempFile(temp_filepath) as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    else:
                        yield json.loads(line)
                        
    def _parse_key(self, source):
        
        parsed = urlparse(source, allow_fragments=False)
        return parsed.path.lstrip('/')
    
    def _download_source_doc(self, source, doc_file_path):

        key = self._parse_key(source)

        logger.debug(f'Loading Amazon Bedrock Knowledge Base underlying source document [source: {source}, bucket: {self.bucket_name}, key: {key}, region: {self.region}]')
            
        object_metadata = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
        content_type = object_metadata.get('ContentType', None)

        with io.BytesIO() as io_stream:
            self.s3_client.download_fileobj(self.bucket_name, key, io_stream)
        
            io_stream.seek(0)

            if content_type and content_type in ['application/pdf']:
                data = base64.b64encode(io_stream.read())
            else:
                data = io_stream.read().decode('utf-8')
            
        metadata = self.metadata_fn(data) if self.metadata_fn else {}

        if 'source' not in metadata:
            metadata['source'] = source
            
        doc = Document(
            text=data,
            metadata=metadata
        )
        
        doc = self.id_rewriter([doc])[0]
        
        with open(doc_file_path, 'w') as f:
                f.write(doc.to_json())
        
        return doc
    
    def _open_source_doc(self, doc_file_path):
        with open(doc_file_path) as f:
            data = json.load(f)
            return Document.from_dict(data)
    
    def _get_source_doc(self, source_docs_dir, source):
                
        source_id = get_hash(source)
        doc_directory_path = join(source_docs_dir, source_id, 'document')
        doc_file_path = join(doc_directory_path, 'source_doc')
        
        if os.path.exists(doc_file_path):
            return self._open_source_doc(doc_file_path)
        else:
            if not os.path.exists(doc_directory_path):
                os.makedirs(doc_directory_path)
            return self._download_source_doc(source, doc_file_path)
            
    def _save_chunk(self, source_docs_dir, chunk, source):

        chunk = self.id_rewriter([chunk])[0]
                
        source_id = get_hash(source)
        chunks_directory_path = join(source_docs_dir, source_id, 'chunks')
        chunk_file_path = join(chunks_directory_path, chunk.id_)
        
        if not os.path.exists(chunks_directory_path):
            os.makedirs(chunks_directory_path)
            
        with open(chunk_file_path, 'w') as f:
                f.write(chunk.to_json())
    
    def _get_doc_count(self, source_docs_dir):
        doc_count = len([name for name in os.listdir(source_docs_dir) if os.path.isfile(name)]) - 1
        logger.info(f'doc_count: {doc_count}')
        return doc_count
    
    def docs(self):
        return self
    
    def _with_page_number(self, metadata, page_number):
        if page_number:
            metadata_copy = copy.deepcopy(metadata)
            metadata_copy['page_number'] = page_number
            return metadata_copy
        else:
            return metadata

    def __iter__(self):

        job_dir = join(self.output_dir, 'bedrock-kb-export', f'{uuid.uuid4().hex}')
        
        bedrock_dir = join(job_dir, 'bedrock')
        llama_index_dir = join(job_dir, 'llama-index')
        
        logger.info(f'Creating Amazon Bedrock Knowledge Base temp directories [bedrock_dir: {bedrock_dir}, llama_index_dir: {llama_index_dir}]')

        count = 0
        
        with TempDir(job_dir) as j, TempDir(bedrock_dir) as k, TempDir(llama_index_dir) as s:
        
            for kb_chunk in self._kb_chunks(bedrock_dir):

                bedrock_id = kb_chunk['id']
                page_number = kb_chunk.get('x-amz-bedrock-kb-document-page-number', None)
                metadata = json.loads(kb_chunk['AMAZON_BEDROCK_METADATA'])
                source = metadata['source']
                
                source_doc = self._get_source_doc(llama_index_dir, source)
                
                chunk = TextNode()

                chunk.text = kb_chunk['AMAZON_BEDROCK_TEXT']
                chunk.metadata = metadata
                chunk.metadata['bedrock_id'] = bedrock_id
                if self.include_embeddings:
                    chunk.embedding = kb_chunk['bedrock-knowledge-base-default-vector']
                chunk.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
                    node_id=source_doc.id_,
                    node_type=NodeRelationship.SOURCE,
                    metadata=source_doc.metadata,
                    hash=source_doc.hash
                )
                
                self._save_chunk(llama_index_dir, chunk, source)
                    
            for d in [d for d in Path(llama_index_dir).iterdir() if d.is_dir()]:
            
                document = None
                
                if self.include_source_doc:
                    source_doc_file_path = join(d, 'document', 'source_doc')
                    with open(source_doc_file_path) as f:
                        document = Document.from_json(f.read())
                 
                file_based_chunks = FileBasedChunks(str(d), 'chunks')
                chunks = [c for c in file_based_chunks.chunks()]
                
                yield SourceDocument(refNode=document, nodes=chunks)
                
                count += 1
                if self.limit > 0 and count >= self.limit:
                    break