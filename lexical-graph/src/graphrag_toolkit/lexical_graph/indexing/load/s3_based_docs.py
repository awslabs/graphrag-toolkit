# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import io
import json
import logging

from os.path import join
from datetime import datetime
from typing import List, Any, Generator, Optional, Dict

from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument, SourceType, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph import GraphRAGConfig

from llama_index.core.schema import TextNode

logger = logging.getLogger(__name__)

class S3BasedDocs(NodeHandler):

    region:str
    bucket_name:str
    key_prefix:str
    collection_id:str
    s3_encryption_key_id:Optional[str]=None
    metadata_keys:Optional[List[str]]=None

    def __init__(self, 
                 region:str, 
                 bucket_name:str, 
                 key_prefix:str, 
                 collection_id:Optional[str]=None,
                 s3_encryption_key_id:Optional[str]=None, 
                 metadata_keys:Optional[List[str]]=None):
        
        super().__init__(
            region=region,
            bucket_name=bucket_name,
            key_prefix=key_prefix,
            collection_id=collection_id or datetime.now().strftime('%Y%m%d-%H%M%S'),
            s3_encryption_key_id=s3_encryption_key_id,
            metadata_keys=metadata_keys
        )

    def docs(self):
        return self
    
    def _filter_metadata(self, node:TextNode) -> TextNode:

        def filter(metadata:Dict):
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
        s3_client = GraphRAGConfig.s3

        collection_path = join(self.key_prefix,  self.collection_id, '')

        logger.debug(f'Getting source documents from S3: [bucket: {self.bucket_name}, key: {collection_path}]')

        paginator = s3_client.get_paginator('list_objects_v2')
        source_doc_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=collection_path, Delimiter='/')

        source_doc_prefixes = [ 
            source_doc_obj['Prefix'] 
            for source_doc_page in source_doc_pages 
            for source_doc_obj in source_doc_page['CommonPrefixes']
             
        ]

        for source_doc_prefix in source_doc_prefixes:
            
            nodes = []
            
            chunk_pages = paginator.paginate(Bucket=self.bucket_name, Prefix=source_doc_prefix)
            
            chunk_keys = [
                chunk_obj['Key']
                for chunk_page in chunk_pages
                for chunk_obj in chunk_page['Contents'] 
            ]
            
            for chunk_key in chunk_keys:
                with io.BytesIO() as io_stream:
                    s3_client.download_fileobj(self.bucket_name, chunk_key, io_stream)        
                    io_stream.seek(0)
                    data = io_stream.read().decode('UTF-8')
                    nodes.append(self._filter_metadata(TextNode.from_json(data)))

            logger.debug(f'Yielding source document [source: {source_doc_prefix}, num_nodes: {len(nodes)}]')
           
            yield SourceDocument(nodes=nodes)

    def __call__(self, nodes: List[SourceType], **kwargs: Any) -> List[SourceDocument]:
        return [n for n in self.accept(source_documents_from_source_types(nodes), **kwargs)]

    def accept(self, source_documents: List[SourceDocument], **kwargs: Any) -> Generator[SourceDocument, None, None]:

        #TODO: Review
        s3_client = GraphRAGConfig.s3

        for source_document in source_documents:
            
            root_path =  join(self.key_prefix, self.collection_id, source_document.source_id())
            logger.debug(f'Writing source document to S3 [bucket: {self.bucket_name}, prefix: {root_path}]')

            for n in source_document.nodes:
                if not [key for key in [INDEX_KEY] if key in n.metadata]:

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

            yield source_document
