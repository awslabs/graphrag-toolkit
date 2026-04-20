# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List, Optional, Dict, Any, Union
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)


class UniversalDirectoryReaderConfig(ReaderProviderConfig):
    """Config for UniversalDirectoryReaderProvider."""
    input_dir: Optional[str] = None
    input_files: Optional[List[str]] = None
    exclude_hidden: bool = True
    recursive: bool = False
    required_exts: Optional[List[str]] = None
    file_extractor: Optional[Dict[str, Any]] = None
    metadata_fn: Optional[callable] = None
    # S3BasedDocs params
    region: Optional[str] = None
    bucket_name: Optional[str] = None
    key_prefix: Optional[str] = None
    collection_id: Optional[str] = None


class UniversalDirectoryReaderProvider(LlamaIndexReaderProviderBase):
    """SimpleDirectoryReader for local, S3BasedDocs for S3."""
    
    def __init__(self, config: UniversalDirectoryReaderConfig):
        self.config = config
        self.metadata_fn = config.metadata_fn
        logger.debug("Initialized UniversalDirectoryReaderProvider")
    
    def read(self, input_source: Optional[Union[str, Dict[str, str]]] = None) -> List[Document]:
        """Read from local or S3 based on config/input."""
        
        if isinstance(input_source, dict) or self.config.bucket_name:
            return self._read_from_s3(input_source)
        else:
            return self._read_from_local(input_source)
    
    def _read_from_local(self, input_source: Optional[str] = None) -> List[Document]:
        """Read from local using SimpleDirectoryReader."""
        try:
            from llama_index.core import SimpleDirectoryReader
        except ImportError as e:
            logger.error("Failed to import SimpleDirectoryReader")
            raise ImportError("SimpleDirectoryReader requires 'llama-index'. Install with: pip install llama-index") from e
        
        input_dir = input_source or self.config.input_dir
        
        if not input_dir and not self.config.input_files:
            logger.error("No input directory or files provided")
            raise ValueError("Either input_dir or input_files must be provided")
        
        logger.info(f"Reading from local: {input_dir or self.config.input_files}")
        
        try:
            reader_kwargs = {
                "exclude_hidden": self.config.exclude_hidden,
                "recursive": self.config.recursive
            }
            
            if input_dir:
                reader_kwargs["input_dir"] = input_dir
            if self.config.input_files:
                reader_kwargs["input_files"] = self.config.input_files
            if self.config.required_exts:
                reader_kwargs["required_exts"] = self.config.required_exts
            if self.config.file_extractor:
                reader_kwargs["file_extractor"] = self.config.file_extractor
            
            reader = SimpleDirectoryReader(**reader_kwargs)
            documents = reader.load_data()
            
            logger.info(f"Successfully read {len(documents)} document(s) from local")
            
            if self.metadata_fn:
                for doc in documents:
                    additional_metadata = self.metadata_fn(input_dir or self.config.input_files)
                    doc.metadata.update(additional_metadata)
            
            return documents
            
        except Exception as e:
            logger.error(f"Failed to read from local: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read documents: {e}") from e
    
    def _read_from_s3(self, s3_config: Optional[Dict[str, str]] = None) -> List[Document]:
        """Read from S3 using S3BasedDocs."""
        try:
            from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs
        except ImportError as e:
            logger.error("Failed to import S3BasedDocs")
            raise ImportError("S3BasedDocs not available") from e
        
        region = (s3_config or {}).get('region') or self.config.region
        bucket_name = (s3_config or {}).get('bucket_name') or self.config.bucket_name
        key_prefix = (s3_config or {}).get('key_prefix') or self.config.key_prefix
        collection_id = (s3_config or {}).get('collection_id') or self.config.collection_id
        
        if not all([region, bucket_name, key_prefix, collection_id]):
            logger.error("Missing S3 configuration")
            raise ValueError("S3 requires: region, bucket_name, key_prefix, collection_id")
        
        logger.info(f"Reading from S3: s3://{bucket_name}/{key_prefix}/{collection_id}")
        
        try:
            s3_docs = S3BasedDocs(
                region=region,
                bucket_name=bucket_name,
                key_prefix=key_prefix,
                collection_id=collection_id
            )
            
            documents = []
            for source_doc in s3_docs:
                for node in source_doc.nodes:
                    doc = Document(text=node.text, metadata=node.metadata)
                    if self.metadata_fn:
                        additional_metadata = self.metadata_fn(f"s3://{bucket_name}/{key_prefix}/{collection_id}")
                        doc.metadata.update(additional_metadata)
                    documents.append(doc)
            
            logger.info(f"Successfully read {len(documents)} document(s) from S3")
            return documents
            
        except Exception as e:
            logger.error(f"Failed to read from S3: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read from S3: {e}") from e
