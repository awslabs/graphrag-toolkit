# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
from ..llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from ..reader_provider_config import JSONReaderConfig
from ..s3_file_mixin import S3FileMixin
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class JSONReaderProvider(LlamaIndexReaderProviderBase, S3FileMixin):
    """Reader provider for JSON files with S3 support using LlamaIndex's JSONReader."""

    def __init__(self, config: JSONReaderConfig):
        """Initialize with JSONReaderConfig."""
        try:
            from llama_index.readers.json import JSONReader
        except ImportError as e:
            logger.error("Failed to import JSONReader")
            raise ImportError(
                "llama-index-readers-json package not found, install with 'pip install llama-index-readers-json'"
            ) from e

        reader_kwargs = {
            "is_jsonl": config.is_jsonl,
            "clean_json": config.clean_json
        }
        
        if hasattr(config, 'levels_back') and config.levels_back is not None:
            reader_kwargs["levels_back"] = config.levels_back
        if hasattr(config, 'collapse_length') and config.collapse_length is not None:
            reader_kwargs["collapse_length"] = config.collapse_length
        if hasattr(config, 'ensure_ascii'):
            reader_kwargs["ensure_ascii"] = config.ensure_ascii
        
        super().__init__(config=config, reader_cls=JSONReader, **reader_kwargs)
        self.metadata_fn = config.metadata_fn
        logger.debug(f"Initialized JSONReaderProvider with is_jsonl={config.is_jsonl}")

    def read(self, input_source) -> List[Document]:
        """Read JSON documents from local files or S3 with metadata handling."""
        if not input_source:
            logger.error("No input source provided to JSONReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        logger.info(f"Reading JSON from: {input_source}")
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        try:
            documents = self._reader.load_data(input_file=processed_paths[0])
            logger.info(f"Successfully read {len(documents)} document(s) from JSON")
            
            if self.metadata_fn:
                for doc in documents:
                    additional_metadata = self.metadata_fn(original_paths[0])
                    doc.metadata.update(additional_metadata)
                    doc.metadata['source'] = self._get_file_source_type(original_paths[0])
            
            return documents
        except Exception as e:
            logger.error(f"Failed to read JSON from {input_source}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read JSON: {e}") from e
        finally:
            self._cleanup_temp_files(temp_files)