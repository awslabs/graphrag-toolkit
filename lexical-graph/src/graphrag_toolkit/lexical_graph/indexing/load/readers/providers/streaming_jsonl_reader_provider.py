"""
Streaming JSONL reader provider for memory-efficient processing of large JSONL files.
Processes files line-by-line without loading the entire file into memory.
"""

import json
import os
import time
from typing import Any, Dict, Iterator, List, Optional

from llama_index.core.schema import Document

from graphrag_toolkit.lexical_graph.logging import logging
from graphrag_toolkit.lexical_graph.indexing.load.readers.base_reader_provider import BaseReaderProvider
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import StreamingJSONLReaderConfig
from graphrag_toolkit.lexical_graph.indexing.load.readers.s3_file_mixin import S3FileMixin

logger = logging.getLogger(__name__)


class StreamingJSONLReaderProvider(BaseReaderProvider, S3FileMixin):
    """
    Streaming JSONL reader that processes files line-by-line.
    
    Unlike LlamaIndex's JSONReader which loads all lines into memory when is_jsonl=True,
    this implementation uses Python's file iteration to process files of any size with
    constant memory usage proportional to the configured batch size.
    """

    def __init__(self, config: StreamingJSONLReaderConfig):
        """
        Initialize with StreamingJSONLReaderConfig.
        
        Args:
            config: Configuration containing batch_size, text_field, strict_mode,
                   log_interval, and optional metadata_fn.
        """
        super().__init__(config)
        self.batch_size = config.batch_size
        self.text_field = config.text_field
        self.strict_mode = config.strict_mode
        self.log_interval = config.log_interval
        self.metadata_fn = config.metadata_fn
        logger.debug(
            f"Initialized StreamingJSONLReaderProvider with batch_size={self.batch_size}, "
            f"text_field={self.text_field}, strict_mode={self.strict_mode}"
        )

    def _process_line(
        self, line: str, line_number: int, source_path: str
    ) -> Optional[Document]:
        """
        Parse a single JSONL line into a Document.
        
        Args:
            line: Raw line string from the JSONL file
            line_number: 1-based line index for error reporting
            source_path: Original source path for metadata
            
        Returns:
            Document on success, None on parse failure (unless strict_mode)
            
        Raises:
            json.JSONDecodeError: If strict_mode=True and JSON is invalid
            ValueError: If strict_mode=True and text_field is missing
        """
        # Skip empty lines silently
        stripped_line = line.strip()
        if not stripped_line:
            return None
        
        try:
            json_obj = json.loads(stripped_line)
        except json.JSONDecodeError as e:
            if self.strict_mode:
                logger.error(f"Malformed JSON at line {line_number} in {source_path}: {e}")
                raise
            logger.warning(
                f"Skipping line {line_number} in {source_path}: "
                f"JSONDecodeError - {e.msg}"
            )
            return None
        
        # Extract text based on text_field configuration
        if self.text_field is None:
            # Use entire JSON line as string
            text = json.dumps(json_obj)
        elif self.text_field in json_obj:
            text = str(json_obj[self.text_field])
        else:
            # Missing text_field
            if self.strict_mode:
                error_msg = f"Missing text_field '{self.text_field}' at line {line_number}"
                logger.error(f"{error_msg} in {source_path}")
                raise ValueError(error_msg)
            logger.warning(
                f"Skipping line {line_number} in {source_path}: "
                f"missing text_field '{self.text_field}'"
            )
            return None
        
        # Build metadata and create document
        metadata = self._build_metadata(source_path, line_number)
        return Document(text=text, metadata=metadata)

    def _build_metadata(
        self, source_path: str, line_number: int
    ) -> Dict[str, Any]:
        """
        Build document metadata including source and line number.
        
        Args:
            source_path: Original source path (local or S3)
            line_number: 1-based line index
            
        Returns:
            Metadata dictionary with source, line_number, file_path,
            and any additional fields from metadata_fn
        """
        metadata = {
            "file_path": source_path,
            "source": self._get_file_source_type(source_path),
            "line_number": line_number,
            "document_type": "jsonl",
        }

        # Merge metadata from metadata_fn if provided
        if self.metadata_fn:
            try:
                if additional_metadata := self.metadata_fn(source_path):
                    metadata |= additional_metadata
            except Exception as e:
                logger.warning(f"metadata_fn failed for {source_path}: {e}")

        return metadata



    def lazy_load_data(self, input_source: str) -> Iterator[List[Document]]:
        """
        Yield document batches for memory-efficient processing.
        
        Processes the file line-by-line, yielding batches of batch_size documents.
        The final batch may contain fewer documents if the file doesn't divide evenly.
        
        Args:
            input_source: Local file path or S3 URI (s3://bucket/key)
            
        Yields:
            Lists of Documents, each containing up to batch_size documents
            
        Raises:
            ValueError: If input_source is None or empty
            FileNotFoundError: If local file doesn't exist
            RuntimeError: If S3 download fails
        """
        if not input_source:
            raise ValueError("input_source cannot be None or empty")

        # Process S3 paths - download to temp file
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        file_path = processed_paths[0]
        original_path = original_paths[0]

        try:
            lines_processed = 0
            docs_created = 0
            lines_skipped = 0
            batch: List[Document] = []

            with open(file_path, 'r', encoding='utf-8') as f:
                for line_number, line in enumerate(f, start=1):
                    lines_processed += 1

                    doc = self._process_line(line, line_number, original_path)

                    if doc is not None:
                        batch.append(doc)
                        docs_created += 1

                        # Yield batch when full
                        if len(batch) >= self.batch_size:
                            yield batch
                            batch = []
                    elif line.strip():
                        lines_skipped += 1

                    # Log progress at intervals
                    if lines_processed % self.log_interval == 0:
                        logger.info(
                            f"Progress: {lines_processed} lines processed, "
                            f"{docs_created} documents created from {original_path}"
                        )

            # Yield final partial batch
            if batch:
                yield batch

            logger.debug(
                f"Completed lazy_load_data: {lines_processed} lines, "
                f"{docs_created} documents, {lines_skipped} skipped"
            )

        finally:
            self._cleanup_temp_files(temp_files)

    def load_data(self, input_source: str) -> List[Document]:
        """
        LlamaIndex BaseReader interface - loads all documents.
        
        Args:
            input_source: Local file path or S3 URI
            
        Returns:
            Flat list of all documents from the JSONL file
        """
        return self.read(input_source)

    def read(self, input_source: str) -> List[Document]:
        """
        Read all documents from JSONL file (GraphRAG interface).
        
        Handles S3 paths via S3FileMixin, collects all documents from lazy_load_data
        into a flat list, and ensures temp file cleanup.
        
        Args:
            input_source: Local file path or S3 URI (s3://bucket/key)
            
        Returns:
            Flat list of all documents from the JSONL file
            
        Raises:
            ValueError: If input_source is None or empty
            FileNotFoundError: If local file doesn't exist
            RuntimeError: If S3 download fails
            json.JSONDecodeError: If strict_mode=True and JSON is invalid
        """
        if not input_source:
            logger.error("No input source provided to StreamingJSONLReaderProvider")
            raise ValueError("input_source cannot be None or empty")

        start_time = time.time()

        # Get file size for logging
        file_size_str = "unknown size"
        try:
            if self._is_s3_path(input_source):
                file_size = self._get_s3_file_size(input_source)
            else:
                file_size = os.path.getsize(input_source)
            file_size_str = f"{file_size / (1024 * 1024):.2f} MB"
        except Exception as e:
            logger.debug(f"Could not determine file size: {e}")

        logger.info(f"Reading JSONL from: {input_source} ({file_size_str})")

        # Collect all documents from lazy_load_data
        all_documents: List[Document] = []
        lines_skipped = 0

        # Process S3 paths - download to temp file
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        file_path = processed_paths[0]
        original_path = original_paths[0]

        try:
            lines_processed = 0

            with open(file_path, 'r', encoding='utf-8') as f:
                batch: List[Document] = []

                for line_number, line in enumerate(f, start=1):
                    lines_processed += 1

                    doc = self._process_line(line, line_number, original_path)

                    if doc is not None:
                        batch.append(doc)

                        # Process batch when full
                        if len(batch) >= self.batch_size:
                            all_documents.extend(batch)
                            batch = []
                    elif line.strip():
                        lines_skipped += 1

                    # Log progress at intervals
                    if lines_processed % self.log_interval == 0:
                        logger.info(
                            f"Progress: {lines_processed} lines processed, "
                            f"{len(all_documents) + len(batch)} documents created"
                        )

                # Add final partial batch
                if batch:
                    all_documents.extend(batch)

            duration = time.time() - start_time
            logger.info(
                f"Completed reading JSONL: {len(all_documents)} documents from "
                f"{lines_processed} lines ({lines_skipped} skipped) in {duration:.2f}s"
            )

            return all_documents

        except Exception as e:
            logger.error(f"Failed to read JSONL from {input_source}: {e}", exc_info=True)
            raise
        finally:
            self._cleanup_temp_files(temp_files)
