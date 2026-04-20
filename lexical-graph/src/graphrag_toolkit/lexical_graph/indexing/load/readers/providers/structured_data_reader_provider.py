# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List, Union
from llama_index.core.schema import Document
from ..reader_provider_config import StructuredDataReaderConfig
from ..base_reader_provider import BaseReaderProvider
from ..s3_file_mixin import S3FileMixin
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class StructuredDataReaderProvider(BaseReaderProvider, S3FileMixin):
    """Provider for structured data files (CSV, Excel, etc.) with S3 support."""

    def __init__(self, config: StructuredDataReaderConfig):
        self.config = config
        self.metadata_fn = config.metadata_fn
        logger.debug("Initialized StructuredDataReaderProvider")

    def read(self, input_source: Union[str, List[str]]) -> List[Document]:
        """Read structured data documents from local files or S3."""
        if not input_source:
            logger.error("No input source provided to StructuredDataReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        try:
            import pandas as pd
        except ImportError as e:
            logger.error("Failed to import pandas")
            raise ImportError("StructuredDataReaderProvider requires 'pandas'. Install with: pip install pandas") from e
        
        from pathlib import Path
        logger.info(f"Reading structured data from: {input_source}")
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        documents = []
        
        try:
            for processed_path, original_path in zip(processed_paths, original_paths):
                try:
                    if original_path.lower().endswith('.csv'):
                        file_type = 'csv'
                        pandas_config = self.config.pandas_config or {}
                    elif original_path.lower().endswith(('.xlsx', '.xls')):
                        file_type = 'excel'
                        pandas_config = {k: v for k, v in (self.config.pandas_config or {}).items() 
                                       if k not in ['sep', 'delimiter']}
                    elif original_path.lower().endswith('.json'):
                        file_type = 'json'
                        pandas_config = {k: v for k, v in (self.config.pandas_config or {}).items() 
                                       if k not in ['sep', 'delimiter']}
                    elif original_path.lower().endswith('.jsonl'):
                        file_type = 'jsonl'
                        pandas_config = {k: v for k, v in (self.config.pandas_config or {}).items() 
                                       if k not in ['sep', 'delimiter']}
                        pandas_config['lines'] = True
                    else:
                        logger.error(f"Unsupported file type: {original_path}")
                        raise ValueError(f"Unsupported file type: {original_path}")

                    logger.debug(f"Processing {file_type} file: {original_path}")
                    
                    if (self._is_s3_path(original_path) and 
                        self._should_stream_s3_file(original_path, self.config.stream_s3, self.config.stream_threshold_mb)):
                        stream_url = self._get_s3_stream_url(original_path)
                        logger.debug(f"Streaming large S3 file from presigned URL")
                        
                        if file_type == 'csv':
                            df = pd.read_csv(stream_url, **pandas_config)
                        elif file_type == 'excel':
                            df = pd.read_excel(stream_url, **pandas_config)
                        elif file_type in ['json', 'jsonl']:
                            df = pd.read_json(stream_url, encoding='utf-8', **pandas_config)
                    else:
                        file_path = Path(processed_path)
                        
                        if file_type == 'csv':
                            df = pd.read_csv(file_path, **pandas_config)
                        elif file_type == 'excel':
                            df = pd.read_excel(file_path, **pandas_config)
                        elif file_type in ['json', 'jsonl']:
                            df = pd.read_json(file_path, encoding='utf-8', **pandas_config)

                    if isinstance(self.config.col_index, int):
                        df_text = df.iloc[:, self.config.col_index]
                    elif isinstance(self.config.col_index, list):
                        if all(isinstance(item, int) for item in self.config.col_index):
                            df_text = df.iloc[:, self.config.col_index]
                        else:
                            df_text = df[self.config.col_index]
                    else:
                        df_text = df[self.config.col_index]

                    if isinstance(df_text, pd.DataFrame):
                        text_list = df_text.apply(
                            lambda row: self.config.col_joiner.join(row.astype(str).tolist()), axis=1
                        ).tolist()
                    elif isinstance(df_text, pd.Series):
                        text_list = df_text.astype(str).tolist()

                    docs = [Document(text=text) for text in text_list]

                    for doc in docs:
                        metadata = {
                            'file_path': original_path,
                            'file_type': file_type,
                            'source': self._get_file_source_type(original_path),
                            'document_type': 'structured_data',
                            'content_category': 'tabular_data'
                        }
                        
                        if self.metadata_fn:
                            custom_metadata = self.metadata_fn(original_path)
                            metadata.update(custom_metadata)
                        
                        doc.metadata.update(metadata)
                    
                    documents.extend(docs)
                    logger.info(f"Successfully processed {len(docs)} document(s) from {file_type} file")
                    
                except Exception as e:
                    logger.error(f"Failed to process file {original_path}: {e}", exc_info=True)
                    continue
        
        finally:
            self._cleanup_temp_files(temp_files)
        
        logger.info(f"Total documents read: {len(documents)}")
        return documents