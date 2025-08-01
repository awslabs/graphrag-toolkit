from typing import List, Union
from llama_index.core.schema import Document
from ..reader_provider_config import StructuredDataReaderConfig
from ..base_reader_provider import BaseReaderProvider
from ..s3_file_mixin import S3FileMixin

class StructuredDataReaderProvider(BaseReaderProvider, S3FileMixin):
    """Provider for structured data files (CSV, Excel, etc.) with S3 support."""

    def __init__(self, config: StructuredDataReaderConfig):
        self.config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source: Union[str, List[str]]) -> List[Document]:
        """Read structured data documents from local files or S3."""
        import pandas as pd
        from pathlib import Path

        # Process file paths (handles S3 downloads)
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        documents = []
        
        try:
            for processed_path, original_path in zip(processed_paths, original_paths):
                try:
                    # Determine file type and pandas config
                    if original_path.lower().endswith('.csv'):
                        file_type = 'csv'
                        pandas_config = self.config.pandas_config or {}
                    elif original_path.lower().endswith(('.xlsx', '.xls')):
                        file_type = 'excel'
                        # Remove CSV-specific parameters for Excel
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
                        pandas_config['lines'] = True  # JSONL requires lines=True
                    else:
                        raise ValueError(f"Unsupported file type: {original_path}")

                    # Check if we should stream S3 files
                    if (self._is_s3_path(original_path) and 
                        self._should_stream_s3_file(original_path, self.config.stream_s3, self.config.stream_threshold_mb)):
                        # Stream large S3 files using presigned URL
                        stream_url = self._get_s3_stream_url(original_path)
                        
                        # Read directly from S3 stream using pandas
                        if file_type == 'csv':
                            df = pd.read_csv(stream_url, **pandas_config)
                        elif file_type == 'excel':
                            df = pd.read_excel(stream_url, **pandas_config)
                        elif file_type == 'json':
                            df = pd.read_json(stream_url, encoding='utf-8', **pandas_config)
                        elif file_type == 'jsonl':
                            df = pd.read_json(stream_url, encoding='utf-8', **pandas_config)
                    else:
                        # Read local files or small S3 files
                        file_path = Path(processed_path)
                        
                        if file_type == 'csv':
                            df = pd.read_csv(file_path, **pandas_config)
                        elif file_type == 'excel':
                            df = pd.read_excel(file_path, **pandas_config)
                        elif file_type == 'json':
                            df = pd.read_json(file_path, encoding='utf-8', **pandas_config)
                        elif file_type == 'jsonl':
                            df = pd.read_json(file_path, encoding='utf-8', **pandas_config)

                    # Convert DataFrame to documents
                    docs = []
                    
                    # Handle col_index
                    if isinstance(self.config.col_index, int):
                        df_text = df.iloc[:, self.config.col_index]
                    elif isinstance(self.config.col_index, list):
                        if all(isinstance(item, int) for item in self.config.col_index):
                            df_text = df.iloc[:, self.config.col_index]
                        else:
                            df_text = df[self.config.col_index]
                    else:
                        df_text = df[self.config.col_index]

                    # Convert to text list
                    if isinstance(df_text, pd.DataFrame):
                        text_list = df_text.apply(
                            lambda row: self.config.col_joiner.join(row.astype(str).tolist()), axis=1
                        ).tolist()
                    elif isinstance(df_text, pd.Series):
                        text_list = df_text.astype(str).tolist()

                    # Create documents
                    for text in text_list:
                        doc = Document(text=text)
                        docs.append(doc)

                    # Add metadata to each document
                    for doc in docs:
                        metadata = {
                            'file_path': original_path,  # Use original path in metadata
                            'file_type': file_type,
                            'source': self._get_file_source_type(original_path),
                            'document_type': 'structured_data',
                            'content_category': 'tabular_data'
                        }
                        
                        # Apply custom metadata function if provided
                        if self.metadata_fn:
                            custom_metadata = self.metadata_fn(original_path)
                            metadata.update(custom_metadata)
                        
                        doc.metadata.update(metadata)
                    
                    documents.extend(docs)
                    
                except Exception as e:
                    print(f"Error processing file {original_path}: {e}")
                    continue
        
        finally:
            # Always cleanup temp files
            self._cleanup_temp_files(temp_files)
        
        return documents