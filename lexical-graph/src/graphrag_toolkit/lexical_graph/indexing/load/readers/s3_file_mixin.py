# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


"""
S3 file handling mixin for reader providers that process file paths.
Provides universal S3 support for any reader that accepts file paths.
"""

import tempfile
import os
from typing import Union, List
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class S3FileMixin:
    """Mixin to add S3 file support to any path-based reader provider."""
    
    def _is_s3_path(self, path: str) -> bool:
        """Check if path is an S3 URL."""
        return path.startswith('s3://')
    
    def _download_s3_file(self, s3_path: str) -> str:
        """Download S3 file to a temporary location using GraphRAGConfig session."""
        try:
            from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        except ImportError as e:
            logger.error("GraphRAGConfig not available for S3 support")
            raise ImportError("S3 support requires GraphRAGConfig") from e
        
        try:
            s3_path_clean = s3_path.replace('s3://', '')
            if '/' not in s3_path_clean:
                logger.error(f"Invalid S3 path format: {s3_path}")
                raise ValueError(f"Invalid S3 path format: {s3_path}. Expected s3://bucket/key")
            
            bucket, key = s3_path_clean.split('/', 1)
            logger.info(f"Downloading S3 file: s3://{bucket}/{key}")
            
            aws_session = GraphRAGConfig.session
            s3_client = aws_session.client('s3')
            
            temp_file = tempfile.NamedTemporaryFile(
                delete=False, 
                suffix=os.path.splitext(key)[1]
            )
            s3_client.download_file(bucket, key, temp_file.name)
            temp_file.close()
            
            logger.debug(f"Downloaded to temporary file: {temp_file.name}")
            return temp_file.name
        except Exception as e:
            logger.error(f"Failed to download S3 file {s3_path}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to download S3 file: {e}") from e
    
    def _process_file_paths(self, paths: Union[str, List[str]]) -> tuple:
        """
        Process file paths, downloading S3 files to temp locations.
        Returns (processed_paths, temp_files_to_cleanup, original_paths)
        """
        if isinstance(paths, str):
            paths = [paths]
        
        processed_paths = []
        temp_files = []
        original_paths = []
        
        for path in paths:
            original_paths.append(path)
            if self._is_s3_path(path):
                try:
                    temp_path = self._download_s3_file(path)
                    processed_paths.append(temp_path)
                    temp_files.append(temp_path)
                except Exception as e:
                    logger.error(f"Failed to process S3 path {path}: {e}")
                    self._cleanup_temp_files(temp_files)
                    raise
            else:
                if not os.path.exists(path):
                    logger.error(f"Local file not found: {path}")
                    self._cleanup_temp_files(temp_files)
                    raise FileNotFoundError(f"File not found: {path}")
                processed_paths.append(path)
        
        return processed_paths, temp_files, original_paths
    
    def _cleanup_temp_files(self, temp_files: List[str]):
        """Clean up temporary files."""
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
                    logger.debug(f"Cleaned up temporary file: {temp_file}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file {temp_file}: {e}")
    
    def _get_file_source_type(self, original_path: str) -> str:
        """Get a source type for metadata."""
        return 's3' if self._is_s3_path(original_path) else 'local_file'
    
    def _get_s3_file_size(self, s3_path: str) -> int:
        """Get S3 file size in bytes."""
        try:
            from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        except ImportError as e:
            logger.error("GraphRAGConfig not available for S3 support")
            raise ImportError("S3 support requires GraphRAGConfig") from e
        
        try:
            s3_path_clean = s3_path.replace('s3://', '')
            bucket, key = s3_path_clean.split('/', 1)
            
            aws_session = GraphRAGConfig.session
            s3_client = aws_session.client('s3')
            
            response = s3_client.head_object(Bucket=bucket, Key=key)
            return response['ContentLength']
        except Exception as e:
            logger.error(f"Failed to get S3 file size for {s3_path}: {e}")
            raise RuntimeError(f"Failed to get S3 file size: {e}") from e
    
    def _get_s3_stream_url(self, s3_path: str) -> str:
        """Get S3 presigned URL for streaming."""
        try:
            from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        except ImportError as e:
            logger.error("GraphRAGConfig not available for S3 support")
            raise ImportError("S3 support requires GraphRAGConfig") from e
        
        try:
            s3_path_clean = s3_path.replace('s3://', '')
            bucket, key = s3_path_clean.split('/', 1)
            
            aws_session = GraphRAGConfig.session
            s3_client = aws_session.client('s3')
            
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=3600
            )
            logger.debug(f"Generated presigned URL for {s3_path}")
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for {s3_path}: {e}")
            raise RuntimeError(f"Failed to generate presigned URL: {e}") from e
    
    def _should_stream_s3_file(self, s3_path: str, stream_s3: bool, threshold_mb: int) -> bool:
        """Determine if S3 file should be streamed based on config and size."""
        if not stream_s3:
            return False
        
        try:
            file_size_bytes = self._get_s3_file_size(s3_path)
            file_size_mb = file_size_bytes / (1024 * 1024)
            should_stream = file_size_mb > threshold_mb
            logger.debug(f"File size: {file_size_mb:.2f}MB, threshold: {threshold_mb}MB, streaming: {should_stream}")
            return should_stream
        except Exception as e:
            logger.warning(f"Could not determine file size for {s3_path}, defaulting to download: {e}")
            return False