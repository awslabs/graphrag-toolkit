# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.lexical_graph.indexing.load.readers.s3_file_mixin import S3FileMixin


class TestS3FileMixinPathDetection:
    """Tests for S3 path detection."""
    
    def test_is_s3_path_detects_s3_url(self):
        """Verify _is_s3_path() detects S3 URLs."""
        mixin = S3FileMixin()
        
        assert mixin._is_s3_path("s3://bucket/key") is True
        assert mixin._is_s3_path("s3://my-bucket/path/to/file.txt") is True
    
    def test_is_s3_path_rejects_non_s3_paths(self):
        """Verify _is_s3_path() rejects non-S3 paths."""
        mixin = S3FileMixin()
        
        assert mixin._is_s3_path("/local/path/file.txt") is False
        assert mixin._is_s3_path("http://example.com/file") is False
        assert mixin._is_s3_path("file.txt") is False
        assert mixin._is_s3_path("") is False


class TestS3FileMixinDownload:
    """Tests for S3 file download functionality."""
    

class TestS3FileMixinProcessFilePaths:
    """Tests for file path processing."""
    
    def test_process_file_paths_with_local_file(self):
        """Verify _process_file_paths() handles local files."""
        mixin = S3FileMixin()
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            processed, temp_files, original = mixin._process_file_paths(temp_path)
            
            assert len(processed) == 1
            assert processed[0] == temp_path
            assert len(temp_files) == 0
            assert original[0] == temp_path
        finally:
            os.unlink(temp_path)
    
    def test_process_file_paths_with_missing_local_file(self):
        """Verify _process_file_paths() handles missing local files."""
        mixin = S3FileMixin()
        
        with pytest.raises(FileNotFoundError, match="File not found"):
            mixin._process_file_paths("/nonexistent/file.txt")
    
    def test_process_file_paths_with_multiple_files(self):
        """Verify _process_file_paths() handles multiple files."""
        mixin = S3FileMixin()
        
        with tempfile.NamedTemporaryFile(delete=False) as temp1, \
             tempfile.NamedTemporaryFile(delete=False) as temp2:
            paths = [temp1.name, temp2.name]
        
        try:
            processed, temp_files, original = mixin._process_file_paths(paths)
            
            assert len(processed) == 2
            assert len(temp_files) == 0
            assert len(original) == 2
        finally:
            os.unlink(temp1.name)
            os.unlink(temp2.name)


class TestS3FileMixinCleanup:
    """Tests for temporary file cleanup."""
    
    def test_cleanup_temp_files_removes_files(self):
        """Verify _cleanup_temp_files() removes temporary files."""
        mixin = S3FileMixin()
        
        # Create temporary files
        temp_files = []
        for _ in range(3):
            with tempfile.NamedTemporaryFile(delete=False) as f:
                temp_files.append(f.name)
        
        # Verify files exist
        for path in temp_files:
            assert os.path.exists(path)
        
        # Cleanup
        mixin._cleanup_temp_files(temp_files)
        
        # Verify files are removed
        for path in temp_files:
            assert not os.path.exists(path)
    
    def test_cleanup_temp_files_handles_missing_files(self):
        """Verify _cleanup_temp_files() handles already-deleted files."""
        mixin = S3FileMixin()
        
        # Try to cleanup non-existent files (should not raise error)
        mixin._cleanup_temp_files(["/nonexistent/file1.txt", "/nonexistent/file2.txt"])
    
    def test_cleanup_temp_files_with_empty_list(self):
        """Verify _cleanup_temp_files() handles empty list."""
        mixin = S3FileMixin()
        
        # Should not raise error
        mixin._cleanup_temp_files([])


class TestS3FileMixinSourceType:
    """Tests for source type detection."""
    
    def test_get_file_source_type_for_s3(self):
        """Verify _get_file_source_type() detects S3 sources."""
        mixin = S3FileMixin()
        
        source_type = mixin._get_file_source_type("s3://bucket/file.txt")
        
        assert source_type == "s3"
    
    def test_get_file_source_type_for_local(self):
        """Verify _get_file_source_type() detects local sources."""
        mixin = S3FileMixin()
        
        source_type = mixin._get_file_source_type("/local/path/file.txt")
        
        assert source_type == "local_file"


class TestS3FileMixinFileSize:
    """Tests for S3 file size operations."""
    
class TestS3FileMixinStreamURL:
    """Tests for S3 presigned URL generation."""
    
class TestS3FileMixinStreamingDecision:
    """Tests for streaming decision logic."""
    
    def test_should_stream_s3_file_when_streaming_disabled(self):
        """Verify _should_stream_s3_file() returns False when streaming disabled."""
        mixin = S3FileMixin()
        
        should_stream = mixin._should_stream_s3_file(
            "s3://bucket/file.txt",
            stream_s3=False,
            threshold_mb=100
        )
        
        assert should_stream is False
    
class TestS3FileMixinIntegration:
    """Integration tests for S3FileMixin."""
    
    def test_mixin_can_be_used_in_class(self):
        """Verify S3FileMixin can be mixed into a class."""
        class TestReader(S3FileMixin):
            def read_file(self, path):
                processed, temp_files, original = self._process_file_paths(path)
                try:
                    # Simulate reading
                    return f"Read from {processed[0]}"
                finally:
                    self._cleanup_temp_files(temp_files)
        
        reader = TestReader()
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            result = reader.read_file(temp_path)
            assert temp_path in result
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
