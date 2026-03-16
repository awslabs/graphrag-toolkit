# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for config.py module.

This module tests configuration loading, validation, and defaults.
"""

import pytest
import os
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.config import (
    GraphRAGConfig,
    _is_json_string,
    string_to_bool,
    DEFAULT_EXTRACTION_MODEL,
    DEFAULT_RESPONSE_MODEL,
    DEFAULT_EMBEDDINGS_MODEL,
    DEFAULT_EXTRACTION_NUM_WORKERS,
    DEFAULT_EXTRACTION_BATCH_SIZE,
    DEFAULT_BUILD_NUM_WORKERS,
    DEFAULT_BUILD_BATCH_SIZE
)


class TestHelperFunctions:
    """Tests for helper functions in config module."""
    
    def test_is_json_string_valid(self):
        """Verify _is_json_string returns True for valid JSON."""
        assert _is_json_string('{"key": "value"}') is True
        assert _is_json_string('[]') is True
        assert _is_json_string('null') is True
        assert _is_json_string('123') is True
    
    def test_is_json_string_invalid(self):
        """Verify _is_json_string returns False for invalid JSON."""
        assert _is_json_string('not json') is False
        assert _is_json_string('{invalid}') is False
        assert _is_json_string('') is False
    
    def test_string_to_bool_true(self):
        """Verify string_to_bool converts 'true' to True."""
        assert string_to_bool('true', False) is True
        assert string_to_bool('True', False) is True
        assert string_to_bool('TRUE', False) is True
    
    def test_string_to_bool_false(self):
        """Verify string_to_bool converts non-'true' strings to False."""
        assert string_to_bool('false', True) is False
        assert string_to_bool('no', True) is False
        assert string_to_bool('0', True) is False
    
    def test_string_to_bool_default(self):
        """Verify string_to_bool returns default for empty/None strings."""
        assert string_to_bool('', True) is True
        assert string_to_bool('', False) is False
        assert string_to_bool(None, True) is True
        assert string_to_bool(None, False) is False


class TestGraphRAGConfigDefaults:
    """Tests for GraphRAGConfig default values."""
    
    def test_initialization_with_defaults(self):
        """Verify GraphRAGConfig initializes with default values."""
        # GraphRAGConfig is a singleton, so we test its default properties
        assert GraphRAGConfig.extraction_num_workers == DEFAULT_EXTRACTION_NUM_WORKERS
        assert GraphRAGConfig.extraction_batch_size == DEFAULT_EXTRACTION_BATCH_SIZE
        assert GraphRAGConfig.build_num_workers == DEFAULT_BUILD_NUM_WORKERS
        assert GraphRAGConfig.build_batch_size == DEFAULT_BUILD_BATCH_SIZE
    
    def test_aws_profile_from_environment(self):
        """Verify aws_profile reads from AWS_PROFILE environment variable."""
        with patch.dict(os.environ, {'AWS_PROFILE': 'test-profile'}):
            # Reset the cached value
            GraphRAGConfig._aws_profile = None
            assert GraphRAGConfig.aws_profile == 'test-profile'
    
    def test_aws_region_from_environment(self):
        """Verify aws_region reads from AWS_REGION environment variable."""
        with patch.dict(os.environ, {'AWS_REGION': 'us-west-2'}):
            # Reset the cached value
            GraphRAGConfig._aws_region = None
            assert GraphRAGConfig.aws_region == 'us-west-2'


class TestGraphRAGConfigSetters:
    """Tests for GraphRAGConfig property setters."""
    
    def test_set_extraction_num_workers(self):
        """Verify extraction_num_workers setter updates value."""
        original = GraphRAGConfig.extraction_num_workers
        try:
            GraphRAGConfig.extraction_num_workers = 10
            assert GraphRAGConfig.extraction_num_workers == 10
        finally:
            GraphRAGConfig.extraction_num_workers = original
    
    def test_set_extraction_batch_size(self):
        """Verify extraction_batch_size setter updates value."""
        original = GraphRAGConfig.extraction_batch_size
        try:
            GraphRAGConfig.extraction_batch_size = 8
            assert GraphRAGConfig.extraction_batch_size == 8
        finally:
            GraphRAGConfig.extraction_batch_size = original
    
    def test_set_build_num_workers(self):
        """Verify build_num_workers setter updates value."""
        original = GraphRAGConfig.build_num_workers
        try:
            GraphRAGConfig.build_num_workers = 4
            assert GraphRAGConfig.build_num_workers == 4
        finally:
            GraphRAGConfig.build_num_workers = original
    
    def test_set_build_batch_size(self):
        """Verify build_batch_size setter updates value."""
        original = GraphRAGConfig.build_batch_size
        try:
            GraphRAGConfig.build_batch_size = 6
            assert GraphRAGConfig.build_batch_size == 6
        finally:
            GraphRAGConfig.build_batch_size = original
    
    def test_set_aws_profile_clears_clients(self):
        """Verify setting aws_profile clears cached AWS clients."""
        original_profile = GraphRAGConfig._aws_profile
        original_clients = GraphRAGConfig._aws_clients.copy()
        try:
            # Add a mock client to the cache
            GraphRAGConfig._aws_clients['test_service'] = Mock()
            assert len(GraphRAGConfig._aws_clients) > 0
            
            # Setting profile should clear clients
            GraphRAGConfig.aws_profile = 'new-profile'
            assert len(GraphRAGConfig._aws_clients) == 0
        finally:
            GraphRAGConfig._aws_profile = original_profile
            GraphRAGConfig._aws_clients = original_clients
    
    def test_set_aws_region_clears_clients(self):
        """Verify setting aws_region clears cached AWS clients."""
        original_region = GraphRAGConfig._aws_region
        original_clients = GraphRAGConfig._aws_clients.copy()
        try:
            # Add a mock client to the cache
            GraphRAGConfig._aws_clients['test_service'] = Mock()
            assert len(GraphRAGConfig._aws_clients) > 0
            
            # Setting region should clear clients
            GraphRAGConfig.aws_region = 'eu-west-1'
            assert len(GraphRAGConfig._aws_clients) == 0
        finally:
            GraphRAGConfig._aws_region = original_region
            GraphRAGConfig._aws_clients = original_clients


class TestGraphRAGConfigEnvironmentVariables:
    """Tests for GraphRAGConfig reading from environment variables."""
    
    def test_extraction_num_workers_from_env(self):
        """Verify extraction_num_workers reads from EXTRACTION_NUM_WORKERS env var."""
        with patch.dict(os.environ, {'EXTRACTION_NUM_WORKERS': '5'}):
            GraphRAGConfig._extraction_num_workers = None
            assert GraphRAGConfig.extraction_num_workers == 5
    
    def test_extraction_batch_size_from_env(self):
        """Verify extraction_batch_size reads from EXTRACTION_BATCH_SIZE env var."""
        with patch.dict(os.environ, {'EXTRACTION_BATCH_SIZE': '10'}):
            GraphRAGConfig._extraction_batch_size = None
            assert GraphRAGConfig.extraction_batch_size == 10
    
    def test_build_num_workers_from_env(self):
        """Verify build_num_workers reads from BUILD_NUM_WORKERS env var."""
        with patch.dict(os.environ, {'BUILD_NUM_WORKERS': '3'}):
            GraphRAGConfig._build_num_workers = None
            assert GraphRAGConfig.build_num_workers == 3
    
    def test_build_batch_size_from_env(self):
        """Verify build_batch_size reads from BUILD_BATCH_SIZE env var."""
        with patch.dict(os.environ, {'BUILD_BATCH_SIZE': '7'}):
            GraphRAGConfig._build_batch_size = None
            assert GraphRAGConfig.build_batch_size == 7
