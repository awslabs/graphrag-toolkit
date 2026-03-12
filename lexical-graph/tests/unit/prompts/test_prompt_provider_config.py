# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import (
    BedrockPromptProviderConfig,
    S3PromptProviderConfig,
    FilePromptProviderConfig,
    StaticPromptProviderConfig
)


class TestBedrockPromptProviderConfig:
    """Tests for BedrockPromptProviderConfig."""
    
    def test_resolved_system_prompt_arn_with_full_arn(self):
        """Test that full ARN is returned as-is."""
        config = BedrockPromptProviderConfig(
            system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/my-prompt",
            user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/user-prompt"
        )
        assert config.resolved_system_prompt_arn == "arn:aws:bedrock:us-east-1:123456789012:prompt/my-prompt"
    
    def test_resolved_user_prompt_arn_with_full_arn(self):
        """Test that full ARN is returned as-is for user prompt."""
        config = BedrockPromptProviderConfig(
            system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/sys-prompt",
            user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/user-prompt"
        )
        assert config.resolved_user_prompt_arn == "arn:aws:bedrock:us-east-1:123456789012:prompt/user-prompt"
    
    @patch.object(BedrockPromptProviderConfig, 'sts')
    def test_resolve_prompt_arn_with_identifier(self, mock_sts):
        """Test ARN resolution from identifier."""
        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:sts::123456789012:assumed-role/test-role",
            "Account": "123456789012"
        }
        
        config = BedrockPromptProviderConfig(
            system_prompt_arn="my-prompt-id",
            user_prompt_arn="user-prompt-id",
            aws_region="us-west-2"
        )
        
        resolved = config._resolve_prompt_arn("my-prompt-id")
        assert resolved == "arn:aws:bedrock:us-west-2:123456789012:prompt/my-prompt-id"
    
    @patch('graphrag_toolkit.lexical_graph.prompts.bedrock_prompt_provider.BedrockPromptProvider')
    def test_build(self, mock_provider_class):
        """Test build method creates BedrockPromptProvider."""
        mock_provider = Mock()
        mock_provider_class.return_value = mock_provider
        
        config = BedrockPromptProviderConfig(
            system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/sys",
            user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/user"
        )
        
        result = config.build()
        
        mock_provider_class.assert_called_once_with(config=config)
        assert result == mock_provider


class TestS3PromptProviderConfig:
    """Tests for S3PromptProviderConfig."""
    
    def test_default_values_from_env(self):
        """Test that default values are loaded from environment."""
        with patch.dict(os.environ, {
            'PROMPT_S3_BUCKET': 'test-bucket',
            'PROMPT_S3_PREFIX': 'test-prefix/'
        }):
            config = S3PromptProviderConfig()
            assert config.bucket == 'test-bucket'
            assert config.prefix == 'test-prefix/'
    
    def test_default_prefix_when_not_set(self):
        """Test default prefix when env var not set."""
        with patch.dict(os.environ, {'PROMPT_S3_BUCKET': 'test-bucket'}, clear=True):
            config = S3PromptProviderConfig()
            assert config.prefix == 'prompts/'
    
    @patch('graphrag_toolkit.lexical_graph.prompts.s3_prompt_provider.S3PromptProvider')
    def test_build(self, mock_provider_class):
        """Test build method creates S3PromptProvider."""
        mock_provider = Mock()
        mock_provider_class.return_value = mock_provider
        
        with patch.dict(os.environ, {'PROMPT_S3_BUCKET': 'test-bucket'}):
            config = S3PromptProviderConfig()
            result = config.build()
            
            mock_provider_class.assert_called_once_with(config=config)
            assert result == mock_provider


class TestFilePromptProviderConfig:
    """Tests for FilePromptProviderConfig."""
    
    def test_default_base_path_from_env(self):
        """Test that default base path is loaded from environment."""
        with patch.dict(os.environ, {'PROMPT_PATH': '/custom/path'}):
            config = FilePromptProviderConfig()
            assert config.base_path == '/custom/path'
    
    def test_default_base_path_when_not_set(self):
        """Test default base path when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = FilePromptProviderConfig()
            assert config.base_path == './prompts'
    
    def test_custom_prompt_files(self):
        """Test custom prompt file names."""
        config = FilePromptProviderConfig(
            base_path='/test',
            system_prompt_file='custom_system.txt',
            user_prompt_file='custom_user.txt'
        )
        assert config.system_prompt_file == 'custom_system.txt'
        assert config.user_prompt_file == 'custom_user.txt'
    
    @patch('graphrag_toolkit.lexical_graph.prompts.file_prompt_provider.FilePromptProvider')
    def test_build(self, mock_provider_class):
        """Test build method creates FilePromptProvider."""
        mock_provider = Mock()
        mock_provider_class.return_value = mock_provider
        
        config = FilePromptProviderConfig(base_path='/test')
        result = config.build()
        
        mock_provider_class.assert_called_once_with(
            config=config,
            system_prompt_file='system_prompt.txt',
            user_prompt_file='user_prompt.txt'
        )
        assert result == mock_provider


class TestStaticPromptProviderConfig:
    """Tests for StaticPromptProviderConfig."""
    
    @patch('graphrag_toolkit.lexical_graph.prompts.static_prompt_provider.StaticPromptProvider')
    def test_build(self, mock_provider_class):
        """Test build method creates StaticPromptProvider."""
        mock_provider = Mock()
        mock_provider_class.return_value = mock_provider
        
        result = StaticPromptProviderConfig.build()
        
        mock_provider_class.assert_called_once_with()
        assert result == mock_provider
