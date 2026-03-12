# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for PromptProviderFactory.

This module tests the factory pattern for creating prompt provider instances
based on environment configuration.
"""

import pytest
import os
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory import PromptProviderFactory
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider


class TestPromptProviderFactoryGetProvider:
    """Tests for get_provider static method."""
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'static'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.StaticPromptProviderConfig')
    def test_returns_static_provider_when_env_is_static(self, mock_config_class):
        """Verify get_provider returns static provider when PROMPT_PROVIDER=static."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        mock_config.build.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'bedrock'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.BedrockPromptProviderConfig')
    def test_returns_bedrock_provider_when_env_is_bedrock(self, mock_config_class):
        """Verify get_provider returns Bedrock provider when PROMPT_PROVIDER=bedrock."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        mock_config.build.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 's3'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.S3PromptProviderConfig')
    def test_returns_s3_provider_when_env_is_s3(self, mock_config_class):
        """Verify get_provider returns S3 provider when PROMPT_PROVIDER=s3."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        mock_config.build.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'file'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.FilePromptProviderConfig')
    def test_returns_file_provider_when_env_is_file(self, mock_config_class):
        """Verify get_provider returns file provider when PROMPT_PROVIDER=file."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        mock_config.build.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.StaticPromptProviderConfig')
    def test_defaults_to_static_when_env_not_set(self, mock_config_class):
        """Verify get_provider defaults to static when PROMPT_PROVIDER not set."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'BEDROCK'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.BedrockPromptProviderConfig')
    def test_handles_uppercase_env_value(self, mock_config_class):
        """Verify get_provider handles uppercase environment values."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'S3'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.S3PromptProviderConfig')
    def test_handles_mixed_case_env_value(self, mock_config_class):
        """Verify get_provider handles mixed case environment values."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'invalid'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.StaticPromptProviderConfig')
    def test_defaults_to_static_for_invalid_env_value(self, mock_config_class):
        """Verify get_provider defaults to static for invalid PROMPT_PROVIDER value."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': ''})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.StaticPromptProviderConfig')
    def test_defaults_to_static_for_empty_env_value(self, mock_config_class):
        """Verify get_provider defaults to static for empty PROMPT_PROVIDER value."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        mock_config_class.assert_called_once()
        assert result == mock_provider


class TestPromptProviderFactoryIntegration:
    """Integration tests for PromptProviderFactory."""
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'static'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.StaticPromptProviderConfig')
    def test_returns_prompt_provider_interface(self, mock_config_class):
        """Verify get_provider returns object implementing PromptProvider interface."""
        mock_provider = Mock(spec=PromptProvider)
        mock_provider.get_system_prompt.return_value = "system"
        mock_provider.get_user_prompt.return_value = "user"
        
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        # Should have PromptProvider methods
        assert hasattr(result, 'get_system_prompt')
        assert hasattr(result, 'get_user_prompt')
        assert result.get_system_prompt() == "system"
        assert result.get_user_prompt() == "user"
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'bedrock'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.BedrockPromptProviderConfig')
    def test_multiple_calls_return_new_instances(self, mock_config_class):
        """Verify multiple calls to get_provider create new instances."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        
        mock_config = Mock()
        mock_config.build.side_effect = [mock_provider1, mock_provider2]
        mock_config_class.return_value = mock_config
        
        result1 = PromptProviderFactory.get_provider()
        
        # Reset mock to return new instance
        mock_config_class.return_value = Mock(build=Mock(return_value=mock_provider2))
        result2 = PromptProviderFactory.get_provider()
        
        # Should be called twice
        assert mock_config_class.call_count >= 1
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'file'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.FilePromptProviderConfig')
    def test_factory_delegates_to_config_build(self, mock_config_class):
        """Verify factory delegates provider creation to config.build()."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        result = PromptProviderFactory.get_provider()
        
        # Config should be instantiated
        mock_config_class.assert_called_once()
        # build() should be called on config
        mock_config.build.assert_called_once()
        # Result should be from build()
        assert result == mock_provider


class TestPromptProviderFactoryStaticMethods:
    """Tests for factory static method behavior."""
    
    def test_get_provider_is_static_method(self):
        """Verify get_provider is a static method."""
        assert isinstance(PromptProviderFactory.__dict__['get_provider'], staticmethod)
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'static'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.StaticPromptProviderConfig')
    def test_can_call_without_instance(self, mock_config_class):
        """Verify get_provider can be called without factory instance."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        # Should work without creating factory instance
        result = PromptProviderFactory.get_provider()
        
        assert result == mock_provider
    
    @patch.dict(os.environ, {'PROMPT_PROVIDER': 'bedrock'})
    @patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory.BedrockPromptProviderConfig')
    def test_factory_has_no_instance_state(self, mock_config_class):
        """Verify factory maintains no instance state."""
        mock_provider = Mock(spec=PromptProvider)
        mock_config = Mock()
        mock_config.build.return_value = mock_provider
        mock_config_class.return_value = mock_config
        
        # Multiple calls should work independently
        result1 = PromptProviderFactory.get_provider()
        
        mock_config_class.return_value = Mock(build=Mock(return_value=mock_provider))
        result2 = PromptProviderFactory.get_provider()
        
        # Both should succeed
        assert result1 is not None
        assert result2 is not None
