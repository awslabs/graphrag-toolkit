# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for BedrockPromptProvider.

This module tests the Bedrock-based prompt provider which loads prompts
from AWS Bedrock using prompt ARNs.
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.lexical_graph.prompts.bedrock_prompt_provider import BedrockPromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import BedrockPromptProviderConfig
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider


class TestBedrockPromptProviderInitialization:
    """Tests for BedrockPromptProvider initialization."""
    
    def test_initialization_with_config(self):
        """Verify BedrockPromptProvider initializes with config."""
        config = BedrockPromptProviderConfig(
            system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/sys",
            user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/user"
        )
        provider = BedrockPromptProvider(config)
        
        assert provider.config == config
    
    def test_is_prompt_provider_subclass(self):
        """Verify BedrockPromptProvider is a PromptProvider subclass."""
        assert issubclass(BedrockPromptProvider, PromptProvider)
    
    def test_stores_config_reference(self):
        """Verify provider stores reference to config object."""
        config = BedrockPromptProviderConfig(
            system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/sys",
            user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/user"
        )
        provider = BedrockPromptProvider(config)
        
        assert hasattr(provider, 'config')
        assert provider.config is config


class TestBedrockPromptProviderLoadPrompt:
    """Tests for _load_prompt private method."""
    
    def test_load_prompt_with_valid_arn(self):
        """Verify _load_prompt loads prompt from Bedrock."""
        mock_bedrock = Mock()
        mock_bedrock.get_prompt.return_value = {
            'variants': [{
                'templateConfiguration': {
                    'text': {
                        'text': 'Test prompt content'
                    }
                }
            }]
        }
        
        config = BedrockPromptProviderConfig(
            system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/test",
            user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/user"
        )
        
        # Patch the _get_or_create_client method to return our mock
        with patch.object(config, '_get_or_create_client', return_value=mock_bedrock):
            provider = BedrockPromptProvider(config)
            result = provider._load_prompt("arn:aws:bedrock:us-east-1:123456789012:prompt/test")
            
            assert result == "Test prompt content"
            mock_bedrock.get_prompt.assert_called_once_with(
                promptIdentifier="arn:aws:bedrock:us-east-1:123456789012:prompt/test"
            )
    





class TestBedrockPromptProviderGetSystemPrompt:
    """Tests for get_system_prompt method."""
    


class TestBedrockPromptProviderGetUserPrompt:
    """Tests for get_user_prompt method."""
    





class TestBedrockPromptProviderIntegration:
    """Integration tests for BedrockPromptProvider."""
    

