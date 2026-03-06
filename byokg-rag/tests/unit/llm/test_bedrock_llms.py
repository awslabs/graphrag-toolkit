"""Tests for BedrockGenerator.

This module tests LLM generation functionality with mocked AWS Bedrock calls.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.byokg_rag.llm.bedrock_llms import (
    BedrockGenerator,
    generate_llm_response
)


@pytest.fixture
def mock_bedrock_client():
    """Fixture providing a mock Bedrock client."""
    mock_client = Mock()
    mock_client.converse.return_value = {
        'output': {
            'message': {
                'content': [
                    {'text': 'Mock LLM response'}
                ]
            }
        }
    }
    return mock_client


class TestBedrockGeneratorInitialization:
    """Tests for BedrockGenerator initialization."""
    
    def test_initialization_defaults(self):
        """Verify generator initializes with default parameters."""
        gen = BedrockGenerator()
        
        assert gen.model_name == "anthropic.claude-3-7-sonnet-20250219-v1:0"
        assert gen.region_name == "us-west-2"
        assert gen.max_new_tokens == 4096
        assert gen.max_retries == 10
        assert gen.prefill is False
        assert gen.inference_config is None
        assert gen.reasoning_config is None
    
    def test_initialization_custom_parameters(self):
        """Verify generator accepts custom parameters."""
        custom_inference_config = {"temperature": 0.7}
        custom_reasoning_config = {"mode": "extended"}
        
        gen = BedrockGenerator(
            model_name="custom-model",
            region_name="us-east-1",
            max_tokens=2048,
            max_retries=5,
            prefill=True,
            inference_config=custom_inference_config,
            reasoning_config=custom_reasoning_config
        )
        
        assert gen.model_name == "custom-model"
        assert gen.region_name == "us-east-1"
        assert gen.max_new_tokens == 2048
        assert gen.max_retries == 5
        assert gen.prefill is True
        assert gen.inference_config == custom_inference_config
        assert gen.reasoning_config == custom_reasoning_config


class TestBedrockGeneratorGenerate:
    """Tests for text generation."""
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    def test_generate_success(self, mock_boto3_client, mock_bedrock_client):
        """Verify successful text generation."""
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator()
        result = gen.generate(prompt="Test prompt")
        
        assert result == "Mock LLM response"
        mock_bedrock_client.converse.assert_called_once()
        
        # Verify the call arguments
        call_args = mock_bedrock_client.converse.call_args[1]
        assert call_args['modelId'] == "anthropic.claude-3-7-sonnet-20250219-v1:0"
        assert call_args['messages'][0]['role'] == 'user'
        assert call_args['messages'][0]['content'][0]['text'] == "Test prompt"
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    def test_generate_with_custom_system_prompt(
        self, mock_boto3_client, mock_bedrock_client
    ):
        """Verify custom system prompt is used."""
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator()
        gen.generate(
            prompt="Test prompt",
            system_prompt="Custom system prompt"
        )
        
        call_args = mock_bedrock_client.converse.call_args[1]
        assert call_args['system'][0]['text'] == "Custom system prompt"
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.time.sleep')
    def test_generate_retry_on_throttling(
        self, mock_sleep, mock_boto3_client, mock_bedrock_client
    ):
        """Verify retry logic on throttling errors."""
        # First call raises throttling error, second succeeds
        mock_bedrock_client.converse.side_effect = [
            Exception("Too many requests"),
            {
                'output': {
                    'message': {
                        'content': [{'text': 'Success after retry'}]
                    }
                }
            }
        ]
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator(max_retries=2)
        result = gen.generate(prompt="Test prompt")
        
        assert result == "Success after retry"
        assert mock_bedrock_client.converse.call_count == 2
        mock_sleep.assert_called()
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.time.sleep')
    def test_generate_failure_after_max_retries(
        self, mock_sleep, mock_boto3_client, mock_bedrock_client
    ):
        """Verify exception raised after max retries."""
        mock_bedrock_client.converse.side_effect = Exception("Persistent error")
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator(max_retries=2)
        
        with pytest.raises(Exception, match="Failed due to other reasons"):
            gen.generate(prompt="Test prompt")
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.time.sleep')
    def test_generate_retry_on_timeout(
        self, mock_sleep, mock_boto3_client, mock_bedrock_client
    ):
        """Verify retry logic on timeout errors."""
        # First call raises timeout error, second succeeds
        mock_bedrock_client.converse.side_effect = [
            Exception("Model has timed out"),
            {
                'output': {
                    'message': {
                        'content': [{'text': 'Success after timeout'}]
                    }
                }
            }
        ]
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator(max_retries=2)
        result = gen.generate(prompt="Test prompt")
        
        assert result == "Success after timeout"
        assert mock_bedrock_client.converse.call_count == 2
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    def test_generate_with_custom_inference_config(
        self, mock_boto3_client, mock_bedrock_client
    ):
        """Verify custom inference config is passed to Bedrock."""
        mock_boto3_client.return_value = mock_bedrock_client
        
        custom_config = {"temperature": 0.7, "topP": 0.9}
        gen = BedrockGenerator(inference_config=custom_config)
        gen.generate(prompt="Test prompt")
        
        call_args = mock_bedrock_client.converse.call_args[1]
        assert call_args['inferenceConfig']['temperature'] == 0.7
        assert call_args['inferenceConfig']['topP'] == 0.9
        assert 'maxTokens' in call_args['inferenceConfig']
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    def test_generate_with_reasoning_config(
        self, mock_boto3_client, mock_bedrock_client
    ):
        """Verify reasoning config is passed to Bedrock."""
        mock_boto3_client.return_value = mock_bedrock_client
        
        reasoning_config = {"mode": "extended"}
        gen = BedrockGenerator(reasoning_config=reasoning_config)
        gen.generate(prompt="Test prompt")
        
        call_args = mock_bedrock_client.converse.call_args[1]
        assert 'additionalModelRequestFields' in call_args
        assert call_args['additionalModelRequestFields'] == reasoning_config


class TestGenerateLLMResponse:
    """Tests for the generate_llm_response function."""
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    def test_generate_llm_response_success(self, mock_boto3_client, mock_bedrock_client):
        """Verify generate_llm_response function works correctly."""
        mock_boto3_client.return_value = mock_bedrock_client
        
        result = generate_llm_response(
            region_name="us-west-2",
            model_id="test-model",
            system_prompt="System prompt",
            query="Test query",
            max_tokens=1000,
            max_retries=3
        )
        
        assert result == "Mock LLM response"
        mock_boto3_client.assert_called_once_with("bedrock-runtime", region_name="us-west-2")
    
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.time.sleep')
    def test_generate_llm_response_all_retries_fail(
        self, mock_sleep, mock_boto3_client, mock_bedrock_client
    ):
        """Verify function returns error message after all retries fail."""
        mock_bedrock_client.converse.side_effect = Exception("Persistent error")
        mock_boto3_client.return_value = mock_bedrock_client
        
        result = generate_llm_response(
            region_name="us-west-2",
            model_id="test-model",
            system_prompt="System prompt",
            query="Test query",
            max_tokens=1000,
            max_retries=2
        )
        
        assert "Failed due to other reasons" in result
        # For non-throttling errors, it returns immediately after first attempt
        assert mock_bedrock_client.converse.call_count == 1
