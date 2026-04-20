# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import os
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock
from llama_index.core.schema import TextNode
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.llms.bedrock_converse import BedrockConverse

from graphrag_toolkit.lexical_graph import BatchJobError
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import (
    get_file_size_mb,
    get_file_sizes_mb,
    split_nodes,
    get_request_body,
    create_inference_inputs_for_messages,
    create_inference_inputs,
    get_parse_output_text_fn,
    BEDROCK_MIN_BATCH_SIZE,
    BEDROCK_MAX_BATCH_SIZE
)


class TestGetFileSize:
    """Tests for get_file_size_mb function."""
    
    def test_get_file_size_mb_returns_float(self):
        """Verify get_file_size_mb returns file size in MB as float."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write('A' * 1024 * 1024)  # 1 MB
            filepath = f.name
        
        try:
            size_mb = get_file_size_mb(filepath)
            assert isinstance(size_mb, float)
            assert size_mb > 0
            assert size_mb <= 1.1  # Allow some overhead
        finally:
            os.unlink(filepath)
    
    def test_get_file_size_mb_empty_file(self):
        """Verify get_file_size_mb handles empty file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            filepath = f.name
        
        try:
            size_mb = get_file_size_mb(filepath)
            assert size_mb == 0.0
        finally:
            os.unlink(filepath)
    
    def test_get_file_sizes_mb_returns_dict(self):
        """Verify get_file_sizes_mb returns dictionary of file sizes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            file1 = os.path.join(tmpdir, 'file1.txt')
            file2 = os.path.join(tmpdir, 'file2.txt')
            
            with open(file1, 'w') as f:
                f.write('A' * 1024)
            with open(file2, 'w') as f:
                f.write('B' * 2048)
            
            sizes = get_file_sizes_mb(tmpdir)
            
            assert isinstance(sizes, dict)
            assert 'file1.txt' in sizes
            assert 'file2.txt' in sizes
            assert isinstance(sizes['file1.txt'], float)
            assert isinstance(sizes['file2.txt'], float)
            # File sizes should be non-negative
            assert sizes['file1.txt'] >= 0
            assert sizes['file2.txt'] >= 0


class TestSplitNodes:
    """Tests for split_nodes function."""
    
    def test_split_nodes_valid_batch_size(self):
        """Verify split_nodes splits nodes into batches correctly."""
        nodes = [TextNode(text=f"node_{i}", id_=f"id_{i}") for i in range(200)]
        batch_size = 100
        
        batches = split_nodes(nodes, batch_size)
        
        assert len(batches) == 2
        assert len(batches[0]) == 100
        assert len(batches[1]) == 100
    
    def test_split_nodes_batch_size_too_small(self):
        """Verify split_nodes raises error for batch size below minimum."""
        nodes = [TextNode(text=f"node_{i}", id_=f"id_{i}") for i in range(200)]
        batch_size = 50  # Below BEDROCK_MIN_BATCH_SIZE (100)
        
        with pytest.raises(BatchJobError, match="smaller than the minimum"):
            split_nodes(nodes, batch_size)
    
    def test_split_nodes_batch_size_too_large(self):
        """Verify split_nodes raises error for batch size above maximum."""
        nodes = [TextNode(text=f"node_{i}", id_=f"id_{i}") for i in range(200)]
        batch_size = 60000  # Above BEDROCK_MAX_BATCH_SIZE (50000)
        
        with pytest.raises(BatchJobError, match="larger than the maximum"):
            split_nodes(nodes, batch_size)
    
    def test_split_nodes_empty_list(self):
        """Verify split_nodes raises error for empty node list."""
        nodes = []
        batch_size = 100
        
        with pytest.raises(BatchJobError, match="Empty list of records"):
            split_nodes(nodes, batch_size)
    
    def test_split_nodes_too_few_nodes(self):
        """Verify split_nodes raises error when nodes below minimum."""
        nodes = [TextNode(text=f"node_{i}", id_=f"id_{i}") for i in range(50)]
        batch_size = 100
        
        with pytest.raises(BatchJobError, match="fewer records.*than the minimum"):
            split_nodes(nodes, batch_size)
    
    def test_split_nodes_handles_remainder(self):
        """Verify split_nodes handles remainder nodes correctly."""
        # Create 250 nodes, batch size 100
        # Should create 3 batches: 100, 100, 50 (but 50 < min, so merge to last)
        # Actually creates 2 batches: 100, 150
        nodes = [TextNode(text=f"node_{i}", id_=f"id_{i}") for i in range(250)]
        batch_size = 100
        
        batches = split_nodes(nodes, batch_size)
        
        # When remainder would be < min, it gets merged with previous batch
        assert len(batches) == 2
        assert len(batches[0]) == 100
        assert len(batches[1]) == 150


class TestGetRequestBody:
    """Tests for get_request_body function."""
    
    def test_get_request_body_nova_model(self):
        """Verify get_request_body creates correct structure for Nova models."""
        mock_llm = Mock(spec=BedrockConverse)
        mock_llm.model = 'amazon.nova-pro-v1:0'
        
        messages = [
            ChatMessage(role=MessageRole.USER, content="Test message")
        ]
        inference_params = {'max_tokens': 1000, 'temperature': 0.7}
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.messages_to_converse_messages') as mock_convert:
            mock_convert.return_value = ([{'role': 'user', 'content': [{'text': 'Test message'}]}], None)
            
            request_body = get_request_body(mock_llm, messages, inference_params)
            
            assert 'messages' in request_body
            assert 'inferenceConfig' in request_body
            assert request_body['inferenceConfig']['maxTokens'] == 1000
            assert request_body['inferenceConfig']['temperature'] == 0.7
    
    def test_get_request_body_nova_with_system_prompt(self):
        """Verify get_request_body includes system prompt for Nova models."""
        mock_llm = Mock(spec=BedrockConverse)
        mock_llm.model = 'amazon.nova-lite-v1:0'
        
        messages = [
            ChatMessage(role=MessageRole.SYSTEM, content="System prompt"),
            ChatMessage(role=MessageRole.USER, content="User message")
        ]
        inference_params = {'max_tokens': 500, 'temperature': 0.5}
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.messages_to_converse_messages') as mock_convert:
            mock_convert.return_value = ([{'role': 'user', 'content': [{'text': 'User message'}]}], 'System prompt')
            
            request_body = get_request_body(mock_llm, messages, inference_params)
            
            assert 'system' in request_body
            assert request_body['system'] == [{'text': 'System prompt'}]
    
    def test_get_request_body_claude_model(self):
        """Verify get_request_body creates correct structure for Claude models."""
        mock_llm = Mock(spec=BedrockConverse)
        mock_llm.model = 'anthropic.claude-3-sonnet-20240229-v1:0'
        
        messages = [
            ChatMessage(role=MessageRole.USER, content="Test message")
        ]
        inference_params = {'max_tokens': 2000, 'temperature': 0.8}
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.messages_to_anthropic_messages') as mock_convert:
            mock_convert.return_value = ([{'role': 'user', 'content': 'Test message'}], None)
            
            request_body = get_request_body(mock_llm, messages, inference_params)
            
            assert 'anthropic_version' in request_body
            assert 'messages' in request_body
            assert request_body['max_tokens'] == 2000
            assert request_body['temperature'] == 0.8
    
    def test_get_request_body_llama_model(self):
        """Verify get_request_body creates correct structure for Llama models."""
        mock_llm = Mock(spec=BedrockConverse)
        mock_llm.model = 'meta.llama3-70b-instruct-v1:0'
        
        messages = [
            ChatMessage(role=MessageRole.USER, content="Test message")
        ]
        inference_params = {'max_tokens': 1500, 'temperature': 0.6}
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.messages_to_converse_messages') as mock_convert:
            mock_convert.return_value = ([{'role': 'user', 'content': [{'text': 'Test message'}]}], None)
            
            request_body = get_request_body(mock_llm, messages, inference_params)
            
            assert 'messages' in request_body
            assert 'parameters' in request_body
            assert request_body['parameters']['max_new_tokens'] == 1500
            assert request_body['parameters']['temperature'] == 0.6
    
    def test_get_request_body_unsupported_model(self):
        """Verify get_request_body raises error for unsupported models."""
        mock_llm = Mock(spec=BedrockConverse)
        mock_llm.model = 'unsupported.model-v1:0'
        
        messages = [ChatMessage(role=MessageRole.USER, content="Test")]
        inference_params = {'max_tokens': 1000, 'temperature': 0.7}
        
        with pytest.raises(ValueError, match="Unrecognized model_id"):
            get_request_body(mock_llm, messages, inference_params)


class TestCreateInferenceInputs:
    """Tests for create_inference_inputs_for_messages and create_inference_inputs."""
    
    def test_create_inference_inputs_for_messages(self):
        """Verify create_inference_inputs_for_messages creates correct structure."""
        mock_llm = Mock(spec=BedrockConverse)
        mock_llm.model = 'amazon.nova-pro-v1:0'
        mock_llm._get_all_kwargs.return_value = {'max_tokens': 1000, 'temperature': 0.7}
        
        nodes = [
            TextNode(text="Node 1", id_="node_1"),
            TextNode(text="Node 2", id_="node_2")
        ]
        messages_batch = [
            [ChatMessage(role=MessageRole.USER, content="Message 1")],
            [ChatMessage(role=MessageRole.USER, content="Message 2")]
        ]
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.get_request_body') as mock_get_body:
            mock_get_body.return_value = {'messages': [], 'inferenceConfig': {}}
            
            inputs = create_inference_inputs_for_messages(mock_llm, nodes, messages_batch)
            
            assert len(inputs) == 2
            assert inputs[0]['recordId'] == 'node_1'
            assert inputs[1]['recordId'] == 'node_2'
            assert 'modelInput' in inputs[0]
            assert 'modelInput' in inputs[1]
    
    def test_create_inference_inputs(self):
        """Verify create_inference_inputs creates correct structure."""
        mock_llm = Mock(spec=BedrockConverse)
        mock_llm._get_all_kwargs.return_value = {'max_tokens': 1000}
        # Use configure_mock to set the method
        mock_llm.configure_mock(**{'completion_to_prompt': lambda x: f"Prompt: {x}"})
        mock_llm._provider = Mock()
        mock_llm._provider.get_request_body.return_value = {'prompt': 'test'}
        
        nodes = [
            TextNode(text="Node 1", id_="node_1"),
            TextNode(text="Node 2", id_="node_2")
        ]
        prompts = ["Prompt 1", "Prompt 2"]
        
        inputs = create_inference_inputs(mock_llm, nodes, prompts)
        
        assert len(inputs) == 2
        assert inputs[0]['recordId'] == 'node_1'
        assert inputs[1]['recordId'] == 'node_2'
        assert 'modelInput' in inputs[0]


class TestGetParseOutputTextFn:
    """Tests for get_parse_output_text_fn function."""
    
    def test_parse_output_nova_model(self):
        """Verify parsing function works for Nova model output."""
        parse_fn = get_parse_output_text_fn('amazon.nova-pro-v1:0')
        
        json_data = {
            'modelOutput': {
                'output': {
                    'message': {
                        'content': [
                            {'text': 'Hello '},
                            {'text': 'World'}
                        ]
                    }
                }
            }
        }
        
        result = parse_fn(json_data)
        assert result == 'Hello World'
    
    def test_parse_output_claude_model(self):
        """Verify parsing function works for Claude model output."""
        parse_fn = get_parse_output_text_fn('anthropic.claude-3-sonnet-20240229-v1:0')
        
        json_data = {
            'modelOutput': {
                'content': [
                    {'text': 'Response '},
                    {'text': 'text'}
                ]
            }
        }
        
        result = parse_fn(json_data)
        assert result == 'Response text'
    
    def test_parse_output_llama_model(self):
        """Verify parsing function works for Llama model output."""
        parse_fn = get_parse_output_text_fn('meta.llama3-70b-instruct-v1:0')
        
        json_data = {
            'generation': 'Generated text response'
        }
        
        result = parse_fn(json_data)
        assert result == 'Generated text response'
    
    def test_parse_output_unsupported_model(self):
        """Verify error raised for unsupported model."""
        with pytest.raises(ValueError, match="Unrecognized model_id"):
            get_parse_output_text_fn('unsupported.model-v1:0')
    
    def test_parse_output_empty_content(self):
        """Verify parsing handles empty content."""
        parse_fn = get_parse_output_text_fn('amazon.nova-lite-v1:0')
        
        json_data = {
            'modelOutput': {
                'output': {
                    'message': {
                        'content': []
                    }
                }
            }
        }
        
        result = parse_fn(json_data)
        assert result == ''
