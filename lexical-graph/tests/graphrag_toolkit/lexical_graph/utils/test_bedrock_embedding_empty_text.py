# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Bug condition exploration tests for empty text embedding validation.

Property 1: Bug Condition - Empty/Whitespace Text Handling
For any text input where the bug condition holds (text.strip() == ""),
the _get_embedding() method SHALL raise a ValueError with a clear message
indicating the text cannot be empty or whitespace-only.

Bug condition: isBugCondition(text) = text.strip() == ""

These tests are expected to FAIL on unfixed code (raises ValidationException
from Bedrock instead of ValueError) and PASS after the fix is implemented.
"""

import pytest
from unittest.mock import MagicMock, patch
from hypothesis import given, strategies as st, settings, HealthCheck


class TestBugConditionExploration:
    """
    Tests that demonstrate the bug condition: empty/whitespace text
    should raise ValueError before calling Bedrock API.
    
    EXPECTED BEHAVIOR (after fix):
    - Empty string "" raises ValueError
    - Whitespace-only strings raise ValueError
    - Error message contains "empty or whitespace-only"
    - No Bedrock API call is made
    """
    
    @pytest.fixture
    def embedding_instance(self):
        """Create a Nova2MultimodalEmbedding instance with mocked client."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import Nova2MultimodalEmbedding
        
        with patch.object(Nova2MultimodalEmbedding, 'client', new_callable=lambda: MagicMock()):
            instance = Nova2MultimodalEmbedding(
                model_name="amazon.nova-embed-multimodal-v2:0",
                embed_dimensions=1024
            )
            yield instance
    
    def test_empty_string_raises_value_error(self, embedding_instance):
        """
        Bug condition: empty string ""
        Expected: Raises ValueError with message about empty text
        Current (unfixed): Raises ValidationException from Bedrock
        """
        with pytest.raises(ValueError) as exc_info:
            embedding_instance._get_embedding("")
        
        assert "empty or whitespace-only" in str(exc_info.value).lower()
    
    def test_whitespace_only_raises_value_error(self, embedding_instance):
        """
        Bug condition: whitespace-only string "   "
        Expected: Raises ValueError with message about empty text
        Current (unfixed): May raise ValidationException or produce bad embedding
        """
        with pytest.raises(ValueError) as exc_info:
            embedding_instance._get_embedding("   ")
        
        assert "empty or whitespace-only" in str(exc_info.value).lower()
    
    def test_newline_tab_only_raises_value_error(self, embedding_instance):
        """
        Bug condition: newline/tab only string "\n\t"
        Expected: Raises ValueError with message about empty text
        """
        with pytest.raises(ValueError) as exc_info:
            embedding_instance._get_embedding("\n\t")
        
        assert "empty or whitespace-only" in str(exc_info.value).lower()
    
    def test_mixed_whitespace_raises_value_error(self, embedding_instance):
        """
        Bug condition: mixed whitespace "  \n  \t  "
        Expected: Raises ValueError with message about empty text
        """
        with pytest.raises(ValueError) as exc_info:
            embedding_instance._get_embedding("  \n  \t  ")
        
        assert "empty or whitespace-only" in str(exc_info.value).lower()
    
    @given(st.text(alphabet=st.sampled_from([' ', '\t', '\n', '\r']), min_size=0, max_size=20))
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_property_all_whitespace_strings_raise_value_error(self, embedding_instance, whitespace_text):
        """
        Property-based test: ALL whitespace-only strings should raise ValueError.
        
        Bug condition: text.strip() == ""
        """
        # Skip empty string as it's tested separately
        if whitespace_text == "":
            whitespace_text = " "  # Ensure at least one whitespace char
            
        with pytest.raises(ValueError) as exc_info:
            embedding_instance._get_embedding(whitespace_text)
        
        assert "empty or whitespace-only" in str(exc_info.value).lower()


class TestPreservationProperty:
    """
    Property 2: Preservation - Valid Text Behavior Unchanged
    
    For any text input where the bug condition does NOT hold (text.strip() != ""),
    the _get_embedding() method SHALL produce the same result as the original method.
    
    These tests should PASS on both unfixed and fixed code.
    """
    
    @pytest.fixture
    def embedding_with_mock_client(self):
        """Create embedding instance with fully mocked Bedrock client."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import Nova2MultimodalEmbedding
        
        mock_client = MagicMock()
        # Mock successful response
        mock_response = MagicMock()
        mock_response.__getitem__ = MagicMock(return_value=MagicMock(
            read=MagicMock(return_value=b'{"embeddings": [{"embeddingType": "TEXT", "embedding": [0.1, 0.2, 0.3]}]}')
        ))
        mock_client.invoke_model.return_value = mock_response
        
        with patch('graphrag_toolkit.lexical_graph.utils.bedrock_utils.Nova2MultimodalEmbedding.client', 
                   new_callable=lambda: mock_client):
            instance = Nova2MultimodalEmbedding(
                model_name="amazon.nova-embed-multimodal-v2:0",
                embed_dimensions=1024
            )
            instance._client = mock_client
            yield instance, mock_client
    
    def test_valid_text_calls_bedrock_api(self, embedding_with_mock_client):
        """
        Preservation: Valid text "hello world" should call Bedrock API.
        """
        instance, mock_client = embedding_with_mock_client
        
        result = instance._get_embedding("hello world")
        
        # Verify API was called
        mock_client.invoke_model.assert_called_once()
        # Verify result is returned
        assert result == [0.1, 0.2, 0.3]
    
    def test_whitespace_padded_text_passes_as_is(self, embedding_with_mock_client):
        """
        Preservation: Text with leading/trailing whitespace "  hello  " 
        should be passed to Bedrock API as-is (no stripping).
        """
        instance, mock_client = embedding_with_mock_client
        
        result = instance._get_embedding("  hello  ")
        
        # Verify API was called with the exact text (not stripped)
        call_args = mock_client.invoke_model.call_args
        import json
        body = json.loads(call_args.kwargs['body'])
        assert body['singleEmbeddingParams']['text']['value'] == "  hello  "
    
    @given(st.text(min_size=1, max_size=100).filter(lambda x: x.strip() != ""))
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_property_valid_text_calls_api_unchanged(self, embedding_with_mock_client, valid_text):
        """
        Property-based test: ALL valid text (text.strip() != "") should be
        passed to Bedrock API without modification.
        """
        instance, mock_client = embedding_with_mock_client
        mock_client.reset_mock()
        
        # Reset mock response for each call
        mock_response = MagicMock()
        mock_response.__getitem__ = MagicMock(return_value=MagicMock(
            read=MagicMock(return_value=b'{"embeddings": [{"embeddingType": "TEXT", "embedding": [0.1, 0.2, 0.3]}]}')
        ))
        mock_client.invoke_model.return_value = mock_response
        
        result = instance._get_embedding(valid_text)
        
        # Verify API was called
        assert mock_client.invoke_model.called
        
        # Verify text was passed unchanged
        import json
        call_args = mock_client.invoke_model.call_args
        body = json.loads(call_args.kwargs['body'])
        assert body['singleEmbeddingParams']['text']['value'] == valid_text
