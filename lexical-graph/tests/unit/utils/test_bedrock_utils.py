# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for utils/bedrock_utils."""

import io
import json
import pickle
from unittest.mock import MagicMock, patch

import pytest

from graphrag_toolkit.lexical_graph.utils import bedrock_utils
from graphrag_toolkit.lexical_graph.utils.bedrock_utils import (
    Nova2MultimodalEmbedding,
    _create_retry_decorator,
)


def _embedder(client=None, **kwargs):
    e = Nova2MultimodalEmbedding(
        model_name='amazon.nova-2-multimodal-embeddings-v1:0', **kwargs,
    )
    if client is not None:
        e._client = client
    return e


def _stub_response(embedding):
    body = io.BytesIO(json.dumps({'embeddings': [{'embedding': embedding}]}).encode())
    return {'body': body}


class TestBuildRequestBody:
    def test_includes_default_params(self):
        body = _embedder()._build_request_body('hello')
        assert body['taskType'] == 'SINGLE_EMBEDDING'
        assert body['singleEmbeddingParams']['embeddingDimension'] == 3072
        assert body['singleEmbeddingParams']['embeddingPurpose'] == 'TEXT_RETRIEVAL'
        assert body['singleEmbeddingParams']['text']['truncationMode'] == 'END'
        assert body['singleEmbeddingParams']['text']['value'] == 'hello'

    def test_respects_custom_dimensions_and_purpose(self):
        body = _embedder(
            embed_dimensions=1024, embed_purpose='CLASSIFICATION', truncation_mode='NONE',
        )._build_request_body('x')
        assert body['singleEmbeddingParams']['embeddingDimension'] == 1024
        assert body['singleEmbeddingParams']['embeddingPurpose'] == 'CLASSIFICATION'
        assert body['singleEmbeddingParams']['text']['truncationMode'] == 'NONE'


class TestIsRetryableError:
    @pytest.mark.parametrize(
        'name',
        ['ThrottlingException', 'ServiceUnavailableException', 'InternalServerException',
         'ServiceException', 'ModelErrorException'],
    )
    def test_named_retryable_exceptions(self, name):
        err = type(name, (Exception,), {})('boom')
        assert _embedder()._is_retryable_error(err) is True

    @pytest.mark.parametrize(
        'msg', ['throttling, try later', 'Service unavailable', 'internal server error',
                'try your request again later', 'unexpected error during processing'],
    )
    def test_message_matches_transient_keywords(self, msg):
        assert _embedder()._is_retryable_error(RuntimeError(msg)) is True

    def test_unrelated_error_not_retryable(self):
        assert _embedder()._is_retryable_error(ValueError('bad input')) is False


class TestGetEmbedding:
    def test_empty_text_raises_value_error(self):
        with pytest.raises(ValueError, match='empty or whitespace'):
            _embedder()._get_embedding('')

    def test_whitespace_only_text_raises_value_error(self):
        with pytest.raises(ValueError, match='empty or whitespace'):
            _embedder()._get_embedding('   \n\t')

    def test_returns_first_embedding_from_response(self):
        client = MagicMock()
        client.invoke_model.return_value = _stub_response([0.1, 0.2, 0.3])
        result = _embedder(client=client)._get_embedding('hi')
        assert result == [0.1, 0.2, 0.3]

    def test_empty_embeddings_array_returns_empty_list(self):
        client = MagicMock()
        body = io.BytesIO(json.dumps({'embeddings': []}).encode())
        client.invoke_model.return_value = {'body': body}
        result = _embedder(client=client)._get_embedding('hi')
        assert result == []

    def test_non_retryable_error_propagates(self):
        client = MagicMock()
        client.invoke_model.side_effect = ValueError('bad request')
        with pytest.raises(ValueError):
            _embedder(client=client)._get_embedding('hi')

    def test_retryable_error_then_success(self):
        client = MagicMock()
        client.invoke_model.side_effect = [
            RuntimeError('ThrottlingException - slow down'),
            _stub_response([1.0]),
        ]
        with patch.object(bedrock_utils, 'time') as mock_time:
            mock_time.sleep = MagicMock()
            result = _embedder(client=client)._get_embedding('hi')
        assert result == [1.0]
        assert client.invoke_model.call_count == 2

    def test_exhausted_retries_reraises_last_error(self):
        client = MagicMock()
        client.invoke_model.side_effect = RuntimeError('ThrottlingException')
        with patch.object(bedrock_utils, 'time') as mock_time:
            mock_time.sleep = MagicMock()
            with pytest.raises(RuntimeError, match='ThrottlingException'):
                _embedder(client=client)._get_embedding('hi')
        assert client.invoke_model.call_count == bedrock_utils.MAX_RETRIES


class TestEmbeddingApi:
    def test_embed_text_delegates(self):
        client = MagicMock()
        client.invoke_model.return_value = _stub_response([0.5])
        result = _embedder(client=client).embed_text('q')
        assert result == [0.5]

    def test_embed_texts_delegates(self):
        client = MagicMock()
        client.invoke_model.return_value = _stub_response([0.7])
        result = _embedder(client=client).embed_texts(['q'])
        assert result == [[0.7]]


class TestClassName:
    def test_dimensions_property(self):
        e = _embedder()
        assert e.dimensions == 3072


class TestPickleSupport:
    def test_client_excluded_from_pickle(self):
        e = _embedder(client=None)
        state = e.__dict__
        # _client should be None when not initialized
        assert state.get('_client') is None

    def test_unpickle_resets_client(self):
        e = _embedder(client=None)
        restored = pickle.loads(pickle.dumps(e))
        assert restored._client is None


class TestCreateRetryDecorator:
    def test_returns_callable_with_attached_retry(self):
        class _ClientExceptions:
            ThrottlingException = type('ThrottlingException', (Exception,), {})
            InternalServerException = type('InternalServerException', (Exception,), {})
            ServiceUnavailableException = type('ServiceUnavailableException', (Exception,), {})
            ModelTimeoutException = type('ModelTimeoutException', (Exception,), {})
            ModelErrorException = type('ModelErrorException', (Exception,), {})

        client = MagicMock()
        client.exceptions = _ClientExceptions()

        decorator = _create_retry_decorator(client, max_retries=2)
        assert callable(decorator)
