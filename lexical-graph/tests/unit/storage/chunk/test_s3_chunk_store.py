# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for S3ChunkStore.

Mocks the boto3 client the way test_s3_based_docs.py does - moto and
localstack are not dependencies of this project.
"""

import pytest
from botocore.exceptions import ClientError
from unittest.mock import MagicMock, Mock, patch

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import S3ChunkStore

BUCKET = 'test-bucket'
PREFIX = 'chunks'


def _client_error(code, operation='GetObject'):
    return ClientError({'Error': {'Code': code, 'Message': code}}, operation)


def _body(text):
    """Stand in for the StreamingBody a real get_object returns."""
    stream = MagicMock()
    stream.read.return_value = text.encode('UTF-8')
    return {'Body': stream}


@pytest.fixture
def s3_client():
    client = MagicMock()
    with patch('graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store.GraphRAGConfig') as config:
        config.s3 = client
        config.extraction_num_threads_per_worker = 4
        yield client


def _store(**kwargs):
    return S3ChunkStore(bucket_name=BUCKET, prefix=PREFIX, **kwargs)


class TestS3ChunkStoreKeys:

    def test_key_is_derived_from_chunk_id(self, s3_client):
        store = _store()

        store.put('chunk-1', 'hello')

        assert s3_client.put_object.call_args.kwargs['Key'] == 'chunks/chunk-1.txt'

    def test_key_without_prefix_is_the_chunk_id_alone(self, s3_client):
        store = S3ChunkStore(bucket_name=BUCKET)

        store.put('chunk-1', 'hello')

        assert s3_client.put_object.call_args.kwargs['Key'] == 'chunk-1.txt'


class TestS3ChunkStoreRoundTrip:

    def test_put_then_get_returns_the_text(self, s3_client):
        store = _store()
        s3_client.get_object.return_value = _body('the quick brown fox')

        store.put('chunk-1', 'the quick brown fox')

        assert store.get('chunk-1') == 'the quick brown fox'
        assert s3_client.put_object.call_args.kwargs['Bucket'] == BUCKET
        assert s3_client.put_object.call_args.kwargs['Body'] == b'the quick brown fox'

    def test_get_batch_returns_text_keyed_by_chunk_id(self, s3_client):
        store = _store()
        s3_client.get_object.side_effect = lambda Bucket, Key: _body(f'text for {Key}')

        result = store.get_batch(['c1', 'c2'])

        assert result == {
            'c1': 'text for chunks/c1.txt',
            'c2': 'text for chunks/c2.txt',
        }

    def test_get_batch_of_nothing_does_not_call_s3(self, s3_client):
        store = _store()

        assert store.get_batch([]) == {}
        s3_client.get_object.assert_not_called()

    def test_large_payload_round_trips(self, s3_client):
        store = _store()
        text = 'x' * (5 * 1024 * 1024)
        s3_client.get_object.return_value = _body(text)

        store.put('big', text)

        assert store.get('big') == text


class TestS3ChunkStoreMisses:

    def test_missing_key_is_omitted_rather_than_raising(self, s3_client):
        store = _store()
        s3_client.get_object.side_effect = _client_error('NoSuchKey')

        assert store.get_batch(['missing']) == {}

    def test_404_status_code_is_also_treated_as_a_miss(self, s3_client):
        # Some S3-compatible endpoints return 404 without the NoSuchKey code.
        store = _store()
        s3_client.get_object.side_effect = _client_error('404')

        assert store.get_batch(['missing']) == {}

    def test_get_returns_none_for_a_missing_chunk(self, s3_client):
        store = _store()
        s3_client.get_object.side_effect = _client_error('NoSuchKey')

        assert store.get('missing') is None


class TestS3ChunkStoreDualRead:

    def test_missing_chunk_falls_back_to_the_in_graph_store(self, s3_client):
        fallback = Mock(spec=ChunkStore)
        fallback.get_batch.return_value = {'pre-migration': 'text held in the graph'}
        store = _store(fallback=fallback)
        s3_client.get_object.side_effect = _client_error('NoSuchKey')

        result = store.get_batch(['pre-migration'])

        assert result == {'pre-migration': 'text held in the graph'}
        fallback.get_batch.assert_called_once_with(['pre-migration'])

    def test_fallback_is_only_asked_for_the_chunks_s3_did_not_have(self, s3_client):
        fallback = Mock(spec=ChunkStore)
        fallback.get_batch.return_value = {'old': 'from graph'}
        store = _store(fallback=fallback)

        def get_object(Bucket, Key):
            if Key == 'chunks/new.txt':
                return _body('from s3')
            raise _client_error('NoSuchKey')

        s3_client.get_object.side_effect = get_object

        result = store.get_batch(['new', 'old'])

        assert result == {'new': 'from s3', 'old': 'from graph'}
        fallback.get_batch.assert_called_once_with(['old'])

    def test_fallback_is_not_consulted_when_s3_has_every_chunk(self, s3_client):
        fallback = Mock(spec=ChunkStore)
        store = _store(fallback=fallback)
        s3_client.get_object.return_value = _body('from s3')

        store.get_batch(['c1'])

        fallback.get_batch.assert_not_called()

    def test_empty_chunk_text_is_a_hit_and_does_not_reach_the_fallback(self, s3_client):
        # '' is falsy: a truthiness check here would send an empty chunk to the
        # fallback and read back stale in-graph text instead.
        fallback = Mock(spec=ChunkStore)
        store = _store(fallback=fallback)
        s3_client.get_object.return_value = _body('')

        assert store.get_batch(['empty']) == {'empty': ''}
        fallback.get_batch.assert_not_called()

    def test_missing_chunk_with_no_fallback_is_simply_absent(self, s3_client):
        store = _store()
        s3_client.get_object.side_effect = _client_error('NoSuchKey')

        assert store.get_batch(['missing']) == {}


class TestS3ChunkStoreErrorPropagation:

    def test_expired_credentials_propagate(self, s3_client):
        store = _store()
        s3_client.get_object.side_effect = _client_error('ExpiredToken')

        with pytest.raises(ClientError):
            store.get_batch(['c1'])

    def test_throttling_propagates_once_botocore_retries_are_exhausted(self, s3_client):
        store = _store()
        s3_client.get_object.side_effect = _client_error('SlowDown')

        with pytest.raises(ClientError):
            store.get_batch(['c1'])

    def test_access_denied_is_not_swallowed_by_the_fallback(self, s3_client):
        fallback = Mock(spec=ChunkStore)
        store = _store(fallback=fallback)
        s3_client.get_object.side_effect = _client_error('AccessDenied')

        with pytest.raises(ClientError):
            store.get_batch(['c1'])

        fallback.get_batch.assert_not_called()

    def test_put_errors_propagate(self, s3_client):
        store = _store()
        s3_client.put_object.side_effect = _client_error('AccessDenied', 'PutObject')

        with pytest.raises(ClientError):
            store.put('c1', 'text')


class TestS3ChunkStoreEncryption:

    def test_put_uses_aes256_by_default(self, s3_client):
        store = _store()

        store.put('c1', 'text')

        assert s3_client.put_object.call_args.kwargs['ServerSideEncryption'] == 'AES256'
        assert 'SSEKMSKeyId' not in s3_client.put_object.call_args.kwargs

    def test_put_uses_kms_when_a_key_is_configured(self, s3_client):
        key_arn = 'arn:aws:kms:us-east-1:123456789012:key/12345678'
        store = _store(kms_key_arn=key_arn)

        store.put('c1', 'text')

        kwargs = s3_client.put_object.call_args.kwargs
        assert kwargs['ServerSideEncryption'] == 'aws:kms'
        assert kwargs['SSEKMSKeyId'] == key_arn
