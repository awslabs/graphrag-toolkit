# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint import (
    S3Checkpoint,
    S3CheckpointFilter,
    S3CheckpointWriter,
    SAVEPOINT_ROOT_DIR,
)
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import DoNotCheckpoint
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY


def _make_404_error():
    """Create a ClientError that simulates S3 404 Not Found."""
    return ClientError(
        error_response={'Error': {'Code': '404', 'Message': 'Not Found'}},
        operation_name='HeadObject',
    )


def _make_s3_mock():
    """Create a mock S3 client."""
    return MagicMock()


class TestS3Checkpoint:
    """Tests for S3Checkpoint class."""

    def test_initialization_enabled(self):
        """Verify S3Checkpoint initializes with correct attributes."""
        cp = S3Checkpoint(
            checkpoint_name='test-cp',
            bucket_name='my-bucket',
            key_prefix='prefix/path',
            enabled=True,
        )
        assert cp.checkpoint_name == 'test-cp'
        assert cp.bucket_name == 'my-bucket'
        assert cp.key_prefix == 'prefix/path'
        assert cp.enabled is True

    def test_initialization_strips_trailing_slash(self):
        """Verify key_prefix has trailing slash stripped."""
        cp = S3Checkpoint(
            checkpoint_name='test',
            bucket_name='bucket',
            key_prefix='prefix/',
        )
        assert cp.key_prefix == 'prefix'

    def test_initialization_disabled(self):
        """Verify S3Checkpoint can be disabled."""
        cp = S3Checkpoint(
            checkpoint_name='test',
            bucket_name='bucket',
            enabled=False,
        )
        assert cp.enabled is False

    def test_add_filter_wraps_transform_component(self):
        """Verify add_filter wraps a TransformComponent when enabled."""
        from llama_index.core.schema import TransformComponent
        cp = S3Checkpoint(
            checkpoint_name='test',
            bucket_name='bucket',
            key_prefix='pfx',
            enabled=True,
        )
        inner = Mock(spec=TransformComponent)
        tenant = TenantId()
        result = cp.add_filter(inner, tenant)
        assert isinstance(result, S3CheckpointFilter)

    def test_add_filter_skips_do_not_checkpoint(self):
        """Verify add_filter does not wrap DoNotCheckpoint instances."""
        from llama_index.core.schema import TransformComponent

        class FakeDoNotCheckpoint(TransformComponent, DoNotCheckpoint):
            def __call__(self, nodes, **kwargs):
                return nodes

        cp = S3Checkpoint(
            checkpoint_name='test',
            bucket_name='bucket',
            key_prefix='pfx',
            enabled=True,
        )
        inner = FakeDoNotCheckpoint()
        tenant = TenantId()
        result = cp.add_filter(inner, tenant)
        assert result is inner

    def test_add_filter_disabled(self):
        """Verify add_filter returns original when disabled."""
        from llama_index.core.schema import TransformComponent
        cp = S3Checkpoint(
            checkpoint_name='test',
            bucket_name='bucket',
            enabled=False,
        )
        inner = Mock(spec=TransformComponent)
        tenant = TenantId()
        result = cp.add_filter(inner, tenant)
        assert result is inner

    def test_add_writer_wraps_node_handler(self):
        """Verify add_writer wraps a NodeHandler when enabled."""
        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        cp = S3Checkpoint(
            checkpoint_name='test',
            bucket_name='bucket',
            key_prefix='pfx',
            enabled=True,
        )
        inner = Mock(spec=NodeHandler)
        result = cp.add_writer(inner)
        assert isinstance(result, S3CheckpointWriter)

    def test_add_writer_disabled(self):
        """Verify add_writer returns original when disabled."""
        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        cp = S3Checkpoint(
            checkpoint_name='test',
            bucket_name='bucket',
            enabled=False,
        )
        inner = Mock(spec=NodeHandler)
        result = cp.add_writer(inner)
        assert result is inner


class TestS3CheckpointFilter:
    """Tests for S3CheckpointFilter functionality."""

    def _make_filter(self, s3_mock):
        from llama_index.core.schema import TransformComponent
        inner = Mock(spec=TransformComponent)
        inner.__call__ = Mock(side_effect=lambda nodes, **kw: nodes)
        tenant = TenantId()
        f = S3CheckpointFilter(
            checkpoint_name='test',
            bucket_name='my-bucket',
            key_prefix='pfx',
            inner=inner,
            tenant_id=tenant,
        )
        return f

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointFilter._get_s3_client')
    def test_checkpoint_does_not_exist_returns_true_on_404(self, mock_get_client):
        """Verify returns True when S3 HEAD returns 404."""
        s3_mock = _make_s3_mock()
        s3_mock.head_object.side_effect = _make_404_error()
        mock_get_client.return_value = s3_mock

        f = self._make_filter(s3_mock)
        assert f.checkpoint_does_not_exist('node_123') is True

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointFilter._get_s3_client')
    def test_checkpoint_does_not_exist_returns_false_when_exists(self, mock_get_client):
        """Verify returns False when S3 HEAD succeeds (marker exists)."""
        s3_mock = _make_s3_mock()
        s3_mock.head_object.return_value = {}  # Success = object exists
        mock_get_client.return_value = s3_mock

        f = self._make_filter(s3_mock)
        assert f.checkpoint_does_not_exist('node_123') is False

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointFilter._get_s3_client')
    def test_checkpoint_does_not_exist_returns_true_on_unexpected_error(self, mock_get_client):
        """Verify returns True on unexpected errors (safe default: re-process)."""
        s3_mock = _make_s3_mock()
        s3_mock.head_object.side_effect = ClientError(
            error_response={'Error': {'Code': '500', 'Message': 'Internal'}},
            operation_name='HeadObject',
        )
        mock_get_client.return_value = s3_mock

        f = self._make_filter(s3_mock)
        assert f.checkpoint_does_not_exist('node_123') is True

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointFilter._get_s3_client')
    def test_call_filters_checkpointed_nodes(self, mock_get_client):
        """Verify __call__ filters out already-checkpointed nodes."""
        s3_mock = _make_s3_mock()

        # n1 exists (checkpointed), n2 does not
        def head_side_effect(Bucket, Key):
            if 'n1' in Key:
                return {}  # Exists
            raise _make_404_error()

        s3_mock.head_object.side_effect = head_side_effect
        mock_get_client.return_value = s3_mock

        from llama_index.core.schema import TransformComponent
        inner = Mock(spec=TransformComponent)
        inner.__call__ = Mock(side_effect=lambda nodes, **kw: nodes)
        tenant = TenantId()

        f = S3CheckpointFilter(
            checkpoint_name='test',
            bucket_name='my-bucket',
            key_prefix='pfx',
            inner=inner,
            tenant_id=tenant,
        )

        node1 = Mock()
        node1.id_ = 'n1'
        node2 = Mock()
        node2.id_ = 'n2'

        result = f([node1, node2])
        # Only n2 should pass through
        assert len(result) == 1
        assert result[0].id_ == 'n2'

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointFilter._get_s3_client')
    def test_marker_key_format(self, mock_get_client):
        """Verify the S3 key follows expected pattern."""
        s3_mock = _make_s3_mock()
        s3_mock.head_object.side_effect = _make_404_error()
        mock_get_client.return_value = s3_mock

        f = self._make_filter(s3_mock)
        expected = f'pfx/{SAVEPOINT_ROOT_DIR}/test/node_abc'
        assert f._marker_key('node_abc') == expected


class TestS3CheckpointWriter:
    """Tests for S3CheckpointWriter functionality."""

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointWriter._get_s3_client')
    def test_touch_writes_zero_byte_object(self, mock_get_client):
        """Verify touch writes a zero-byte S3 object."""
        s3_mock = _make_s3_mock()
        mock_get_client.return_value = s3_mock

        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        inner = Mock(spec=NodeHandler)
        writer = S3CheckpointWriter(
            checkpoint_name='test',
            bucket_name='my-bucket',
            key_prefix='pfx',
            inner=inner,
        )
        writer.touch('node_xyz')

        s3_mock.put_object.assert_called_once_with(
            Bucket='my-bucket',
            Key=f'pfx/{SAVEPOINT_ROOT_DIR}/test/node_xyz',
            Body=b'',
        )

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointWriter._get_s3_client')
    def test_touch_failure_is_non_fatal(self, mock_get_client):
        """Verify touch does not raise on S3 errors."""
        s3_mock = _make_s3_mock()
        s3_mock.put_object.side_effect = Exception('S3 unavailable')
        mock_get_client.return_value = s3_mock

        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        inner = Mock(spec=NodeHandler)
        writer = S3CheckpointWriter(
            checkpoint_name='test',
            bucket_name='my-bucket',
            key_prefix='pfx',
            inner=inner,
        )
        # Should not raise
        writer.touch('node_xyz')

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointWriter._get_s3_client')
    def test_accept_yields_from_inner_and_writes_markers(self, mock_get_client):
        """Verify accept yields nodes and writes checkpoint markers."""
        s3_mock = _make_s3_mock()
        mock_get_client.return_value = s3_mock

        node = Mock()
        node.node_id = 'n1'
        node.metadata = {}  # No INDEX_KEY = checkpointable

        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        inner = Mock(spec=NodeHandler)
        inner.accept = Mock(return_value=iter([node]))

        writer = S3CheckpointWriter(
            checkpoint_name='test',
            bucket_name='my-bucket',
            key_prefix='pfx',
            inner=inner,
        )
        results = list(writer.accept([node]))
        assert len(results) == 1
        assert results[0].node_id == 'n1'
        s3_mock.put_object.assert_called_once()

    @patch('graphrag_toolkit.lexical_graph.indexing.build.s3_checkpoint.S3CheckpointWriter._get_s3_client')
    def test_accept_skips_marker_for_index_nodes(self, mock_get_client):
        """Verify accept does not write markers for nodes with INDEX_KEY metadata."""
        s3_mock = _make_s3_mock()
        mock_get_client.return_value = s3_mock

        node = Mock()
        node.node_id = 'n1'
        node.metadata = {INDEX_KEY: {'index': 'chunk', 'key': 'abc'}}

        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        inner = Mock(spec=NodeHandler)
        inner.accept = Mock(return_value=iter([node]))

        writer = S3CheckpointWriter(
            checkpoint_name='test',
            bucket_name='my-bucket',
            key_prefix='pfx',
            inner=inner,
        )
        results = list(writer.accept([node]))
        assert len(results) == 1
        s3_mock.put_object.assert_not_called()
