# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import pytest
from unittest.mock import Mock, patch
from pathlib import Path
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import (
    Checkpoint,
    CheckpointFilter,
    CheckpointWriter,
    DoNotCheckpoint,
)
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY


class TestDoNotCheckpoint:
    """Tests for DoNotCheckpoint marker."""

    def test_marker_instantiation(self):
        """Verify DoNotCheckpoint can be instantiated."""
        marker = DoNotCheckpoint()
        assert marker is not None

    def test_is_base_of_checkpoint_filter(self):
        """Verify CheckpointFilter inherits from DoNotCheckpoint."""
        assert issubclass(CheckpointFilter, DoNotCheckpoint)


class TestCheckpointFilter:
    """Tests for CheckpointFilter functionality."""

    def _make_filter(self, checkpoint_dir='/tmp/test_cp'):
        from llama_index.core.schema import TransformComponent
        inner = Mock(spec=TransformComponent)
        inner.__call__ = Mock(return_value=[])
        tenant = TenantId()
        return CheckpointFilter(
            checkpoint_name='test',
            checkpoint_dir=checkpoint_dir,
            inner=inner,
            tenant_id=tenant,
        )

    def test_checkpoint_does_not_exist_returns_true(self, tmp_path):
        """Verify returns True when checkpoint file is absent."""
        f = self._make_filter(checkpoint_dir=str(tmp_path))
        assert f.checkpoint_does_not_exist('node_123') is True

    def test_checkpoint_does_not_exist_returns_false(self, tmp_path):
        """Verify returns False when checkpoint file exists."""
        (tmp_path / 'node_123').touch()
        f = self._make_filter(checkpoint_dir=str(tmp_path))
        assert f.checkpoint_does_not_exist('node_123') is False

    def test_call_filters_checkpointed_nodes(self, tmp_path):
        """Verify __call__ filters out already-checkpointed nodes."""
        (tmp_path / 'n1').touch()  # n1 is checkpointed

        from llama_index.core.schema import TransformComponent
        inner = Mock(spec=TransformComponent)
        inner.__call__ = Mock(side_effect=lambda nodes, **kw: nodes)
        tenant = TenantId()
        f = CheckpointFilter(
            checkpoint_name='test',
            checkpoint_dir=str(tmp_path),
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


class TestCheckpointWriter:
    """Tests for CheckpointWriter functionality."""

    def test_touch_creates_file(self, tmp_path):
        """Verify touch creates a file at the given path."""
        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        inner = Mock(spec=NodeHandler)
        writer = CheckpointWriter(
            checkpoint_name='test',
            checkpoint_dir=str(tmp_path),
            inner=inner,
        )
        target = str(tmp_path / 'new_file')
        writer.touch(target)
        assert os.path.exists(target)

    def test_accept_yields_from_inner(self, tmp_path):
        """Verify accept yields nodes from inner handler."""
        node = Mock()
        node.node_id = 'n1'
        node.metadata = {INDEX_KEY: {'index': 'chunk', 'key': 'abc'}}

        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        inner = Mock(spec=NodeHandler)
        inner.accept = Mock(return_value=iter([node]))

        writer = CheckpointWriter(
            checkpoint_name='test',
            checkpoint_dir=str(tmp_path),
            inner=inner,
        )
        results = list(writer.accept([node]))
        assert len(results) == 1
        assert results[0].node_id == 'n1'

    def test_accept_creates_checkpoint_for_non_index_nodes(self, tmp_path):
        """Verify accept creates checkpoint file for nodes without INDEX_KEY."""
        node = Mock()
        node.node_id = 'n_checkpointable'
        node.metadata = {}

        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        inner = Mock(spec=NodeHandler)
        inner.accept = Mock(return_value=iter([node]))

        writer = CheckpointWriter(
            checkpoint_name='test',
            checkpoint_dir=str(tmp_path),
            inner=inner,
        )
        list(writer.accept([node]))
        assert os.path.exists(str(tmp_path / 'n_checkpointable'))


class TestCheckpoint:
    """Tests for Checkpoint class."""

    @patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.os.makedirs')
    @patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.os.path.exists', return_value=False)
    def test_initialization_creates_directory(self, mock_exists, mock_makedirs):
        """Verify Checkpoint creates output directory on init."""
        with patch('graphrag_toolkit.lexical_graph.config.GraphRAGConfig') as mock_config:
            mock_config.local_output_dir = '/tmp/output'
            cp = Checkpoint(checkpoint_name='my_cp', output_dir='/tmp/output')
            mock_makedirs.assert_called_once()
            assert cp.checkpoint_name == 'my_cp'

    @patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.os.makedirs')
    @patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.os.path.exists', return_value=True)
    def test_initialization_skips_existing_directory(self, mock_exists, mock_makedirs):
        """Verify Checkpoint does not recreate existing directory."""
        cp = Checkpoint(checkpoint_name='my_cp', output_dir='/tmp/output')
        mock_makedirs.assert_not_called()

    def test_add_filter_wraps_transform_component(self):
        """Verify add_filter wraps a TransformComponent when enabled."""
        from llama_index.core.schema import TransformComponent
        cp = Checkpoint(checkpoint_name='test', output_dir='/tmp/test', enabled=True)
        inner = Mock(spec=TransformComponent)
        tenant = TenantId()
        result = cp.add_filter(inner, tenant)
        assert isinstance(result, CheckpointFilter)

    def test_add_filter_skips_do_not_checkpoint(self):
        """Verify add_filter does not wrap DoNotCheckpoint instances."""
        from llama_index.core.schema import TransformComponent
        cp = Checkpoint(checkpoint_name='test', output_dir='/tmp/test', enabled=True)
        inner = Mock(spec=[TransformComponent, DoNotCheckpoint])
        # Make isinstance checks work
        inner.__class__ = type('FakeDoNotCheckpoint', (TransformComponent, DoNotCheckpoint), {})
        tenant = TenantId()
        result = cp.add_filter(inner, tenant)
        # Should return the original object, not wrapped
        assert not isinstance(result, CheckpointFilter)

    def test_add_filter_disabled(self):
        """Verify add_filter returns original when disabled."""
        from llama_index.core.schema import TransformComponent
        cp = Checkpoint(checkpoint_name='test', output_dir='/tmp/test', enabled=False)
        inner = Mock(spec=TransformComponent)
        tenant = TenantId()
        result = cp.add_filter(inner, tenant)
        assert result is inner

    def test_add_writer_wraps_node_handler(self):
        """Verify add_writer wraps a NodeHandler when enabled."""
        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        cp = Checkpoint(checkpoint_name='test', output_dir='/tmp/test', enabled=True)
        inner = Mock(spec=NodeHandler)
        result = cp.add_writer(inner)
        assert isinstance(result, CheckpointWriter)

    def test_add_writer_disabled(self):
        """Verify add_writer returns original when disabled."""
        from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
        cp = Checkpoint(checkpoint_name='test', output_dir='/tmp/test', enabled=False)
        inner = Mock(spec=NodeHandler)
        result = cp.add_writer(inner)
        assert result is inner
