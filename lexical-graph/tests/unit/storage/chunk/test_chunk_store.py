# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the ChunkStore abstract base class."""

import pytest

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore


class TestChunkStoreContract:
    """Verify ChunkStore enforces its abstract contract."""

    def test_cannot_instantiate_chunk_store_directly(self):
        with pytest.raises(TypeError):
            ChunkStore()

    def test_subclass_missing_get_cannot_instantiate(self):
        class IncompleteChunkStore(ChunkStore):
            def put(self, chunk_id, text):
                pass

            def get_batch(self, chunk_ids):
                return {}

        with pytest.raises(TypeError):
            IncompleteChunkStore()

    def test_subclass_missing_put_cannot_instantiate(self):
        class IncompleteChunkStore(ChunkStore):
            def get(self, chunk_id):
                return None

            def get_batch(self, chunk_ids):
                return {}

        with pytest.raises(TypeError):
            IncompleteChunkStore()

    def test_subclass_missing_get_batch_cannot_instantiate(self):
        class IncompleteChunkStore(ChunkStore):
            def get(self, chunk_id):
                return None

            def put(self, chunk_id, text):
                pass

        with pytest.raises(TypeError):
            IncompleteChunkStore()

    def test_subclass_implementing_all_methods_can_instantiate(self):
        class CompleteChunkStore(ChunkStore):
            def get(self, chunk_id):
                return 'text'

            def put(self, chunk_id, text):
                pass

            def get_batch(self, chunk_ids):
                return {chunk_id: 'text' for chunk_id in chunk_ids}

        store = CompleteChunkStore()

        assert store.get('chunk-1') == 'text'
        assert store.get_batch(['chunk-1']) == {'chunk-1': 'text'}
