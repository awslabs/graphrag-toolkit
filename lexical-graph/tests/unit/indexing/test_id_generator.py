# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest


class TestCreateChunkId:
    """Tests for IdGenerator.create_chunk_id method."""

    def test_create_chunk_id_basic(self, default_id_gen):
        """Test basic chunk ID creation."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        chunk_id = default_id_gen.create_chunk_id(source_id, text, metadata)

        assert chunk_id.startswith(source_id + ":")
        assert len(chunk_id) == len(source_id) + 1 + 8  # source_id:8_char_hash

    def test_create_chunk_id_deterministic(self, default_id_gen):
        """Test that same inputs produce same chunk ID."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen.create_chunk_id(source_id, text, metadata)
        id2 = default_id_gen.create_chunk_id(source_id, text, metadata)

        assert id1 == id2

    def test_create_chunk_id_no_boundary_collision(self, default_id_gen):
        """
        Test that different (text, metadata) pairs with same concatenation don't collide.

        This is a regression test for issue #107:
        Previously, ("hello", "world") and ("hell", "oworld") would both hash
        "helloworld" and produce identical IDs. With the delimiter fix, they
        should produce different IDs.
        """
        source_id = "aws::12345678:abcd"

        # These would collide without a delimiter: "hello" + "world" = "helloworld"
        id1 = default_id_gen.create_chunk_id(source_id, "hello", "world")

        # This would also produce "helloworld" without delimiter
        id2 = default_id_gen.create_chunk_id(source_id, "hell", "oworld")

        # With the fix, they should be different
        assert id1 != id2, (
            "Chunk IDs should differ for ('hello', 'world') vs ('hell', 'oworld'). "
            "Boundary collision detected - this means the delimiter fix is not working."
        )

    def test_create_chunk_id_boundary_collision_more_cases(self, default_id_gen):
        """Test additional boundary collision cases."""
        source_id = "aws::12345678:abcd"

        # Test various boundary shift patterns
        test_cases = [
            (("abc", "def"), ("ab", "cdef")),
            (("abc", "def"), ("abcd", "ef")),
            (("", "abcdef"), ("abc", "def")),
            (("abcdef", ""), ("abc", "def")),
            (("a", "bcdef"), ("abcde", "f")),
        ]

        for (text1, meta1), (text2, meta2) in test_cases:
            id1 = default_id_gen.create_chunk_id(source_id, text1, meta1)
            id2 = default_id_gen.create_chunk_id(source_id, text2, meta2)
            assert id1 != id2, (
                f"Boundary collision: ({text1!r}, {meta1!r}) vs ({text2!r}, {meta2!r})"
            )

    def test_create_chunk_id_empty_strings(self, default_id_gen):
        """Test chunk ID creation with empty strings."""
        source_id = "aws::12345678:abcd"

        # Empty text
        id1 = default_id_gen.create_chunk_id(source_id, "", "metadata")
        assert id1.startswith(source_id + ":")

        # Empty metadata
        id2 = default_id_gen.create_chunk_id(source_id, "text", "")
        assert id2.startswith(source_id + ":")

        # Both should be different (delimiter separates them)
        assert id1 != id2

    def test_create_chunk_id_different_source_ids(self, default_id_gen):
        """Test that different source IDs produce different chunk IDs."""
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen.create_chunk_id("source1", text, metadata)
        id2 = default_id_gen.create_chunk_id("source2", text, metadata)

        assert id1 != id2
        assert id1.startswith("source1:")
        assert id2.startswith("source2:")


class TestCreateSourceId:
    """Tests for IdGenerator.create_source_id method."""

    def test_create_source_id_format(self, default_id_gen):
        """Test source ID format."""
        text = "Hello world"
        metadata = "test_metadata"

        source_id = default_id_gen.create_source_id(text, metadata)

        assert source_id.startswith("aws::")
        parts = source_id.split(":")
        assert len(parts) == 4  # "aws", "", "hash1", "hash2"

    def test_create_source_id_deterministic(self, default_id_gen):
        """Test that same inputs produce same source ID."""
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen.create_source_id(text, metadata)
        id2 = default_id_gen.create_source_id(text, metadata)

        assert id1 == id2


class TestTenantIsolation:
    """Tests for tenant isolation in ID generation."""

    def test_chunk_id_tenant_isolation(self, default_id_gen, custom_id_gen):
        """Test that chunk IDs from different tenants can be distinguished via rewrite."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        # Chunk IDs themselves are the same (tenant affects rewrite_id_for_tenant)
        default_chunk_id = default_id_gen.create_chunk_id(source_id, text, metadata)
        custom_chunk_id = custom_id_gen.create_chunk_id(source_id, text, metadata)

        # The raw chunk IDs are the same
        assert default_chunk_id == custom_chunk_id

        # But when rewritten for tenant, they differ
        default_rewritten = default_id_gen.rewrite_id_for_tenant(default_chunk_id)
        custom_rewritten = custom_id_gen.rewrite_id_for_tenant(custom_chunk_id)

        assert default_rewritten != custom_rewritten
