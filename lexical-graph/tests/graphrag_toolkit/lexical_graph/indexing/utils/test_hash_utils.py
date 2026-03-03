"""Tests for get_hash (hash_utils.py).

get_hash is the foundation of all ID generation in this library. Every source ID,
chunk ID, entity ID, topic ID, etc. is ultimately an MD5 hex digest of a formatted
string. These tests verify the properties that the rest of the system depends on:
determinism (same input -> same ID on every run), collision resistance (different
inputs -> different IDs), correct output format (32-char lowercase hex), and safe
handling of edge-case inputs (unicode, empty string).

The known-vector test pins the exact MD5 digest of "hello" so that any future
change to the hashing algorithm (e.g. switching from MD5 or changing the encoding)
will be caught immediately.
"""

import re

import pytest
from graphrag_toolkit.lexical_graph.indexing.utils.hash_utils import get_hash


def test_get_hash_deterministic():
    """Same input always produces the same hash.

    This is the core contract: the graph deduplication logic relies on hashing
    the same content twice and getting the same node ID both times.
    """
    assert get_hash("hello") == get_hash("hello")


def test_get_hash_known_vector():
    """MD5("hello") matches the well-known hex digest.

    Pinning a known-good value catches any silent change to the algorithm or
    encoding without needing to recompute what the "correct" answer should be.
    """
    assert get_hash("hello") == "5d41402abc4b2a76b9719d911017c592"


def test_get_hash_different_inputs():
    """Different inputs produce different hashes.

    If two distinct strings hashed to the same value, distinct graph nodes
    would be collapsed into one — a silent data-loss bug.
    """
    assert get_hash("a") != get_hash("b")


def test_get_hash_unicode():
    """Non-ASCII input is encoded as UTF-8 and hashed without error.

    Entity and topic values can contain accented characters, CJK, etc.
    The function must not raise and must return a valid 32-char hex string.
    """
    result = get_hash("caf\u00e9")
    assert re.match(r"^[a-f0-9]{32}$", result)


def test_get_hash_empty_string():
    """Empty string is a valid input.

    Callers may pass empty metadata strings; the function must not raise
    and must return a stable 32-char hex string (MD5 of b'' is well-defined).
    """
    result = get_hash("")
    assert re.match(r"^[a-f0-9]{32}$", result)


def test_get_hash_hex_format():
    """Output is always a 32-character lowercase hexadecimal string.

    Downstream code slices fixed character positions from this string
    (e.g. [:8] for source IDs, [:4] for metadata suffixes), so the
    length and character set must be exact.
    """
    assert re.match(r"^[a-f0-9]{32}$", get_hash("arbitrary input"))
