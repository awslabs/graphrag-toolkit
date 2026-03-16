# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests Covers:
  - TenantId.__str__
  - NodeId.__str__
  - ProcessorArgs.__repr__
"""

import pytest

from graphrag_toolkit.lexical_graph.tenant_id import TenantId, DEFAULT_TENANT_NAME
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.retrieval.processors.processor_args import ProcessorArgs


# ---------------------------------------------------------------------------
# TenantId.__str__
# ---------------------------------------------------------------------------

def test_tenant_id_str_default():
    """Default tenant (value=None) should stringify to DEFAULT_TENANT_NAME."""
    t = TenantId()
    assert str(t) == DEFAULT_TENANT_NAME


def test_tenant_id_str_custom():
    """Custom tenant should stringify to its value."""
    t = TenantId("acme")
    assert str(t) == "acme"


# ---------------------------------------------------------------------------
# NodeId.__str__
# ---------------------------------------------------------------------------

def test_node_id_str():
    """NodeId.__str__ returns the value field."""
    node = NodeId(key="myKey", value="`~id`")
    assert str(node) == "`~id`"


def test_node_id_str_simple():
    """NodeId with simple value."""
    node = NodeId(key="entityId", value="entityId")
    assert str(node) == "entityId"


# ---------------------------------------------------------------------------
# ProcessorArgs.__repr__
# ---------------------------------------------------------------------------

def test_processor_args_repr_contains_defaults():
    """__repr__ returns a string representation of the dict."""
    args = ProcessorArgs()
    r = repr(args)
    assert "expand_entities" in r
    assert "reranker" in r


def test_processor_args_repr_reflects_custom_values():
    """__repr__ reflects custom values passed at construction."""
    args = ProcessorArgs(reranker="cross-encoder", max_statements=50)
    r = repr(args)
    assert "cross-encoder" in r
    assert "50" in r


def test_processor_args_repr_is_string():
    """__repr__ always returns a str."""
    args = ProcessorArgs()
    assert isinstance(repr(args), str)
