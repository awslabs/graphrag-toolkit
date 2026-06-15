# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Anti-corruption layer for optional LlamaIndex integration."""

from graphrag_toolkit.lexical_graph.indexing.compat.llama_index_adapter import (
    convert_llama_node,
    normalize_relationship_keys,
)

__all__ = ["convert_llama_node", "normalize_relationship_keys"]
