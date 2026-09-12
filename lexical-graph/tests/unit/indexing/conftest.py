# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.lexical_graph import GraphRAGConfig


@pytest.fixture
def isolated_source_id_width(monkeypatch):
    """
    The width is env-backed and cached on the config, so a value left in either
    place changes ids for every test that follows.
    """
    monkeypatch.delenv('SOURCE_ID_WIDTH', raising=False)
    GraphRAGConfig.source_id_width = None
    yield
    GraphRAGConfig.source_id_width = None
