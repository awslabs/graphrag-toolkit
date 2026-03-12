# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.batch_topic_extractor_sync import BatchTopicExtractorSync


class TestBatchTopicExtractorSyncInitialization:
    """Tests for BatchTopicExtractorSync initialization."""
    
    def test_class_name(self):
        """Verify class_name returns correct name."""
        assert BatchTopicExtractorSync.class_name() == "BatchTopicExtractorSync"
    
class TestBatchTopicExtractorSyncCall:
    """Tests for __call__ method."""
    
class TestBatchTopicExtractorSyncBatchConfig:
    """Tests for batch configuration."""
    