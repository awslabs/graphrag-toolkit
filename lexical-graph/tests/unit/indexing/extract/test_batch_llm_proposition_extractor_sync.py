# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.batch_llm_proposition_extractor_sync import BatchLLMPropositionExtractorSync


class TestBatchLLMPropositionExtractorSyncInitialization:
    """Tests for BatchLLMPropositionExtractorSync initialization."""
    
    def test_class_name(self):
        """Verify class_name returns correct name."""
        assert BatchLLMPropositionExtractorSync.class_name() == "BatchLLMPropositionExtractorSync"
    
class TestBatchLLMPropositionExtractorSyncCall:
    """Tests for __call__ method."""
    
class TestBatchLLMPropositionExtractorSyncBatchConfig:
    """Tests for batch configuration."""
    