# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for BatchExtractorBase._process_single_batch.

Covers:
  - Successful batch: writes input file, uploads to S3, runs batch job,
    downloads outputs, yields (node_id, text) pairs, deletes dir on success.
  - Exception path: wraps error in BatchJobError.
"""

import os
import json
import pytest
from unittest.mock import MagicMock, patch, mock_open, call
from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph import BatchJobError
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig


def _make_batch_config(**kwargs):
    defaults = dict(
        bucket_name="my-bucket",
        role_arn="arn:aws:iam::123456789012:role/MyRole",
        region="us-east-1",
        max_batch_size=1000,
        max_num_concurrent_batches=2,
        delete_on_success=False,
        key_prefix=None,
    )
    defaults.update(kwargs)
    return BatchConfig(**defaults)


def _make_extractor(batch_config, tmp_path):
    """Create a minimal concrete BatchExtractorBase subclass."""
    from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
    from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache

    class ConcreteExtractor(BatchExtractorBase):
        def _get_json(self, node, llm, inference_parameters):
            return {"recordId": node.node_id, "modelInput": {}}

        def _run_non_batch_extractor(self, nodes):
            return []

        def _update_node(self, node, node_metadata_map):
            return node

    mock_llm = MagicMock()
    mock_llm._get_all_kwargs.return_value = {}
    llm_cache = LLMCache.model_construct(llm=mock_llm, enable_cache=False, model="test-model")

    batch_dir = str(tmp_path / "batch")
    os.makedirs(batch_dir, exist_ok=True)

    extractor = ConcreteExtractor.model_construct(
        batch_config=batch_config,
        llm=llm_cache,
        prompt_template="test",
        source_metadata_field=None,
        batch_inference_dir=batch_dir,
        description="Topic",
        disable_template_rewrite=True,
        node_text_template="{content}",
    )
    return extractor


class TestProcessSingleBatch:

    def _run_batch(self, extractor, nodes, s3_client, bedrock_client, process_output=None, extra_patches=None):
        """Helper to run _process_single_batch with all required mocks."""
        if process_output is None:
            process_output = iter([])

        with patch.object(type(extractor.llm), "model", new_callable=lambda: property(lambda self: "anthropic.claude-v3")):
            with patch("graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base.create_and_run_batch_job"):
                with patch("graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base.download_output_files"):
                    with patch(
                        "graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base.process_batch_output_sync",
                        return_value=process_output
                    ):
                        with patch("graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base.get_file_size_mb", return_value=0.1):
                            with patch("graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base.get_file_sizes_mb", return_value={}):
                                if extra_patches:
                                    # Apply extra patches via context managers
                                    return list(extractor._process_single_batch(0, iter(nodes), s3_client, bedrock_client))
                                return list(extractor._process_single_batch(0, iter(nodes), s3_client, bedrock_client))

    def test_successful_batch_yields_results(self, tmp_path):
        extractor = _make_extractor(_make_batch_config(), tmp_path)
        node = TextNode(text="hello world")
        s3_client = MagicMock()
        bedrock_client = MagicMock()

        results = self._run_batch(
            extractor, [node], s3_client, bedrock_client,
            process_output=iter([(node.node_id, "extracted text")])
        )

        assert len(results) == 1
        assert results[0] == (node.node_id, "extracted text")

    def test_exception_raises_batch_job_error(self, tmp_path):
        extractor = _make_extractor(_make_batch_config(), tmp_path)

        node = TextNode(text="hello world")
        s3_client = MagicMock()
        bedrock_client = MagicMock()
        s3_client.upload_file.side_effect = Exception("S3 upload failed")

        with patch.object(type(extractor.llm), "model", new_callable=lambda: property(lambda self: "anthropic.claude-v3")):
            with patch(
                "graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base.get_file_size_mb",
                return_value=0.1
            ):
                with pytest.raises(BatchJobError, match="S3 upload failed"):
                    list(extractor._process_single_batch(0, iter([node]), s3_client, bedrock_client))

    def test_delete_on_success_removes_dir(self, tmp_path):
        extractor = _make_extractor(_make_batch_config(delete_on_success=True), tmp_path)
        node = TextNode(text="hello world")
        s3_client = MagicMock()
        bedrock_client = MagicMock()

        with patch("shutil.rmtree") as mock_rmtree:
            self._run_batch(extractor, [node], s3_client, bedrock_client)
            mock_rmtree.assert_called_once()

    def test_with_key_prefix_uses_prefix_in_s3_path(self, tmp_path):
        extractor = _make_extractor(_make_batch_config(key_prefix="my/prefix"), tmp_path)
        node = TextNode(text="hello world")
        s3_client = MagicMock()
        bedrock_client = MagicMock()

        self._run_batch(extractor, [node], s3_client, bedrock_client)
        upload_call = s3_client.upload_file.call_args
        s3_key = upload_call[0][2]
        assert s3_key.startswith("my/prefix")
