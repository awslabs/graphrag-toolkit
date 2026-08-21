# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Checks that the benchmark's environment variables reach the objects that use them.

test_benchmark_extract_config.py reads benchmark_extract.py with ast, so it can
prove the code's shape but not its behaviour: replacing the use_batch parameter
with a literal True in the function body leaves every one of those tests
passing. These tests call the two config helpers for real.

Skipped unless graphrag_toolkit is installed, which it is not in the plain
benchmarks/tests environment. Run with the lexical-graph package available.
"""

import pytest

pytest.importorskip('graphrag_toolkit', reason='needs the lexical-graph package')

from graphrag_toolkit.lexical_graph import GraphRAGConfig, IndexingConfig

from benchmarks.scripts.benchmark_extract import (
    apply_extraction_config,
    build_indexing_config,
)

BATCH_ENV = {
    'AWS_REGION_NAME': 'us-west-2',
    'S3_RESULTS_BUCKET': 'a-bucket',
    'S3_RESULTS_PREFIX': 'a/prefix',
    'BATCH_INFERENCE_ROLE': 'arn:aws:iam::123456789012:role/batch',
}

EXTRACTION_VARS = (
    'EXTRACTION_NUM_WORKERS',
    'EXTRACTION_BATCH_SIZE',
    'TEST_EXTRACTION_LLM',
)


@pytest.fixture(autouse=True)
def clean_config(monkeypatch):
    """GraphRAGConfig is process-global, so put back what these tests overwrite."""
    saved = {
        name: getattr(GraphRAGConfig, name)
        for name in ('extraction_batch_size', 'extraction_num_workers')
    }
    for var in EXTRACTION_VARS:
        monkeypatch.delenv(var, raising=False)

    # extraction_llm builds a Bedrock client, which needs a region.
    monkeypatch.setenv('AWS_REGION_NAME', 'us-west-2')

    yield

    for name, value in saved.items():
        setattr(GraphRAGConfig, name, value)


class TestExtractionConfigReachesGraphRAGConfig:

    def test_environment_overrides_the_defaults(self, monkeypatch):
        monkeypatch.setenv('EXTRACTION_NUM_WORKERS', '8')
        monkeypatch.setenv('EXTRACTION_BATCH_SIZE', '999')

        apply_extraction_config()

        assert GraphRAGConfig.extraction_num_workers == 8
        assert GraphRAGConfig.extraction_batch_size == 999

    def test_defaults_when_unset(self):
        apply_extraction_config()

        assert GraphRAGConfig.extraction_num_workers == 2
        assert GraphRAGConfig.extraction_batch_size == 15000

    def test_empty_string_falls_back_rather_than_raising(self, monkeypatch):
        # int('') on the harness's empty string killed every run 27s in.
        monkeypatch.setenv('EXTRACTION_NUM_WORKERS', '')
        monkeypatch.setenv('EXTRACTION_BATCH_SIZE', '')

        apply_extraction_config()

        assert GraphRAGConfig.extraction_num_workers == 2
        assert GraphRAGConfig.extraction_batch_size == 15000


class TestBatchConfigIsBuiltFromTheEnvironment:

    def test_wires_bucket_and_prefix(self, monkeypatch):
        for name, value in BATCH_ENV.items():
            monkeypatch.setenv(name, value)

        config = build_indexing_config('wikihow')

        assert isinstance(config, IndexingConfig)
        assert config.batch_config.bucket_name == 'a-bucket'
        assert config.batch_config.key_prefix == 'a/prefix/batch-extract/wikihow'

    def test_requires_the_batch_variables(self, monkeypatch):
        # Only reachable when use_batch is true, so a missing variable here is
        # a misconfigured batch run rather than a working on-demand one.
        for name in BATCH_ENV:
            monkeypatch.delenv(name, raising=False)

        with pytest.raises(KeyError):
            build_indexing_config('wikihow')
