# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared ground for the live tests, which write to a real S3 endpoint."""

import os
import uuid

import pytest

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import MAX_KEYS_PER_DELETE
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import to_batches

S3_TEST_BUCKET = os.environ.get('S3_TEST_BUCKET')


def delete_prefix(bucket_name, prefix):
    """Remove everything under a prefix, a page of keys at a time."""
    pages = GraphRAGConfig.s3.get_paginator('list_objects_v2').paginate(
        Bucket=bucket_name, Prefix=prefix
    )

    keys = [{'Key': obj['Key']} for page in pages for obj in page.get('Contents', [])]

    for batch in to_batches(keys, MAX_KEYS_PER_DELETE):
        refused = GraphRAGConfig.s3.delete_objects(Bucket=bucket_name, Delete={'Objects': batch}).get('Errors', [])
        if refused:
            raise RuntimeError(f'Could not clean up under {prefix}: {refused}')


@pytest.fixture
def key_prefix(request):
    """
    A prefix of this test's own, emptied afterwards.

    Every live test shares one bucket, so the uuid is what keeps two of them
    from reading each other's objects. The module names the prefix, so a run
    left behind by a failed delete says which test wrote it.

    A module that needs something else defines its own key_prefix, which
    shadows this one.
    """
    module = request.module.__name__.rsplit('.', 1)[-1]
    name = module.removeprefix('test_').removesuffix('_live').replace('_', '-')

    run_prefix = f'{name}-tests/{uuid.uuid4()}'
    yield run_prefix

    delete_prefix(S3_TEST_BUCKET, run_prefix)
