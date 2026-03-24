# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_index_factory import parse_s3_vectors_connection_string

def test_parse_bucket_name_only():
    (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string('s3Vectors://mybucket1')

    assert bucket_name == 'mybucket1'
    assert prefix is None
    assert kms_key_arn is None

    (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string('s3Vectors://mybucket2/')

    assert bucket_name == 'mybucket2'
    assert prefix is None
    assert kms_key_arn is None

def test_prefix():

    (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string('s3Vectors://mybucket/prefix1')

    assert bucket_name == 'mybucket'
    assert prefix == 'prefix1'
    assert kms_key_arn is None

    (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string('s3Vectors://mybucket/prefix2/')

    assert bucket_name == 'mybucket'
    assert prefix == 'prefix2'
    assert kms_key_arn is None

def test_kms_key_arn():
    (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string('s3Vectors://mybucket/prefix?kmsKeyArn=key123')

    assert bucket_name == 'mybucket'
    assert prefix == 'prefix'
    assert kms_key_arn == 'key123'

    (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string('s3Vectors://mybucket/prefix/?kmsKeyArn=key123')

    assert bucket_name == 'mybucket'
    assert prefix == 'prefix'
    assert kms_key_arn == 'key123'

    (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string('s3Vectors://mybucket/prefix/?kmsKeyArn=key123&anotherProp=abc')

    assert bucket_name == 'mybucket'
    assert prefix == 'prefix'
    assert kms_key_arn == 'key123'