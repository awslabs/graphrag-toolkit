# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Ids become path segments and S3 key segments, so the characters they may carry
are the same question in both places. These cover the one check both sides call.
"""

import pytest

from llama_index.core.schema import Document

from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator
from graphrag_toolkit.lexical_graph.utils.id_validation import validate_id_segment


class TestIdsTheToolkitGenerates:
    """IdGenerator output has to pass, or extraction cannot write anything."""

    @pytest.mark.parametrize('value', [
        'aws::dead:beef',
        'aws::dead:beef:cafe',
        'aws:tenant::dead:beef:cafe',
        'c1',
        'chunk-1_v2.json',
    ])
    def test_a_generated_id_is_accepted(self, value):
        validate_id_segment(value, 'node_id')


class TestIdsThatWouldOpenANewSegment:

    @pytest.mark.parametrize('value', ['a/b', 'a\\b', '/abs', '_markers/x', '../b'])
    def test_an_id_carrying_a_separator_is_rejected(self, value):
        with pytest.raises(ValueError, match='invalid characters'):
            validate_id_segment(value, 'node_id')

    @pytest.mark.parametrize('value', ['.', '..', ' . ', ' .. '])
    def test_an_id_that_names_a_directory_is_rejected(self, value):
        with pytest.raises(ValueError, match='directory'):
            validate_id_segment(value, 'node_id')


class TestIdsAnAllowlistCatchesAndADenylistDoesNot:
    """
    The reason this is an allowlist. Each of these reaches S3 or the filesystem
    as something other than the bytes a separator check was looking for.
    """

    @pytest.mark.parametrize('value', [
        'a%2Fb',
        'a%252Fb',
        'a∕b',
        'a／b',
        'a b',
        'a\x00b',
        'a\nb',
        'a\x7fb',
    ])
    def test_it_is_rejected(self, value):
        with pytest.raises(ValueError, match='invalid characters'):
            validate_id_segment(value, 'node_id')


class TestIdsAUserSupplies:
    """IdRewriter replaces any id that does not already start with 'aws:'."""

    @pytest.fixture
    def rewrite(self):
        rewriter = IdRewriter(id_generator=IdGenerator())
        return lambda value: rewriter([Document(text='some text', id_=value)])[0].id_

    @pytest.mark.parametrize('user_id', [
        'annual report 2024',
        'rapport-été',
        'отчёт/2024',
    ])
    def test_a_user_id_is_rewritten_before_it_is_validated(self, rewrite, user_id):
        validate_id_segment(rewrite(user_id), 'source_id')

    def test_an_id_already_claiming_the_aws_prefix_is_kept_and_rejected(self, rewrite):
        # Generated ids are hex, so this is an id built by hand.
        document_id = rewrite('aws:annual report 2024')

        assert document_id == 'aws:annual report 2024'
        with pytest.raises(ValueError, match='invalid characters'):
            validate_id_segment(document_id, 'source_id')


class TestIdsThatAreNotIds:

    @pytest.mark.parametrize('value', ['', '   ', None])
    def test_an_empty_id_is_rejected(self, value):
        with pytest.raises(ValueError, match='non-empty'):
            validate_id_segment(value, 'node_id')

    def test_the_name_in_the_message_is_the_caller_s(self):
        with pytest.raises(ValueError, match='source_id'):
            validate_id_segment('a/b', 'source_id')
