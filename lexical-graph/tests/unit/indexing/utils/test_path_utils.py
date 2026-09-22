# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the shared id validator used by every file sink."""

import pytest

from graphrag_toolkit.lexical_graph.indexing.utils.path_utils import validate_id


class TestValidateId:
    @pytest.mark.parametrize('value', ['a/b', 'a\\b', '../b', '/abs', 'aws:../../etc/x'])
    def test_a_path_separator_is_rejected(self, value):
        with pytest.raises(ValueError, match='separator'):
            validate_id(value, 'node_id')

    @pytest.mark.parametrize('value', ['a\nb', 'a\tb', 'a\x00b', 'a\x7fb'])
    def test_a_control_character_is_rejected(self, value):
        with pytest.raises(ValueError, match='control character'):
            validate_id(value, 'node_id')

    @pytest.mark.parametrize('value', ['', '   ', None])
    def test_an_empty_id_is_rejected(self, value):
        with pytest.raises(ValueError, match='non-empty'):
            validate_id(value, 'node_id')

    @pytest.mark.parametrize('value', ['.', '..', ' . ', ' .. '])
    def test_an_id_that_names_a_directory_is_rejected(self, value):
        """Neither climbs without a separator, but both resolve to a directory
        and would otherwise fail inside open() with no id named."""
        with pytest.raises(ValueError, match='directory'):
            validate_id(value, 'node_id')

    def test_the_name_appears_in_the_message(self):
        with pytest.raises(ValueError, match='doc_id'):
            validate_id('a/b', 'doc_id')

    @pytest.mark.parametrize('value', [
        'aws::1a2b3c4d:5e6f',                 # source id
        'aws::1a2b3c4d:5e6f:0a1b2c3d',        # chunk id
        'aws:tenant:1a2b3c4d:5e6f',           # source id rewritten for a tenant
        '4f2e1d0c9b8a7654',                   # node id hash
        'simple-source',
    ])
    def test_the_ids_the_pipeline_produces_are_accepted(self, value):
        validate_id(value, 'node_id')
