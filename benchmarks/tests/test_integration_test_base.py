# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from benchmarks.scripts.integration_test_base import IntegrationTestBase


class _CompleteTest(IntegrationTestBase):

    @property
    def description(self):
        return 'complete test'

    def _run_test(self, handler, params):
        pass


def test_base_class_cannot_be_instantiated():
    with pytest.raises(TypeError):
        IntegrationTestBase()


def test_subclass_without_run_test_cannot_be_instantiated():

    class MissingRunTest(IntegrationTestBase):

        @property
        def description(self):
            return 'missing _run_test'

    with pytest.raises(TypeError):
        MissingRunTest()


def test_subclass_without_description_cannot_be_instantiated():

    class MissingDescription(IntegrationTestBase):

        def _run_test(self, handler, params):
            pass

    with pytest.raises(TypeError):
        MissingDescription()


def test_complete_subclass_can_be_instantiated():
    assert _CompleteTest().description == 'complete test'
