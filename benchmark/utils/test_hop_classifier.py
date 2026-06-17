# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Property-based tests for the hop_classifier module.
"""

from hypothesis import given, settings
from hypothesis.strategies import text, dictionaries, one_of, integers, none, floats, just

from benchmark.utils.hop_classifier import (
    classify_hop,
    VALID_CLASSIFICATIONS,
)


class TestHopClassificationOutputValidityProperty:
    """
    Hop classification output validity

    For any question string, the hop classifier SHALL return exactly one of
    {'single-hop', 'multi-hop', 'unknown'}, and the hop_classification field
    in the JSONL output SHALL contain that value.
    """

    @settings(max_examples=100)
    @given(question=text())
    def test_classify_hop_returns_valid_classification_for_any_string(self, question):
        """
        For each generated random question string, call classify_hop(question)
        and verify the result is exactly one of {'single-hop', 'multi-hop', 'unknown'}.
        The function should never raise an exception for any input string.
        """
        result = classify_hop(question)

        assert result in VALID_CLASSIFICATIONS, (
            f"classify_hop returned '{result}' for question '{question!r}', "
            f"but expected one of {VALID_CLASSIFICATIONS}"
        )

    @settings(max_examples=100)
    @given(
        question=text(),
        metadata=dictionaries(
            keys=text(min_size=1, max_size=20),
            values=one_of(
                text(max_size=50),
                integers(min_value=-10, max_value=100),
                floats(allow_nan=False, allow_infinity=False, min_value=-10, max_value=100),
                none(),
            ),
            max_size=5,
        ),
    )
    def test_classify_hop_returns_valid_classification_with_metadata(self, question, metadata):
        """
        For each generated random question string with random metadata dict,
        call classify_hop(question, dataset_metadata=metadata) and verify the
        result is exactly one of {'single-hop', 'multi-hop', 'unknown'}.
        The function should never raise an exception for any input.
        """
        result = classify_hop(question, dataset_metadata=metadata)

        assert result in VALID_CLASSIFICATIONS, (
            f"classify_hop returned '{result}' for question '{question!r}' "
            f"with metadata {metadata!r}, "
            f"but expected one of {VALID_CLASSIFICATIONS}"
        )

    @settings(max_examples=100)
    @given(question=text())
    def test_classify_hop_with_none_metadata_returns_valid_classification(self, question):
        """
        Verify that passing None as dataset_metadata (the default) still
        produces a valid classification for any question string.
        """
        result = classify_hop(question, dataset_metadata=None)

        assert result in VALID_CLASSIFICATIONS, (
            f"classify_hop returned '{result}' for question '{question!r}' "
            f"with metadata=None, but expected one of {VALID_CLASSIFICATIONS}"
        )
