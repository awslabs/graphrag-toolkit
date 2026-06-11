# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib
import sys
import warnings

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


# All deprecated names that go through __getattr__
_ALL_DEPRECATED_NAMES = [
    'SemanticGuidedRetriever',
    'SemanticGuidedRetrieverType',
    'SemanticGuidedChunkRetriever',
    'SemanticGuidedChunkRetrieverType',
    'KeywordRankingSearch',
    'RerankingBeamGraphSearch',
    'SemanticBeamGraphSearch',
    'StatementCosineSimilaritySearch',
]

# Non-deprecated names that are directly imported in __init__.py
_NON_DEPRECATED_NAMES = [
    'ChunkBasedSearch',
    'ChunkBasedSemanticSearch',
    'EntityBasedSearch',
    'EntityContextSearch',
    'EntityNetworkSearch',
    'TopicBasedSearch',
    'CompositeTraversalBasedRetriever',
    'QueryModeRetriever',
    'ChunkCosineSimilaritySearch',
    'SemanticChunkBeamGraphSearch',
]

_RETRIEVERS_MODULE = 'graphrag_toolkit.lexical_graph.retrieval.retrievers'


def _get_retrievers_module():
    return importlib.import_module(_RETRIEVERS_MODULE)


class TestDeprecatedImportEmitsWarning:
    """Importing deprecated names from the retrievers package emits DeprecationWarning."""

    def test_deprecated_import_emits_warning(self):
        """Basic check: importing a deprecated name emits a DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            from graphrag_toolkit.lexical_graph.retrieval.retrievers import SemanticGuidedRetriever
            deprecation_warnings = [
                x for x in w
                if issubclass(x.category, DeprecationWarning)
                and 'deprecated' in str(x.message).lower()
            ]
            assert len(deprecation_warnings) >= 1
            assert 'SemanticGuidedRetriever' in str(deprecation_warnings[0].message)

    @given(name=st.sampled_from(_ALL_DEPRECATED_NAMES))
    def test_every_deprecated_name_emits_warning(self, name):
        """Every deprecated name emits a DeprecationWarning when accessed."""
        module = _get_retrievers_module()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            getattr(module, name)
            deprecation_warnings = [
                x for x in w
                if issubclass(x.category, DeprecationWarning)
                and name in str(x.message)
            ]
            assert len(deprecation_warnings) == 1, (
                f"Expected exactly 1 DeprecationWarning for '{name}', got {len(deprecation_warnings)}"
            )

    @given(name=st.sampled_from(_ALL_DEPRECATED_NAMES))
    def test_warning_message_includes_recommended_path(self, name):
        """Warning message tells the user where to import from instead."""
        module = _get_retrievers_module()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            getattr(module, name)
            deprecation_warnings = [
                x for x in w
                if issubclass(x.category, DeprecationWarning)
                and name in str(x.message)
            ]
            msg = str(deprecation_warnings[0].message)
            assert 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated' in msg, (
                f"Warning for '{name}' should include the recommended import path, got: {msg}"
            )

    @given(name=st.sampled_from(_ALL_DEPRECATED_NAMES))
    def test_warning_category_is_exactly_deprecation_warning(self, name):
        """Warning category is DeprecationWarning (not a subclass)."""
        module = _get_retrievers_module()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            getattr(module, name)
            deprecation_warnings = [
                x for x in w
                if x.category is DeprecationWarning
                and name in str(x.message)
            ]
            assert len(deprecation_warnings) == 1, (
                f"Expected DeprecationWarning (exact class) for '{name}'"
            )


class TestNonDeprecatedImportsNoWarning:
    """Non-deprecated names do not emit any DeprecationWarning."""

    @given(name=st.sampled_from(_NON_DEPRECATED_NAMES))
    def test_non_deprecated_name_no_warning(self, name):
        """Accessing a non-deprecated name does not emit a DeprecationWarning."""
        module = _get_retrievers_module()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            getattr(module, name)
            deprecation_warnings = [
                x for x in w
                if issubclass(x.category, DeprecationWarning)
                and name in str(x.message)
            ]
            assert len(deprecation_warnings) == 0, (
                f"Non-deprecated name '{name}' should not emit DeprecationWarning"
            )


class TestUnknownAttributeRaises:
    """Accessing unknown names raises an error."""

    def test_unknown_attribute_raises(self):
        """Importing a non-existent name raises ImportError or AttributeError."""
        with pytest.raises((ImportError, AttributeError)):
            from graphrag_toolkit.lexical_graph.retrieval.retrievers import NonExistentClass

    def test_unknown_attribute_via_getattr_raises_attribute_error(self):
        """Using getattr on a non-existent name raises AttributeError."""
        module = _get_retrievers_module()
        with pytest.raises(AttributeError, match="has no attribute"):
            getattr(module, 'TotallyFakeName')

    @given(name=st.text(min_size=1, max_size=30).filter(
        lambda n: n not in _ALL_DEPRECATED_NAMES and n not in _NON_DEPRECATED_NAMES and n.isidentifier() and not n.startswith('_')
    ))
    @settings(max_examples=20)
    def test_random_names_raise_attribute_error(self, name):
        """Random valid identifiers that aren't real names raise AttributeError."""
        module = _get_retrievers_module()
        with pytest.raises(AttributeError):
            getattr(module, name)


class TestWarningStackLevel:
    """The stacklevel in the warning points to the caller, not internal code."""

    @given(name=st.sampled_from(_ALL_DEPRECATED_NAMES))
    def test_warning_points_to_caller_file(self, name):
        """Warning filename should reference this test file, not __init__.py."""
        module = _get_retrievers_module()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            getattr(module, name)
            deprecation_warnings = [
                x for x in w
                if issubclass(x.category, DeprecationWarning)
                and name in str(x.message)
            ]
            assert len(deprecation_warnings) == 1
            warning_filename = deprecation_warnings[0].filename
            # The warning should NOT point to the retrievers __init__.py
            assert '__init__.py' not in warning_filename or 'retrievers/__init__.py' not in warning_filename, (
                f"Warning for '{name}' should point to caller, not retrievers/__init__.py. "
                f"Got: {warning_filename}"
            )


class TestRepeatedAccessEmitsWarning:
    """Each access to a deprecated name emits a fresh warning (no suppression)."""

    @given(name=st.sampled_from(_ALL_DEPRECATED_NAMES))
    def test_repeated_access_emits_warning_each_time(self, name):
        """Accessing the same deprecated name twice emits two warnings."""
        module = _get_retrievers_module()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            getattr(module, name)
            getattr(module, name)
            deprecation_warnings = [
                x for x in w
                if issubclass(x.category, DeprecationWarning)
                and name in str(x.message)
            ]
            assert len(deprecation_warnings) == 2, (
                f"Expected 2 DeprecationWarnings for repeated access to '{name}', "
                f"got {len(deprecation_warnings)}"
            )
