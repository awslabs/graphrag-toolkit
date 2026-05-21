# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for suggestion-mode ontology-guided extraction.

Covers Tasks 6.1, 6.2, and 6.3 from the ontology-guided-extraction spec:

* **6.1** — building ``ExtractionConfig.from_stages`` with the bridged
  :class:`ExtractionSchema`, asserting schema injection into
  :class:`LLMTopicExtractionStage` carries the richer ontology prompt
  text (Requirement 5.2), and asserting no entity / fact / topic is
  dropped by any feature-owned code path (Requirement 5.3).
* **6.2 (PBT)** — Property **P3**: suggestion mode never filters.
  Hypothesis generates arbitrary :class:`TopicCollection`\\ s; the
  feature's suggestion-mode code path leaves every input bit-identical
  on output (Requirements 5.3, 5.4).
* **6.3** — separability smoke test: with the strict-mode module
  simulated absent via ``sys.modules``, the ``extract`` package still
  imports cleanly, :class:`OntologySchema` remains accessible, and the
  conditional re-export of :class:`OntologyFilterStage` is a clean
  no-op rather than a package-import-time ``ImportError``
  (Requirements 14.3, 14.4, 14.5, NFR-7).

Per Requirement 14.8 this file is kept **separate** from the strict-
mode integration suite (``test_ontology_integration_strict.py``) so a
``pytest -k "not strict"`` selector excludes only the strict-mode
tests without taking this file with it. The module name therefore
does **not** contain the substring ``strict``.

Implementation note on the "integration" scope
----------------------------------------------
Running the full ingestion pipeline
(``LexicalGraphIndex.extract(...)`` → ``LLMTopicExtractionStage`` →
``TopicExtractor`` → live LLM call) in a unit test is expensive and
would require mocking the LLM. The "integration" tests here focus on
stage-level composition and schema injection rather than a full
document-to-graph run, which is sufficient for the three requirements
they cover:

* Config-level composition (``ExtractionConfig.from_stages(...)``)
  binds the bridged :class:`ExtractionSchema` correctly and the
  schema's prompt rendering carries the ontology-specific markers.
* The only feature-owned code path that *could* drop content in a no-
  ``OntologyFilterStage`` pipeline is :class:`SchemaFilter` consuming
  the bridged schema; with ``strict=False`` (set by
  :meth:`OntologySchema.as_extraction_schema`) it is a no-op, proving
  Requirement 5.3.
* Package-import separability can be tested directly via
  ``sys.modules`` manipulation without touching the LLM at all.
"""

import importlib
import sys

import pytest

# Soft-skip when rdflib or hypothesis are absent — same treatment as
# ``test_ontology_filter_stage.py`` so skipped-test reporting is
# consistent across the ontology-guided-extraction suite.
pytest.importorskip("rdflib")
pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, given, settings, strategies as st

from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph import ExtractionConfig
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract import (
    ExtractionSchema,
    LLMPropositionStage,
    LLMTopicExtractionStage,
    OntologySchema,
    SchemaFilterStage,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.schema_filter_stage import (
    SchemaFilter,
)
from graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor import (
    TopicExtractor,
)
from graphrag_toolkit.lexical_graph.indexing.model import (
    Entity,
    Fact,
    Relation,
    Statement,
    Topic,
    TopicCollection,
)


# ---------------------------------------------------------------------------
# Fixtures
#
# The Person / Employee / Manager / Company ontology mirrors the fixture
# used in :mod:`test_ontology_filter_stage` and design §5.1. Redeclared
# here (rather than imported across test files) so this module stays
# self-contained — a future suggestion-mode-only launch should be able
# to keep this file even if the strict-mode test file is excised.
# ---------------------------------------------------------------------------

PERSON_ONTOLOGY_TURTLE = """
@prefix : <https://example.com/kg/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

: a owl:Ontology .

:Person a owl:Class ;
    rdfs:label "Person" .

:Employee a owl:Class ;
    rdfs:subClassOf :Person ;
    rdfs:label "Employee" .

:Manager a owl:Class ;
    rdfs:subClassOf :Employee ;
    rdfs:label "Manager" .

:Company a owl:Class ;
    rdfs:label "Company" .

:worksFor a owl:ObjectProperty ;
    rdfs:label "works for" ;
    rdfs:domain :Employee ;
    rdfs:range :Company .

:age a owl:DatatypeProperty ;
    rdfs:label "age" ;
    rdfs:domain :Person ;
    rdfs:range xsd:integer .
"""


@pytest.fixture
def ontology() -> OntologySchema:
    """Load the fixture ontology once per test.

    A fresh instance per test keeps isolation clean even though
    :class:`OntologySchema` is effectively immutable after load —
    avoids surprising cross-test coupling if the implementation ever
    grows mutable state.
    """
    return OntologySchema.from_turtle_string(PERSON_ONTOLOGY_TURTLE)


def _make_node(tc: TopicCollection) -> TextNode:
    """Wrap a :class:`TopicCollection` in a :class:`TextNode` the way
    :class:`LLMTopicExtractionStage` would emit it.

    Serialising via ``model_dump()`` matches the exact metadata shape
    the downstream filter transforms expect (they round-trip through
    ``TopicCollection(**topics_data)`` → ``tc.model_dump()``).
    """
    node = TextNode(text="test")
    node.metadata[TOPICS_KEY] = tc.model_dump()
    return node


# ---------------------------------------------------------------------------
# Task 6.1 — Suggestion-mode composition and schema injection
# ---------------------------------------------------------------------------


class TestSuggestionModeCompositionAndPromptInjection:
    """Task 6.1 — pipeline composition and ontology-prompt injection.

    Validates: Requirements 5.1, 5.2, 5.3, 5.4, 14.8.
    """

    def test_from_stages_binds_bridged_schema(self, ontology):
        """Requirement 5.1 — ``ExtractionConfig.from_stages(schema=...)``
        accepts the bridged :class:`ExtractionSchema` without any
        further ceremony, and the resulting config carries that exact
        object on its ``schema`` attribute so downstream wiring sees
        the richer prompt renderer attached to it.

        No :class:`OntologyFilterStage` is added to the stage list —
        the pipeline is in suggestion mode purely by composition.
        """
        bridged = ontology.as_extraction_schema()
        config = ExtractionConfig.from_stages(
            stages=[LLMPropositionStage(), LLMTopicExtractionStage()],
            schema=bridged,
        )
        # Identity, not just equality: the rebound
        # ``format_as_prompt_constraint`` lives on this specific
        # instance; any copy would revert to the flat rendering.
        assert config.schema is bridged
        assert isinstance(config.schema, ExtractionSchema)
        # Requirement 11.3 — strict enforcement lives in the filter
        # stage, never on ``ExtractionSchema.strict``.
        assert config.schema.strict is False

    def test_bridged_schema_renders_ontology_prompt_markers(self, ontology):
        """Requirement 5.2 — the bridged schema's
        ``format_as_prompt_constraint()`` delegates to
        :meth:`OntologySchema.format_as_prompt_constraint`, so the LLM
        sees the richer ontology text (class hierarchy, domain/range
        arrows, datatype properties, STRICT MODE paragraph) rather
        than the flat :class:`ExtractionSchema` rendering.

        We assert by substring on markers that are present only in the
        ontology rendering — ``# Ontology-guided extraction`` heading,
        the ``Employee -> Company`` domain/range arrow, the
        ``Person.age`` datatype-property line, and the ``STRICT MODE``
        closing paragraph (the default when ``strict_prompt=True``).
        The flat :class:`ExtractionSchema` rendering emits none of
        these — its output starts with ``Entity types:`` and lists
        ``Person``, ``Employee`` etc. without any ``->`` arrows.
        """
        bridged = ontology.as_extraction_schema()
        text = bridged.format_as_prompt_constraint()

        # Ontology-specific header — not present in the flat rendering.
        assert "# Ontology-guided extraction" in text

        # Domain/range arrow syntax unique to the ontology renderer.
        assert "Employee -> Company" in text

        # Datatype-property line — flat schema has no datatype surface
        # (Requirement 11.6) so this substring cannot appear in its
        # output.
        assert "Person.age" in text
        assert "integer" in text

        # ``strict_prompt=True`` is the default
        # (:meth:`OntologySchema.format_as_prompt_constraint`), so the
        # STRICT MODE closing paragraph must be present.
        assert "STRICT MODE" in text

        # Sanity-check that the *flat* ExtractionSchema rendering
        # would NOT produce these markers. We construct a
        # hand-built flat schema with the same entity/relationship
        # content and assert the markers are absent — this is the
        # negative control that makes the positive assertions
        # meaningful.
        flat = ExtractionSchema(
            entity_types=bridged.entity_types,
            relationship_types=bridged.relationship_types,
            strict=False,
        )
        flat_text = flat.format_as_prompt_constraint()
        assert "# Ontology-guided extraction" not in flat_text
        assert "Employee -> Company" not in flat_text
        assert "Person.age" not in flat_text

    def test_llm_topic_stage_transform_receives_ontology_prompt(self, ontology):
        """Requirement 5.2 (stage-level) — wiring the bridged schema
        into :class:`LLMTopicExtractionStage` and building its
        :class:`TopicExtractor` transform propagates the ontology
        prompt text into ``schema_constraints``.

        This closes the loop from Requirement 5.1 (config binds
        schema) through 5.2 (schema renders ontology prompt): the
        stage's transform is the object that :class:`TopicExtractor`
        uses at invocation time, so proving the richer text reaches
        this point is equivalent to proving the LLM would see it.
        """
        bridged = ontology.as_extraction_schema()
        stage = LLMTopicExtractionStage(schema=bridged)
        transform = stage.as_transform()

        assert isinstance(transform, TopicExtractor)
        # ``schema_constraints`` is the string the prompt template's
        # ``{schema_constraints}`` slot interpolates; it must carry
        # the ontology-specific markers.
        constraints = transform.schema_constraints
        assert "# Ontology-guided extraction" in constraints
        assert "Employee -> Company" in constraints
        assert "Person.age" in constraints

    def test_suggestion_mode_does_not_drop_any_topic(self, ontology):
        """Requirement 5.3 — no entity, fact, or topic is dropped by
        any feature-owned code path in suggestion mode.

        The *only* feature-owned code path in a no-
        :class:`OntologyFilterStage` pipeline that could even
        hypothetically drop content is :class:`SchemaFilter` consuming
        the bridged schema. Because
        :meth:`OntologySchema.as_extraction_schema` sets
        ``strict=False``, the filter short-circuits to a passthrough
        (see :class:`SchemaFilter.__call__`). We verify that here
        with a :class:`TopicCollection` that contains a deliberate
        mix of ontology-valid and ontology-invalid entities / facts:
        every one of them must survive.

        Bit-identity is the assertion: the input metadata dict and the
        output metadata dict are ``==``. That proves no feature-owned
        code edited the collection in any way.
        """
        bridged = ontology.as_extraction_schema()
        # Suggestion-mode bridge is ``strict=False``; the filter is a
        # no-op in that state.
        assert bridged.strict is False

        # Build a deliberately mixed TopicCollection. If any future
        # regression introduced feature-owned filtering in the no-
        # OntologyFilterStage path, the ``UnknownCls`` and
        # ``UNKNOWN_PRED`` entries would disappear and this assertion
        # would fail loudly.
        tc = TopicCollection(topics=[
            Topic(
                value="topic-1",
                entities=[
                    Entity(value="Alice", classification="Person"),   # valid
                    Entity(value="Bob", classification="UnknownCls"),  # invalid
                ],
                statements=[
                    Statement(
                        value="stmt",
                        facts=[
                            # Ontology-valid SPO fact.
                            Fact(
                                subject=Entity(value="Alice", classification="Employee"),
                                predicate=Relation(value="WORKS_FOR"),
                                object=Entity(value="Acme", classification="Company"),
                            ),
                            # Ontology-invalid SPO fact — predicate
                            # not in the ontology.
                            Fact(
                                subject=Entity(value="Alice", classification="Person"),
                                predicate=Relation(value="UNKNOWN_PRED"),
                                object=Entity(value="Bob", classification="Person"),
                            ),
                            # Ontology-valid SPC fact.
                            Fact(
                                subject=Entity(value="Alice", classification="Person"),
                                predicate=Relation(value="age"),
                                complement="42",
                            ),
                            # Ontology-invalid SPC fact — bad literal
                            # for xsd:integer.
                            Fact(
                                subject=Entity(value="Alice", classification="Person"),
                                predicate=Relation(value="age"),
                                complement="forty-two",
                            ),
                        ],
                    ),
                ],
            ),
            Topic(
                value="topic-empty",  # no entities and no facts — still kept in suggestion mode
            ),
        ])

        node = _make_node(tc)
        # Snapshot the metadata *before* running the filter. Use
        # ``dict(...)`` to capture a shallow copy so a later in-place
        # mutation on ``node.metadata`` would be caught.
        before = dict(node.metadata)

        # Run SchemaFilter with the bridged schema. This is the
        # feature-owned filter in a no-OntologyFilterStage pipeline.
        SchemaFilter(extraction_schema=bridged)([node])

        # Bit-identity: metadata is unchanged.
        assert node.metadata == before
        # Requirement 5.3 restated in content terms — every topic,
        # every entity, every fact is still present.
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert len(out.topics) == 2
        assert [t.value for t in out.topics] == ["topic-1", "topic-empty"]
        # Every original entity still there.
        assert [e.value for e in out.topics[0].entities] == ["Alice", "Bob"]
        # Every original fact still there (4 of them).
        assert len(out.topics[0].statements[0].facts) == 4


# ---------------------------------------------------------------------------
# Task 6.2 (PBT) — Property P3: suggestion mode never filters
# ---------------------------------------------------------------------------


# Hypothesis strategies for arbitrary TopicCollections. The aim is
# coverage, not realism: we deliberately generate classifications and
# predicates that are overwhelmingly out-of-ontology to ensure the
# filter has every opportunity to drop something — and fails the
# property if it ever does.

_CLASSIFICATION_ST = st.text(
    alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
    min_size=1, max_size=8,
)

_PREDICATE_ST = st.text(
    alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
    min_size=1, max_size=8,
)

_VALUE_ST = st.text(min_size=1, max_size=6)


def _entity_strategy() -> st.SearchStrategy:
    return st.builds(
        Entity,
        value=_VALUE_ST,
        classification=st.one_of(st.none(), _CLASSIFICATION_ST),
    )


def _spo_fact_strategy() -> st.SearchStrategy:
    return st.builds(
        Fact,
        subject=_entity_strategy(),
        predicate=st.builds(Relation, value=_PREDICATE_ST),
        object=_entity_strategy(),
    )


def _spc_fact_strategy() -> st.SearchStrategy:
    # ``complement`` is typed as Optional[Union[Entity, str]]; keep the
    # string form since that is what the LLM typically emits and
    # exercises the opt-out path if strict mode were (incorrectly)
    # active.
    return st.builds(
        Fact,
        subject=_entity_strategy(),
        predicate=st.builds(Relation, value=_PREDICATE_ST),
        complement=_VALUE_ST,
    )


def _fact_strategy() -> st.SearchStrategy:
    return st.one_of(_spo_fact_strategy(), _spc_fact_strategy())


def _statement_strategy() -> st.SearchStrategy:
    return st.builds(
        Statement,
        value=_VALUE_ST,
        facts=st.lists(_fact_strategy(), max_size=3),
    )


def _topic_strategy() -> st.SearchStrategy:
    return st.builds(
        Topic,
        value=_VALUE_ST,
        entities=st.lists(_entity_strategy(), max_size=3),
        statements=st.lists(_statement_strategy(), max_size=2),
    )


def _topic_collection_strategy() -> st.SearchStrategy:
    return st.builds(
        TopicCollection,
        topics=st.lists(_topic_strategy(), max_size=3),
    )


class TestSuggestionModeNeverFilters:
    """Task 6.2 — Property **P3**: suggestion mode never filters.

    Validates: Requirements 5.3, 5.4.

    Hypothesis generates arbitrary :class:`TopicCollection`\\ s; the
    suggestion-mode pipeline path (bridged :class:`ExtractionSchema`
    + :class:`SchemaFilter` in ``strict=False`` mode) must leave every
    one bit-identical.

    This is the property form of ``test_suggestion_mode_does_not_drop_any_topic``:
    the example-based test pins one specific mixed collection, and
    this property verifies the invariant holds universally across
    machine-generated inputs that include cases a human would not
    think to hand-craft.
    """

    @given(tc=_topic_collection_strategy())
    @settings(
        max_examples=50,
        suppress_health_check=[
            HealthCheck.too_slow,
            # ``ontology`` is a function-scoped fixture; hypothesis warns
            # that state may leak across examples. In our case the
            # fixture is read-only inside the property body (the bridged
            # schema is a fresh object per example via
            # ``ontology.as_extraction_schema()``), so the warning is
            # safe to suppress.
            HealthCheck.function_scoped_fixture,
        ],
    )
    def test_arbitrary_topic_collection_passes_through_unchanged(
        self, ontology, tc
    ):
        """For any :class:`TopicCollection`, running the suggestion-
        mode filter chain produces an output that is byte-for-byte
        identical to the input.

        Proves P3 by Hypothesis exhaustion across the generator
        space: any future change that introduces silent dropping
        would produce a counterexample here.
        """
        bridged = ontology.as_extraction_schema()
        node = _make_node(tc)
        before = dict(node.metadata)

        SchemaFilter(extraction_schema=bridged)([node])

        assert node.metadata == before


# ---------------------------------------------------------------------------
# Task 6.3 — Separability smoke test
# ---------------------------------------------------------------------------


# Fully-qualified names of every module in the strict-mode
# re-export chain plus the parents we need to force Python to
# re-run in a fresh import. Putting these in one list keeps the
# setup and teardown code paired.
_STRICT_MODULE_FQN = (
    "graphrag_toolkit.lexical_graph.indexing.extract.stages.ontology_filter_stage"
)
_PARENT_MODULE_FQNS = (
    "graphrag_toolkit.lexical_graph.indexing.extract.stages",
    "graphrag_toolkit.lexical_graph.indexing.extract",
)


class TestSeparabilitySmoke:
    """Task 6.3 — separability smoke test.

    Validates: Requirements 14.3, 14.4, 14.5, NFR-7.

    Simulates the absence of
    ``graphrag_toolkit.lexical_graph.indexing.extract.stages.ontology_filter_stage``
    via ``sys.modules`` manipulation, forces a fresh import of the
    ``extract`` package, and asserts:

    1. The package re-imports without raising ``ImportError`` — proves
       the conditional ``try/except ImportError`` guards in
       ``stages/__init__.py`` and ``extract/__init__.py`` actually
       handle the missing strict-mode module (Requirements 14.4, 14.5).
    2. :class:`OntologySchema` (and its suggestion-mode surface) is
       still exported — proves the suggestion-mode module has no
       runtime dependency on the strict-mode module (Requirement
       14.3).
    3. :class:`OntologyFilterStage` is absent from the re-imported
       package — proves the conditional re-export is a true no-op
       when the underlying module is unavailable (Requirement 14.5).

    ``sys.modules`` state is saved before the simulation and
    restored in ``finally`` so no other tests in this process see
    side-effects.
    """

    def test_extract_package_imports_cleanly_without_strict_module(self):
        """With the strict-mode module blocked from import, re-importing
        the ``extract`` package must succeed and must expose the
        suggestion-mode surface.

        Implementation note on ``sys.modules``: setting
        ``sys.modules[name] = None`` is Python's documented way to
        force an ``ImportError`` when ``name`` is imported. We use
        this instead of physically removing or renaming the on-disk
        file because:

        * No filesystem writes → no risk of leaving the repo in a
          broken state if the test crashes mid-run.
        * The behaviour under test is precisely the one the
          separability guard (``try/except ImportError``) is meant to
          handle.
        """
        # --- Snapshot the current state so we can restore on exit. ---
        #
        # We need the *full ancestor chain* including
        # ``graphrag_toolkit.lexical_graph`` and its parents, because
        # removing a module from ``sys.modules`` without also removing
        # its attribute on the parent package leaves a dangling
        # reference that ``from ... import`` would still resolve. The
        # simplest safe move is to snapshot all ancestor entries and
        # put them back unchanged in ``finally``.
        snapshot_keys = list(_PARENT_MODULE_FQNS) + [_STRICT_MODULE_FQN]
        saved: dict = {key: sys.modules.get(key) for key in snapshot_keys}
        # Also snapshot attribute state on the grand-parent package so
        # we can restore references that ``del sys.modules[child]``
        # would otherwise leave stale.
        grandparent_fqn = "graphrag_toolkit.lexical_graph.indexing"
        grandparent = sys.modules.get(grandparent_fqn)
        saved_extract_attr = (
            getattr(grandparent, "extract", None) if grandparent else None
        )
        extract_mod = sys.modules.get(
            "graphrag_toolkit.lexical_graph.indexing.extract"
        )
        saved_stages_attr = (
            getattr(extract_mod, "stages", None) if extract_mod else None
        )

        try:
            # --- 1. Block strict-mode module imports. ---
            #
            # ``sys.modules[key] = None`` causes any subsequent
            # ``import key`` to raise ``ImportError`` per the import
            # machinery's documented behaviour, which the
            # ``try/except ImportError`` wrappers in ``stages/__init__``
            # and ``extract/__init__`` will catch.
            sys.modules[_STRICT_MODULE_FQN] = None

            # --- 2. Evict the parent packages so ``__init__.py``
            #        re-runs on next import. ---
            for parent in _PARENT_MODULE_FQNS:
                sys.modules.pop(parent, None)
            # Also drop the cached attribute on the grand-parent so
            # the ``import ... extract`` below triggers ``_find_and_load``
            # rather than returning the cached module object.
            if grandparent is not None and hasattr(grandparent, "extract"):
                try:
                    delattr(grandparent, "extract")
                except AttributeError:
                    # ``extract`` may not be a regular attribute on
                    # every Python version; treat this defensively.
                    pass

            # --- 3. Fresh import of the extract package. ---
            #
            # This is the assertion under test: the conditional
            # re-export guards must catch the simulated ``ImportError``
            # and leave the rest of ``extract/__init__`` running to
            # completion. A failure here (unhandled ``ImportError``)
            # would cause this ``import`` line itself to raise,
            # failing the test loudly.
            ext_mod = importlib.import_module(
                "graphrag_toolkit.lexical_graph.indexing.extract"
            )

            # --- 4. Suggestion-mode surface is intact. ---
            #
            # Requirement 14.3 — ``OntologySchema`` does not depend on
            # the strict-mode module, so it must still be exported.
            assert hasattr(ext_mod, "OntologySchema"), (
                "OntologySchema must remain exported when the strict-"
                "mode module is absent (Requirement 14.3)."
            )
            assert hasattr(ext_mod, "OntologyLoadError"), (
                "OntologyLoadError must remain exported (Requirement 14.3)."
            )
            assert hasattr(ext_mod, "OntologyClass")
            assert hasattr(ext_mod, "ObjectProperty")
            assert hasattr(ext_mod, "DatatypeProperty")

            # --- 5. Strict-mode re-export became a no-op. ---
            #
            # Requirement 14.5 — the conditional re-export is expected
            # to be absent, NOT present-but-broken. ``hasattr`` is the
            # right predicate because the ``try/except`` in
            # ``extract/__init__`` simply ``pass``\\ es on failure,
            # which means the name is never bound at all.
            assert not hasattr(ext_mod, "OntologyFilterStage"), (
                "OntologyFilterStage must NOT be exported when the "
                "strict-mode module is simulated absent; the "
                "conditional re-export must be a true no-op "
                "(Requirement 14.5)."
            )

            # --- 6. Also check the stages sub-package. ---
            #
            # The re-export in ``stages/__init__.py`` mirrors the one
            # in ``extract/__init__.py``; both must be no-ops.
            stages_mod = importlib.import_module(
                "graphrag_toolkit.lexical_graph.indexing.extract.stages"
            )
            assert not hasattr(stages_mod, "OntologyFilterStage"), (
                "stages.OntologyFilterStage must NOT be exported when "
                "the strict-mode module is simulated absent "
                "(Requirement 14.5)."
            )

        finally:
            # Restore ``sys.modules`` state precisely so no other test
            # in this process sees side-effects. Using ``pop`` before
            # reassignment avoids leaving ghost entries for keys that
            # did not exist in the original snapshot.
            for key, mod in saved.items():
                if mod is None:
                    sys.modules.pop(key, None)
                else:
                    sys.modules[key] = mod
            # Restore the ``extract`` attribute on the grand-parent
            # package so other code paths that do attribute lookup
            # rather than ``sys.modules`` get the original module back.
            if grandparent is not None and saved_extract_attr is not None:
                try:
                    setattr(grandparent, "extract", saved_extract_attr)
                except Exception:
                    pass
            if saved_stages_attr is not None and saved_extract_attr is not None:
                try:
                    setattr(saved_extract_attr, "stages", saved_stages_attr)
                except Exception:
                    pass
