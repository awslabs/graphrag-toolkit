# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for strict-mode ontology-guided extraction.

Covers Task 6.4 from the ontology-guided-extraction spec:

* Building :class:`ExtractionConfig.from_stages` with
  :class:`OntologyFilterStage` appended to the stage list, asserting
  the config composes cleanly through the existing entry point
  (Requirements 8.1, 8.4).
* Running the same mixed mock :class:`TopicCollection` as Task 6.1's
  suggestion-mode test through the strict-mode filter and asserting
  that only the ontology-valid subset survives (Requirements 6.1–6.6,
  7.1, 7.2).
* Parametrisation on ``validate_datatypes`` — when ``True``, SPC facts
  with bad literals drop; when ``False``, the same bad-literal facts
  are kept while predicate-out-of-ontology SPC facts still drop
  (Requirements 9.1, 9.2, 9.3).

The module name intentionally contains ``strict`` so a single
``pytest -k "not strict"`` selector or ``--ignore=<this file>`` cleanly
excludes it without excising the suggestion-mode integration file
(``test_ontology_integration_suggestion.py``). This is the structural
separability guarantee that Requirement 14.8 exists to enforce, and
Requirement 14.2 requires strict-mode tests to live in files separate
from suggestion-mode tests.

Implementation note on the "integration" scope
----------------------------------------------
As in :mod:`test_ontology_integration_suggestion`, we test stage-level
composition and filter behaviour rather than a full LLM-driven
pipeline run. A real ingestion run would invoke an LLM to produce
the :class:`TopicCollection` — irrelevant to the strict-mode filter
contract, which operates on whatever :class:`TopicCollection` is
handed to it. The filter is the main object under test here;
:class:`LLMTopicExtractionStage` composition is verified at the
config level, not via an actual LLM call.
"""

import pytest

# Soft-skip when rdflib is missing — matches the treatment in
# ``test_ontology_filter_stage.py`` and
# ``test_ontology_integration_suggestion.py`` so skipped-test
# reporting stays consistent across the ontology-guided-extraction
# suite.
pytest.importorskip("rdflib")

from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph import ExtractionConfig
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract import (
    ExtractionSchema,
    LLMPropositionStage,
    LLMTopicExtractionStage,
    OntologyFilterStage,
    OntologySchema,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.ontology_filter_stage import (
    OntologyFilterTransform,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.schema_filter_stage import (
    SchemaFilter,
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
# used in :mod:`test_ontology_integration_suggestion` and design §5.1.
# Redeclared here (rather than imported across files) so this module
# stays self-contained and can be excised as a strict-mode unit without
# touching the suggestion-mode file (Requirement 14.2).
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
    :class:`OntologySchema` is effectively immutable after load — a
    shared fixture could otherwise couple tests via name-index or
    ancestor state if the implementation ever grew mutable internals.
    """
    return OntologySchema.from_turtle_string(PERSON_ONTOLOGY_TURTLE)


def _make_node(tc: TopicCollection) -> TextNode:
    """Wrap a :class:`TopicCollection` as a :class:`TextNode` the way
    :class:`LLMTopicExtractionStage` would emit it.

    Serialising via ``model_dump()`` matches the exact metadata shape
    the filter transforms expect — they round-trip through
    ``TopicCollection(**topics_data)`` → ``tc.model_dump()``, so
    anything we feed them must survive that serialisation.
    """
    node = TextNode(text="test")
    node.metadata[TOPICS_KEY] = tc.model_dump()
    return node


def _build_mixed_topic_collection() -> TopicCollection:
    """Build the same mixed :class:`TopicCollection` Task 6.1 uses.

    Contains a deliberate mix of ontology-valid and ontology-invalid
    entities and facts so downstream assertions can precisely enumerate
    which survive and which drop:

    * **Alice / Person** — valid entity.
    * **Bob / UnknownCls** — invalid entity (class not in ontology);
      dropped under Requirement 6.1.
    * **Employee worksFor Company** — valid SPO fact; kept.
    * **Person UNKNOWN_PRED Person** — invalid predicate; dropped under
      Requirement 6.2.
    * **Person age "42"** — valid SPC fact; kept when
      ``validate_datatypes=True`` and also when ``False``.
    * **Person age "forty-two"** — SPC fact with literal that fails
      ``xsd:integer``; dropped under Requirement 9.1 with
      ``validate_datatypes=True``; kept under Requirement 9.2 with
      ``validate_datatypes=False``.
    * **topic-empty** — a second topic with no entities and no
      statements; dropped under Requirement 7.2 regardless of flag.

    Keeping this inline (rather than extracting to a conftest fixture)
    preserves the strict-mode file's separability — excising this file
    leaves nothing orphan elsewhere in the test tree.
    """
    return TopicCollection(topics=[
        Topic(
            value="topic-1",
            entities=[
                Entity(value="Alice", classification="Person"),    # valid
                Entity(value="Bob", classification="UnknownCls"),  # invalid
            ],
            statements=[
                Statement(
                    value="stmt",
                    facts=[
                        # Ontology-valid SPO — kept.
                        Fact(
                            subject=Entity(
                                value="Alice",
                                classification="Employee",
                            ),
                            predicate=Relation(value="WORKS_FOR"),
                            object=Entity(
                                value="Acme",
                                classification="Company",
                            ),
                        ),
                        # Ontology-invalid SPO — predicate absent.
                        Fact(
                            subject=Entity(
                                value="Alice",
                                classification="Person",
                            ),
                            predicate=Relation(value="UNKNOWN_PRED"),
                            object=Entity(
                                value="Bob",
                                classification="Person",
                            ),
                        ),
                        # Ontology-valid SPC — literal matches xsd:integer.
                        Fact(
                            subject=Entity(
                                value="Alice",
                                classification="Person",
                            ),
                            predicate=Relation(value="age"),
                            complement="42",
                        ),
                        # Ontology-invalid SPC — literal fails xsd:integer.
                        Fact(
                            subject=Entity(
                                value="Alice",
                                classification="Person",
                            ),
                            predicate=Relation(value="age"),
                            complement="forty-two",
                        ),
                    ],
                ),
            ],
        ),
        # Second topic is intentionally empty — survives no filter and
        # must be dropped by the topic-level preservation rule.
        Topic(value="topic-empty"),
    ])


# ---------------------------------------------------------------------------
# Task 6.4 — Strict-mode composition and filter behaviour
# ---------------------------------------------------------------------------


class TestStrictModeComposition:
    """Pipeline composition via ``ExtractionConfig.from_stages``.

    Validates: Requirements 8.1, 8.4, 14.8.
    """

    def test_from_stages_accepts_ontology_filter_stage(self, ontology):
        """``ExtractionConfig.from_stages`` accepts an
        :class:`OntologyFilterStage` appended after
        :class:`LLMTopicExtractionStage` without requiring any
        pipeline-builder or config-class change (Requirements 8.1,
        8.4).

        The assertion chain verifies three things in one pass:

        * The config object exposes the provided stage list verbatim
          on its ``stages`` attribute (the composable-pipeline
          contract of ``from_stages``).
        * The bridged :class:`ExtractionSchema` sits on ``schema`` and
          is an ``ExtractionSchema`` instance (Requirement 11.4).
        * The filter stage is the last stage in the list — pipeline
          order matters because ``OntologyFilterStage`` must run
          *after* ``LLMTopicExtractionStage`` produces topics
          (design §"Mode selection").
        """
        bridged = ontology.as_extraction_schema()
        config = ExtractionConfig.from_stages(
            stages=[
                LLMPropositionStage(),
                LLMTopicExtractionStage(schema=bridged),
                OntologyFilterStage(ontology),
            ],
            schema=bridged,
        )

        assert config.stages is not None
        assert len(config.stages) == 3
        # Filter must be last — strict mode runs post-extraction.
        assert isinstance(config.stages[-1], OntologyFilterStage)
        # Middle stage is the LLM extraction stage.
        assert isinstance(config.stages[1], LLMTopicExtractionStage)
        # Config still carries the bridged schema so the LLM stage
        # picks up the richer ontology prompt.
        assert config.schema is bridged
        assert isinstance(config.schema, ExtractionSchema)

    def test_from_stages_with_validate_datatypes_false(self, ontology):
        """``OntologyFilterStage(ontology, validate_datatypes=False)``
        composes through ``from_stages`` identically — the opt-out
        flag is a stage-level construction argument, not a pipeline-
        level configuration knob (Requirement 8.1).

        This is the config-time companion of the parametrised
        behavioural test below: we verify the flag reaches the stage
        through the standard composition path before verifying its
        effect on fact filtering.
        """
        bridged = ontology.as_extraction_schema()
        stage = OntologyFilterStage(ontology, validate_datatypes=False)
        config = ExtractionConfig.from_stages(
            stages=[
                LLMTopicExtractionStage(schema=bridged),
                stage,
            ],
            schema=bridged,
        )
        assert config.stages[-1] is stage
        # The stage honours the flag at construction time; the
        # behavioural assertion lives in
        # ``TestStrictModeFiltering.test_validate_datatypes_false_keeps_bad_literals``.
        assert stage._validate_datatypes is False


# ---------------------------------------------------------------------------
# Strict-mode filter behaviour — the main deliverable of Task 6.4
# ---------------------------------------------------------------------------


class TestStrictModeFiltering:
    """Strict-mode filter drops the ontology-invalid subset.

    Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2,
    7.3, 7.4, 7.5, 9.1, 9.2, 9.3, 14.1, 14.2, 14.8.

    The key invariant these tests pin is: running the
    :class:`OntologyFilterTransform` produced by
    :meth:`OntologyFilterStage.as_transform` over the same mixed mock
    :class:`TopicCollection` that Task 6.1's suggestion-mode test
    feeds through :class:`SchemaFilter` produces the precise
    ontology-valid subset, with empty topics removed and surviving
    topic order preserved.
    """

    def test_strict_mode_drops_ontology_invalid_content(self, ontology):
        """With ``validate_datatypes=True`` (the default), only the
        ontology-valid content survives.

        Expected output from the mixed collection in
        :func:`_build_mixed_topic_collection`:

        * ``topic-empty`` — dropped (Requirement 7.2).
        * ``Bob / UnknownCls`` entity — dropped (Requirement 6.1).
        * ``UNKNOWN_PRED`` SPO fact — dropped (Requirement 6.2).
        * ``age forty-two`` SPC fact — dropped (Requirements 6.3c,
          9.1).

        Surviving content:

        * ``topic-1`` with one entity (``Alice``), one SPO fact
          (``Employee WORKS_FOR Company``), one SPC fact
          (``Person age "42"``).
        """
        tc = _build_mixed_topic_collection()
        node = _make_node(tc)

        # Use the stage's ``as_transform`` method rather than
        # constructing the transform directly — this is the exact
        # object the pipeline would invoke, so running it here proves
        # the stage's wiring works end-to-end for this composition.
        stage = OntologyFilterStage(ontology)  # validate_datatypes=True by default
        stage.as_transform()([node])

        out = TopicCollection(**node.metadata[TOPICS_KEY])

        # Only ``topic-1`` survives — ``topic-empty`` is dropped by
        # Requirement 7.2.
        assert len(out.topics) == 1
        assert out.topics[0].value == "topic-1"

        # Entity-level check — Alice survives, Bob (UnknownCls) dropped.
        assert [e.value for e in out.topics[0].entities] == ["Alice"]

        # Fact-level check — exactly two facts survive:
        # * Alice (Employee) WORKS_FOR Acme (Company)  — SPO
        # * Alice (Person) age "42"                    — SPC
        kept_facts = out.topics[0].statements[0].facts
        assert len(kept_facts) == 2

        # Identify facts by predicate + shape so the assertion is
        # order-independent (the filter preserves input order so the
        # current output order is deterministic, but asserting on
        # predicate/shape decouples the test from that implementation
        # detail).
        kept_spo = [f for f in kept_facts if f.object is not None]
        kept_spc = [f for f in kept_facts if f.complement is not None]
        assert len(kept_spo) == 1
        assert len(kept_spc) == 1

        # The SPO that survived is the WORKS_FOR fact.
        assert kept_spo[0].predicate.value == "WORKS_FOR"
        assert kept_spo[0].subject.classification == "Employee"
        assert kept_spo[0].object.classification == "Company"

        # The SPC that survived is the age "42" fact — the "forty-two"
        # variant was dropped by ``_validate_literal_against_xsd``.
        assert kept_spc[0].predicate.value == "age"
        assert kept_spc[0].complement == "42"

    @pytest.mark.parametrize(
        "validate_datatypes, expect_bad_literal_kept",
        [
            # Requirement 9.1 — bad literal drops.
            (True, False),
            # Requirement 9.2 — bad literal kept.
            (False, True),
        ],
        ids=["validate_datatypes=True", "validate_datatypes=False"],
    )
    def test_validate_datatypes_parametrised(
        self, ontology, validate_datatypes, expect_bad_literal_kept
    ):
        """The ``validate_datatypes`` flag isolates the XSD literal check.

        Same mixed input as the default-flag test. Assertions differ
        only in one cell: the ``age forty-two`` SPC fact.

        When ``validate_datatypes=True``: Requirement 9.1 drops it.
        When ``validate_datatypes=False``: Requirement 9.2 keeps it.

        Both flag values MUST still:

        * Drop the ``UNKNOWN_PRED`` SPO fact (Requirement 6.2 — flag
          doesn't apply to SPO).
        * Drop the ``Bob / UnknownCls`` entity (Requirement 6.1 — flag
          doesn't apply to entities).
        * Drop ``topic-empty`` (Requirement 7.2 — flag doesn't apply
          to topic preservation).
        * Keep the ``WORKS_FOR`` SPO fact and the ``age "42"`` SPC
          fact (both ontology-valid under either flag).

        The parametrisation verifies all of those invariants in one
        test by asserting on the kept-fact set rather than just on the
        one bit-of-difference.
        """
        tc = _build_mixed_topic_collection()
        node = _make_node(tc)

        stage = OntologyFilterStage(
            ontology, validate_datatypes=validate_datatypes,
        )
        stage.as_transform()([node])

        out = TopicCollection(**node.metadata[TOPICS_KEY])

        # Invariants that must hold under both flag values.
        assert len(out.topics) == 1
        assert out.topics[0].value == "topic-1"
        # Entity filter is not affected by validate_datatypes.
        assert [e.value for e in out.topics[0].entities] == ["Alice"]

        kept_facts = out.topics[0].statements[0].facts
        kept_predicates = [f.predicate.value for f in kept_facts]

        # ``UNKNOWN_PRED`` is a predicate-level miss; dropped under
        # either flag (Requirement 9.3 restated for SPO).
        assert "UNKNOWN_PRED" not in kept_predicates

        # ``WORKS_FOR`` SPO and ``age`` SPC are ontology-valid; kept
        # under either flag.
        assert "WORKS_FOR" in kept_predicates

        # Collect the SPC ``age`` complements to assert on the literal-
        # sensitive cell. Two distinct SPC facts share the ``age``
        # predicate in the mixed input: one valid ("42"), one bad
        # ("forty-two").
        age_spc_complements = [
            f.complement for f in kept_facts
            if f.predicate.value == "age" and f.complement is not None
        ]

        # The valid ``age "42"`` fact always survives.
        assert "42" in age_spc_complements

        if expect_bad_literal_kept:
            # Requirement 9.2 — the bad literal is kept when the
            # opt-out is engaged.
            assert "forty-two" in age_spc_complements
            # Total SPC count under the opt-out: both age facts
            # survive (and no other SPC was in the input).
            assert len(age_spc_complements) == 2
            # Total kept facts: WORKS_FOR SPO + two ``age`` SPC.
            assert len(kept_facts) == 3
        else:
            # Requirement 9.1 — the bad literal drops when validating.
            assert "forty-two" not in age_spc_complements
            # Only the valid ``age "42"`` SPC remains.
            assert len(age_spc_complements) == 1
            # Total kept facts: WORKS_FOR SPO + valid ``age`` SPC.
            assert len(kept_facts) == 2

    def test_predicate_out_of_ontology_spc_drops_under_both_flags(
        self, ontology
    ):
        """Explicit check for Requirement 9.3 — an SPC fact whose
        predicate is not a declared :class:`DatatypeProperty` drops
        under both ``validate_datatypes=True`` and
        ``validate_datatypes=False``.

        The mixed-input test above covers the SPO predicate-miss case
        (``UNKNOWN_PRED``). This test adds an SPC predicate-miss case
        — a ``Fact`` whose predicate ``BOGUS_DT`` is not in the
        ontology's datatype-property set — and asserts both flag
        values drop it. This pins the "opt-out is *literal-only*"
        contract that Requirement 9.3 specifies.
        """
        tc = TopicCollection(topics=[
            Topic(
                value="t",
                entities=[Entity(value="Alice", classification="Person")],
                statements=[
                    Statement(
                        value="s",
                        facts=[
                            # SPC with an out-of-ontology predicate.
                            # Both flag values must drop this fact.
                            Fact(
                                subject=Entity(
                                    value="Alice",
                                    classification="Person",
                                ),
                                predicate=Relation(value="BOGUS_DT"),
                                complement="whatever",
                            ),
                        ],
                    ),
                ],
            ),
        ])

        for flag in (True, False):
            node = _make_node(tc)  # fresh node per flag to avoid cross-run state
            OntologyFilterTransform(
                ontology=ontology, validate_datatypes=flag,
            )([node])
            out = TopicCollection(**node.metadata[TOPICS_KEY])
            # Entity ``Alice`` is valid, so the topic survives; but
            # the SPC fact with the out-of-ontology predicate is
            # dropped regardless of the opt-out.
            assert len(out.topics) == 1, (
                f"Topic should survive via Alice entity (flag={flag})"
            )
            assert out.topics[0].statements[0].facts == [], (
                f"SPC fact with out-of-ontology predicate must drop "
                f"under validate_datatypes={flag} (Requirement 9.3)"
            )


# ---------------------------------------------------------------------------
# Pipeline-level composition — SchemaFilter + OntologyFilterTransform chained
# ---------------------------------------------------------------------------


class TestStrictModeChainedWithSchemaFilter:
    """Strict-mode pipeline with both :class:`SchemaFilter` and
    :class:`OntologyFilterTransform` in sequence produces the
    ontology-valid subset.

    Validates: Requirements 5.2, 6.x, 8.1, 8.4, 14.1, 14.8.

    Under the bridged schema (``strict=False`` — see
    :meth:`OntologySchema.as_extraction_schema`), :class:`SchemaFilter`
    is a documented no-op on the input, and the ontology filter then
    applies the strict-mode rules. This test verifies that the two
    stages compose in sequence without interference — i.e. the
    strict-mode output does NOT depend on whether a preceding schema-
    filter runs with the bridged schema.
    """

    def test_chain_produces_ontology_valid_subset(self, ontology):
        """Run SchemaFilter (no-op under bridged schema) then
        OntologyFilterTransform and assert the output is exactly the
        ontology-valid subset — the same subset the filter-only test
        above verifies.

        This also proves the ``TOPICS_KEY`` round-trip through two
        consecutive transforms — the bridged schema's ``strict=False``
        passthrough plus the ontology filter's full rewrite — leaves
        the final :class:`TopicCollection` in the same shape downstream
        stages expect.
        """
        bridged = ontology.as_extraction_schema()
        # ``strict=False`` is the contract of the bridge — the schema
        # filter is a passthrough, so the ontology filter's output is
        # what actually drives the assertions below.
        assert bridged.strict is False

        tc = _build_mixed_topic_collection()
        node = _make_node(tc)

        # Simulate the strict-mode pipeline step 1: SchemaFilter with
        # the bridged schema — expected no-op because bridged.strict
        # is False.
        SchemaFilter(extraction_schema=bridged)([node])

        # Step 2: OntologyFilterTransform — the strict-mode filter.
        OntologyFilterTransform(ontology=ontology)([node])

        out = TopicCollection(**node.metadata[TOPICS_KEY])

        # Same exact expected subset as in
        # ``test_strict_mode_drops_ontology_invalid_content``.
        assert len(out.topics) == 1
        assert [e.value for e in out.topics[0].entities] == ["Alice"]
        kept = out.topics[0].statements[0].facts
        assert len(kept) == 2
        assert {f.predicate.value for f in kept} == {"WORKS_FOR", "age"}
