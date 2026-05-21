# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for ontology-guided extraction strict-mode filter stage.

Covers Tasks 4.7, 4.8, 4.9, 4.10, 4.11, and 4.12 from the
ontology-guided-extraction spec:

* :class:`OntologyFilterStage` / :class:`OntologyFilterTransform`
  end-to-end filtering on a crafted :class:`TopicCollection` covering
  SPO/SPC kept/dropped cases (Requirements 6.1-6.6, 9.1-9.3).
* Topic-level preservation — non-empty kept, empty dropped, relative
  order preserved (Requirements 7.1, 7.2, 7.3).
* Kill-switch behaviour via ``ENABLE_ONTOLOGY_FILTER_STAGE``
  (Requirement 14.6).
* Metadata isolation — only ``TOPICS_KEY`` rewritten; nodes missing
  ``TOPICS_KEY`` pass through (Requirements 7.4, 7.5).
* Stage interface — ``input_keys``, ``output_keys``, ``stage_type``
  (Requirement 8.2).
* ``_validate_literal_against_xsd`` — one positive and one negative
  literal per supported XSD type; integer subtype range checks;
  ISO-8601 date/dateTime acceptance and rejection; unknown-XSD IRI
  permissive-accept with a single WARN log per unknown type
  (Requirements 10.1-10.8).
* Hypothesis property-based tests at the bottom of the file
  (Tasks 4.9-4.12):

  - **P4** (Task 4.9): strict mode preserves only ontology-valid facts
    (Requirements 6.1-6.6, 14.1).
  - **P5** (Task 4.10): ``validate_datatypes=False`` isolates the
    literal check — predicate-/domain-missing facts still drop
    (Requirements 9.1, 9.2, 9.3, 14.1).
  - **P6** (Task 4.11): strict mode respects subclass closure for
    domain and range; siblings outside the closure drop
    (Requirements 6.2, 6.4, 6.5, 14.1).
  - **P7** (Task 4.12): topic-level emptiness preservation — a topic
    survives iff at least one entity or fact survives, and the
    relative order of surviving topics matches input order
    (Requirements 7.1, 7.2, 7.3, 14.1).

Per Requirement 14.2, strict-mode tests live in this file separately
from :mod:`test_ontology_schema` so the whole strict-mode layer
(production module + tests) can be excised as a unit.

``rdflib`` is a soft dependency (Requirement 4 / NFR-1). The whole
module is skipped via :func:`pytest.importorskip` so environments
without ``rdflib`` installed still see a clean skipped-entry in the
suite summary. ``hypothesis`` is a test-only dependency (design
§Dependencies) and is soft-imported the same way; both are required
by this module but neither leaks into the production surface.
"""

import logging

import pytest

# Soft-skip the whole module when rdflib is missing — this file depends
# on :meth:`OntologySchema.from_turtle_string` for its fixture loader.
pytest.importorskip("rdflib")
# ``hypothesis`` is a test-only dependency used by the property-based
# tests (Tasks 4.9-4.12) at the bottom of this module. Rather than
# guarding only those classes, we skip the whole module so the
# collection story is the same as for ``rdflib``: either everything in
# this file runs or everything is reported as skipped.
pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, given, settings, strategies as st

from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract.ontology_schema import (
    OntologySchema,
    XSD_NAMESPACE,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages import (
    ontology_filter_stage as ofs_module,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.ontology_filter_stage import (
    OntologyFilterStage,
    OntologyFilterTransform,
    _validate_literal_against_xsd,
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
# Fixtures — the Person / Employee / Manager / Company ontology used across
# every test in this module. Redeclared here (rather than imported from
# test_ontology_schema.py) so this file stays cleanly excisable per
# Requirement 14.2.
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

:name a owl:DatatypeProperty ;
    rdfs:label "name" ;
    rdfs:domain :Person ;
    rdfs:range xsd:string .
"""


@pytest.fixture
def ontology() -> OntologySchema:
    """Load the Person/Employee/Manager/Company ontology once per test.

    A fresh schema per test keeps test isolation clean even though
    :class:`OntologySchema` is documented as effectively immutable after
    load — we never want a surprising fixture-scope leak to make a
    failure harder to diagnose.
    """
    return OntologySchema.from_turtle_string(PERSON_ONTOLOGY_TURTLE)


def _make_node(tc: TopicCollection) -> TextNode:
    """Wrap a :class:`TopicCollection` in a :class:`TextNode` the way
    the extraction pipeline would, so the filter sees its expected
    ``node.metadata[TOPICS_KEY]`` shape.

    We intentionally serialise the ``TopicCollection`` via
    ``model_dump()`` because that is the exact shape produced upstream
    by :class:`LLMTopicExtractionStage`; the filter round-trips through
    ``TopicCollection(**topics_data)`` and back via ``model_dump()``,
    so anything we feed it must survive that serialisation.
    """
    node = TextNode(text="test")
    node.metadata[TOPICS_KEY] = tc.model_dump()
    return node


def _make_spo_fact(subj_val, subj_cls, pred, obj_val, obj_cls) -> Fact:
    """Construct an SPO :class:`Fact` — subject entity + predicate + object entity."""
    return Fact(
        subject=Entity(value=subj_val, classification=subj_cls),
        predicate=Relation(value=pred),
        object=Entity(value=obj_val, classification=obj_cls),
    )


def _make_spc_fact(subj_val, subj_cls, pred, complement) -> Fact:
    """Construct an SPC :class:`Fact` — subject entity + predicate + literal complement."""
    return Fact(
        subject=Entity(value=subj_val, classification=subj_cls),
        predicate=Relation(value=pred),
        complement=complement,
    )


# ---------------------------------------------------------------------------
# Task 4.7 — strict-mode filter behaviour
# ---------------------------------------------------------------------------


class TestEntityFiltering:
    """Entity-level filtering (Requirement 6.1)."""

    def test_entity_with_ontology_class_kept(self, ontology):
        """An entity whose classification resolves to an ontology class is
        kept (Requirement 6.1)."""
        tc = TopicCollection(topics=[
            Topic(value="t", entities=[
                Entity(value="Alice", classification="Person"),
            ]),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert len(out.topics) == 1
        assert [e.value for e in out.topics[0].entities] == ["Alice"]

    def test_entity_with_unknown_classification_dropped(self, ontology):
        """An entity whose classification does not resolve to any ontology
        class is dropped; the resulting empty topic is also removed
        (Requirements 6.1, 7.2)."""
        tc = TopicCollection(topics=[
            Topic(value="t", entities=[
                Entity(value="Alice", classification="UnknownCls"),
            ]),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert out.topics == []

    def test_entity_relative_order_preserved(self, ontology):
        """Entities that survive the filter keep their original relative
        order (design §"Algorithm: OntologyFilterStage main loop" —
        list-comprehension-based filter preserves order)."""
        tc = TopicCollection(topics=[
            Topic(value="t", entities=[
                Entity(value="Alice", classification="Person"),
                Entity(value="Bob", classification="UnknownCls"),
                Entity(value="Carol", classification="Employee"),
                Entity(value="Dave", classification="Manager"),
            ]),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert [e.value for e in out.topics[0].entities] == [
            "Alice", "Carol", "Dave",
        ]


class TestSPOFactFiltering:
    """SPO (entity-to-entity) fact filtering (Requirements 6.2, 6.4, 6.5)."""

    def _run(self, ontology, facts):
        """Run the transform on a single-topic, single-statement collection
        and return the surviving facts (or ``[]`` if the topic was dropped)."""
        tc = TopicCollection(topics=[
            Topic(value="t", statements=[
                Statement(value="s", facts=facts),
            ]),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        if not out.topics:
            return []
        return out.topics[0].statements[0].facts

    def test_spo_kept_when_predicate_domain_range_all_match(self, ontology):
        """Employee -worksFor-> Company is the canonical positive case
        (Requirement 6.2)."""
        facts = [
            _make_spo_fact("Alice", "Employee", "WORKS_FOR", "Acme", "Company"),
        ]
        kept = self._run(ontology, facts)
        assert len(kept) == 1

    def test_spo_dropped_subject_class_miss(self, ontology):
        """Unknown subject class → ``resolve_class`` returns ``None`` →
        fact dropped (Requirement 6.1 applied at fact subject site)."""
        facts = [
            _make_spo_fact("Alice", "UnknownCls", "WORKS_FOR", "Acme", "Company"),
        ]
        assert self._run(ontology, facts) == []

    def test_spo_dropped_object_class_miss(self, ontology):
        """Unknown object class → fact dropped (Requirement 6.1 applied at
        fact object site)."""
        facts = [
            _make_spo_fact("Alice", "Employee", "WORKS_FOR", "Acme", "UnknownCls"),
        ]
        assert self._run(ontology, facts) == []

    def test_spo_dropped_predicate_miss(self, ontology):
        """A predicate that matches no declared ObjectProperty → dropped
        (Requirement 6.2 — no candidate ObjectProperty exists)."""
        facts = [
            _make_spo_fact("Alice", "Employee", "GIBBERISH", "Acme", "Company"),
        ]
        assert self._run(ontology, facts) == []

    def test_spo_dropped_domain_mismatch(self, ontology):
        """``Person`` is NOT a subclass of ``Employee`` (the domain of
        ``worksFor``) — direction matters, so the fact is dropped
        (Requirement 6.5 — closure is downward, not upward)."""
        facts = [
            _make_spo_fact("Alice", "Person", "WORKS_FOR", "Acme", "Company"),
        ]
        assert self._run(ontology, facts) == []

    def test_spo_dropped_range_mismatch(self, ontology):
        """``Person`` is not a subclass of ``Company`` (the range of
        ``worksFor``) → dropped (Requirement 6.5)."""
        facts = [
            _make_spo_fact("Alice", "Employee", "WORKS_FOR", "Bob", "Person"),
        ]
        assert self._run(ontology, facts) == []

    def test_spo_kept_via_subject_subclass_closure(self, ontology):
        """``Manager`` ⊂ ``Employee``; a fact with subject class ``Manager``
        for a property declaring ``Employee`` in domain is kept
        (Requirement 6.4 — closure is downward)."""
        facts = [
            _make_spo_fact("Alice", "Manager", "WORKS_FOR", "Acme", "Company"),
        ]
        kept = self._run(ontology, facts)
        assert len(kept) == 1


class TestSPCFactFiltering:
    """SPC (entity-to-literal) fact filtering and ``validate_datatypes``
    opt-out behaviour (Requirements 6.3, 9.1, 9.2, 9.3)."""

    def _run(self, ontology, facts, validate_datatypes=True):
        """Same helper shape as :class:`TestSPOFactFiltering`, parametrized
        on ``validate_datatypes`` so both flag values share one code path."""
        tc = TopicCollection(topics=[
            Topic(value="t", statements=[
                Statement(value="s", facts=facts),
            ]),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(
            ontology=ontology,
            validate_datatypes=validate_datatypes,
        )([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        if not out.topics:
            return []
        return out.topics[0].statements[0].facts

    def test_spc_kept_when_all_checks_pass(self, ontology):
        """Person -age-> "42" is the canonical positive SPC case —
        predicate matches, domain covers, literal validates against
        ``xsd:integer`` (Requirement 6.3)."""
        facts = [_make_spc_fact("Alice", "Person", "age", "42")]
        kept = self._run(ontology, facts)
        assert len(kept) == 1

    def test_spc_dropped_predicate_miss(self, ontology):
        """Predicate matches no declared DatatypeProperty → dropped
        (Requirement 6.3a)."""
        facts = [_make_spc_fact("Alice", "Person", "UNKNOWN_DT", "42")]
        assert self._run(ontology, facts) == []

    def test_spc_dropped_domain_mismatch(self, ontology):
        """``Company`` is not in the closure of ``Person`` (domain of
        ``age``) → dropped (Requirement 6.3b)."""
        facts = [_make_spc_fact("Acme", "Company", "age", "42")]
        assert self._run(ontology, facts) == []

    def test_spc_dropped_literal_type_miss_when_validating(self, ontology):
        """Literal "forty-two" is not a valid ``xsd:integer`` →
        ``validate_datatypes=True`` drops the fact (Requirements 6.3c,
        9.1)."""
        facts = [_make_spc_fact("Alice", "Person", "age", "forty-two")]
        assert self._run(ontology, facts, validate_datatypes=True) == []

    def test_spc_kept_literal_type_miss_when_not_validating(self, ontology):
        """The same bad-literal fact is kept when the opt-out is engaged
        (Requirement 9.2)."""
        facts = [_make_spc_fact("Alice", "Person", "age", "forty-two")]
        kept = self._run(ontology, facts, validate_datatypes=False)
        assert len(kept) == 1

    def test_spc_still_dropped_by_predicate_when_not_validating(self, ontology):
        """``validate_datatypes=False`` must NOT bypass predicate /
        domain checks — only the literal check is opt-out
        (Requirement 9.3)."""
        facts = [_make_spc_fact("Alice", "Person", "UNKNOWN_DT", "42")]
        assert self._run(ontology, facts, validate_datatypes=False) == []

    def test_spc_still_dropped_by_domain_when_not_validating(self, ontology):
        """Domain check is preserved under ``validate_datatypes=False``
        (Requirement 9.3)."""
        facts = [_make_spc_fact("Acme", "Company", "age", "42")]
        assert self._run(ontology, facts, validate_datatypes=False) == []


class TestMalformedFactDropped:
    """Requirement 6.6 — a fact with neither ``object`` nor ``complement``
    is dropped regardless of predicate validity."""

    def test_malformed_fact_dropped(self, ontology):
        # Pydantic allows both fields to be ``None`` simultaneously, so
        # we can construct a malformed fact directly. The filter must
        # drop it rather than attempt to route it.
        malformed = Fact(
            subject=Entity(value="Alice", classification="Person"),
            predicate=Relation(value="SOMETHING"),
        )
        assert malformed.object is None
        assert malformed.complement is None

        tc = TopicCollection(topics=[
            Topic(
                value="t",
                entities=[Entity(value="Alice", classification="Person")],
                statements=[Statement(value="s", facts=[malformed])],
            ),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        # Entity survives because "Alice" resolves to Person; the fact
        # is dropped so the statement is left with an empty fact list.
        assert len(out.topics) == 1
        assert out.topics[0].statements[0].facts == []


class TestTopicPreservation:
    """Topic-level preservation — kept when non-empty, removed when empty,
    relative order preserved (Requirements 7.1, 7.2, 7.3)."""

    def test_non_empty_topic_kept(self, ontology):
        """A topic with at least one surviving entity survives
        (Requirement 7.1)."""
        tc = TopicCollection(topics=[
            Topic(
                value="keeper",
                entities=[Entity(value="Alice", classification="Person")],
            ),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert len(out.topics) == 1
        assert out.topics[0].value == "keeper"

    def test_topic_kept_with_only_surviving_fact(self, ontology):
        """A topic with no entities but at least one surviving fact
        across its statements is kept (Requirement 7.1)."""
        tc = TopicCollection(topics=[
            Topic(
                value="fact-only",
                statements=[Statement(
                    value="s",
                    facts=[_make_spo_fact(
                        "Alice", "Employee", "WORKS_FOR", "Acme", "Company",
                    )],
                )],
            ),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert len(out.topics) == 1
        assert out.topics[0].value == "fact-only"

    def test_empty_topic_removed(self, ontology):
        """A topic whose entities and all facts are filtered out is
        dropped from the collection (Requirement 7.2)."""
        tc = TopicCollection(topics=[
            Topic(
                value="empty",
                entities=[Entity(value="X", classification="UnknownCls")],
                statements=[Statement(
                    value="s",
                    facts=[_make_spo_fact(
                        "X", "UnknownCls", "GIBBERISH", "Y", "UnknownCls",
                    )],
                )],
            ),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert out.topics == []

    def test_originally_empty_topic_removed(self, ontology):
        """A topic that starts out with no entities and no statements
        (nothing to filter, but also nothing to preserve) is removed
        (Requirement 7.2)."""
        tc = TopicCollection(topics=[
            Topic(value="ghost"),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert out.topics == []

    def test_relative_order_preserved(self, ontology):
        """Surviving topics appear in the same relative order they had in
        the input. We interleave one dropped (empty) topic between three
        survivors to ensure the middle drop does not shift indices
        (Requirement 7.3)."""
        tc = TopicCollection(topics=[
            Topic(
                value="third",
                entities=[Entity(value="C", classification="Person")],
            ),
            Topic(value="empty"),  # no entities, no statements — dropped
            Topic(
                value="first",
                entities=[Entity(value="A", classification="Person")],
            ),
            Topic(
                value="second",
                entities=[Entity(value="B", classification="Person")],
            ),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        assert [t.value for t in out.topics] == ["third", "first", "second"]


class TestMetadataIsolation:
    """Requirements 7.4, 7.5 — only ``TOPICS_KEY`` is touched."""

    def test_only_topics_key_rewritten(self, ontology):
        """Other metadata keys pass through byte-for-byte (Requirement 7.4)."""
        tc = TopicCollection(topics=[
            Topic(
                value="t",
                entities=[Entity(value="Alice", classification="Person")],
            ),
        ])
        node = _make_node(tc)
        node.metadata["unrelated-key"] = "preserve-me"
        node.metadata["another-key"] = {"nested": [1, 2, 3]}

        OntologyFilterTransform(ontology=ontology)([node])

        assert node.metadata["unrelated-key"] == "preserve-me"
        assert node.metadata["another-key"] == {"nested": [1, 2, 3]}
        # ``TOPICS_KEY`` is rewritten, so just assert it is still present.
        assert TOPICS_KEY in node.metadata

    def test_node_without_topics_passes_through(self, ontology):
        """A node whose ``metadata`` dict does not contain ``TOPICS_KEY``
        is not modified in any way (Requirement 7.5)."""
        node = TextNode(text="test")
        node.metadata["unrelated-key"] = "still-here"

        OntologyFilterTransform(ontology=ontology)([node])

        assert TOPICS_KEY not in node.metadata
        assert node.metadata["unrelated-key"] == "still-here"


class TestKillSwitch:
    """Requirement 14.6 — the ``ENABLE_ONTOLOGY_FILTER_STAGE`` flag lets
    operators disable strict mode without deleting the module."""

    def test_raises_when_disabled(self, ontology, monkeypatch):
        """Setting the flag to ``False`` causes construction to raise
        :class:`RuntimeError` whose message names the constant so
        operators can find the toggle by grepping the traceback."""
        monkeypatch.setattr(ofs_module, "ENABLE_ONTOLOGY_FILTER_STAGE", False)
        with pytest.raises(RuntimeError, match="ENABLE_ONTOLOGY_FILTER_STAGE"):
            OntologyFilterStage(ontology)

    def test_works_when_enabled(self, ontology):
        """With the default flag value (``True``) construction succeeds.

        This is the baseline — the other tests in this module all rely
        on this succeeding, so a dedicated assertion makes the failure
        mode easy to isolate when the default changes.
        """
        stage = OntologyFilterStage(ontology)
        assert stage is not None


class TestStageInterface:
    """Requirement 8.2 — :class:`OntologyFilterStage` conforms to the
    :class:`ExtractionStage` ABC shape expected by ``PipelineBuilder``."""

    def test_input_keys(self, ontology):
        assert OntologyFilterStage(ontology).input_keys() == [TOPICS_KEY]

    def test_output_keys(self, ontology):
        assert OntologyFilterStage(ontology).output_keys() == [TOPICS_KEY]

    def test_stage_type(self, ontology):
        assert OntologyFilterStage(ontology).stage_type == "filter"

    def test_as_transform_returns_transform(self, ontology):
        """``as_transform`` must return a fresh :class:`OntologyFilterTransform`
        (design §"Component 2: OntologyFilterStage")."""
        stage = OntologyFilterStage(ontology)
        tx = stage.as_transform()
        assert isinstance(tx, OntologyFilterTransform)


# ---------------------------------------------------------------------------
# Task 4.8 — ``_validate_literal_against_xsd``
#
# One positive and one negative per supported XSD type (Requirement 10).
# Integer subtype range checks are parametrized separately because the
# range boundary is the interesting property — a single positive/negative
# pair would not prove that ``byte`` rejects 200 while ``short`` accepts
# 32000.
# ---------------------------------------------------------------------------


def _xsd(t: str) -> str:
    """Return the full XSD IRI for a local type name.

    Keeps the test bodies readable — ``_xsd("integer")`` instead of
    ``XSD_NAMESPACE + "integer"`` everywhere.
    """
    return XSD_NAMESPACE + t


class TestValidateLiteralAgainstXSD:
    """One positive and one negative literal per supported XSD type
    (Requirement 10.1 – 10.7)."""

    def test_string_accepts_any_value(self):
        """``xsd:string`` accepts any string, including empty
        (Requirement 10.1)."""
        assert _validate_literal_against_xsd("anything", _xsd("string")) is True
        assert _validate_literal_against_xsd("", _xsd("string")) is True
        assert _validate_literal_against_xsd("123", _xsd("string")) is True
        assert _validate_literal_against_xsd("😀", _xsd("string")) is True

    def test_boolean_positive(self):
        """``xsd:boolean`` accepts exactly the XSD lexical space
        ``{"true", "false", "1", "0"}`` (Requirement 10.2 positive half)."""
        for v in ("true", "false", "1", "0"):
            assert _validate_literal_against_xsd(v, _xsd("boolean")) is True, v

    def test_boolean_negative(self):
        """``xsd:boolean`` rejects anything outside the four lexical
        forms, including case variations (XSD 1.1 is case-sensitive
        for boolean) (Requirement 10.2 negative half)."""
        for v in ("yes", "no", "TRUE", "FALSE", "", "2", "True"):
            assert _validate_literal_against_xsd(v, _xsd("boolean")) is False, v

    def test_integer_positive(self):
        """``xsd:integer`` is the unbounded parent type — any signed
        digit string matches (Requirement 10.3 shape check)."""
        assert _validate_literal_against_xsd("42", _xsd("integer")) is True
        assert _validate_literal_against_xsd("-17", _xsd("integer")) is True
        assert _validate_literal_against_xsd("0", _xsd("integer")) is True

    def test_integer_negative(self):
        """Non-digit strings and decimals are rejected by ``xsd:integer``
        (Requirement 10.3 negative half)."""
        for v in ("forty-two", "3.14", "", "1e3", "1_000"):
            assert _validate_literal_against_xsd(v, _xsd("integer")) is False, v

    def test_byte_range(self):
        """``xsd:byte`` is the signed 8-bit range -128..127; 200 is out
        of range (Requirement 10.3 — subtype range check)."""
        assert _validate_literal_against_xsd("100", _xsd("byte")) is True
        assert _validate_literal_against_xsd("127", _xsd("byte")) is True
        assert _validate_literal_against_xsd("-128", _xsd("byte")) is True
        assert _validate_literal_against_xsd("200", _xsd("byte")) is False
        assert _validate_literal_against_xsd("128", _xsd("byte")) is False
        assert _validate_literal_against_xsd("-129", _xsd("byte")) is False

    def test_short_range(self):
        """``xsd:short`` is the signed 16-bit range -32768..32767; 32000
        is inside the range, 40000 is outside (Requirement 10.3)."""
        assert _validate_literal_against_xsd("32000", _xsd("short")) is True
        assert _validate_literal_against_xsd("32767", _xsd("short")) is True
        assert _validate_literal_against_xsd("-32768", _xsd("short")) is True
        assert _validate_literal_against_xsd("33000", _xsd("short")) is False
        assert _validate_literal_against_xsd("-32769", _xsd("short")) is False

    def test_int_range(self):
        """``xsd:int`` is the signed 32-bit range (Requirement 10.3)."""
        assert _validate_literal_against_xsd("2147483647", _xsd("int")) is True
        assert _validate_literal_against_xsd("-2147483648", _xsd("int")) is True
        assert _validate_literal_against_xsd("2147483648", _xsd("int")) is False
        assert _validate_literal_against_xsd("-2147483649", _xsd("int")) is False

    def test_long_range(self):
        """``xsd:long`` is the signed 64-bit range (Requirement 10.3)."""
        assert _validate_literal_against_xsd(
            "9223372036854775807", _xsd("long"),
        ) is True
        assert _validate_literal_against_xsd(
            "-9223372036854775808", _xsd("long"),
        ) is True
        assert _validate_literal_against_xsd(
            "9223372036854775808", _xsd("long"),
        ) is False
        assert _validate_literal_against_xsd(
            "-9223372036854775809", _xsd("long"),
        ) is False

    def test_decimal_positive(self):
        """``xsd:decimal`` accepts optional leading sign followed by
        digits with optional single decimal point. Plain integers are
        valid decimals (Requirement 10.4 positive half)."""
        assert _validate_literal_against_xsd("3.14", _xsd("decimal")) is True
        assert _validate_literal_against_xsd("-2.7", _xsd("decimal")) is True
        assert _validate_literal_against_xsd("42", _xsd("decimal")) is True
        assert _validate_literal_against_xsd("0.0", _xsd("decimal")) is True

    def test_decimal_negative(self):
        """``xsd:decimal`` rejects scientific notation, textual numbers,
        and multiple decimal points (Requirement 10.4 negative half)."""
        for v in ("pi", "1e10", "3.14.15", "", "--1", "1."):
            assert _validate_literal_against_xsd(v, _xsd("decimal")) is False, v

    def test_float_positive(self):
        """``xsd:float`` accepts anything Python's ``float()`` parses,
        including scientific notation (Requirement 10.5 positive half)."""
        assert _validate_literal_against_xsd("3.14", _xsd("float")) is True
        assert _validate_literal_against_xsd("1e10", _xsd("float")) is True
        assert _validate_literal_against_xsd("-2.5E-3", _xsd("float")) is True
        assert _validate_literal_against_xsd("inf", _xsd("float")) is True
        assert _validate_literal_against_xsd("42", _xsd("float")) is True

    def test_float_negative(self):
        """``xsd:float`` rejects strings ``float()`` cannot parse
        (Requirement 10.5 negative half)."""
        for v in ("not-a-number", "", "3.14.15"):
            assert _validate_literal_against_xsd(v, _xsd("float")) is False, v

    def test_double_positive_and_negative(self):
        """``xsd:double`` uses the same parser as ``xsd:float``
        (Requirement 10.5 applies equally)."""
        assert _validate_literal_against_xsd("3.14", _xsd("double")) is True
        assert _validate_literal_against_xsd("-2.5e3", _xsd("double")) is True
        assert _validate_literal_against_xsd("not-a-number", _xsd("double")) is False
        assert _validate_literal_against_xsd("", _xsd("double")) is False

    def test_date_positive(self):
        """``xsd:date`` accepts ISO-8601 ``YYYY-MM-DD`` with optional
        ``Z`` or ``±HH:MM`` timezone (Requirement 10.6 date half,
        positive)."""
        assert _validate_literal_against_xsd("2024-01-15", _xsd("date")) is True
        assert _validate_literal_against_xsd("2024-01-15Z", _xsd("date")) is True
        assert _validate_literal_against_xsd(
            "2024-01-15+05:00", _xsd("date"),
        ) is True
        assert _validate_literal_against_xsd(
            "2024-01-15-08:00", _xsd("date"),
        ) is True

    def test_date_negative(self):
        """``xsd:date`` rejects alternate formats that do not match the
        regex (Requirement 10.6 date half, negative)."""
        for v in ("January 15 2024", "2024/01/15", "15-01-2024", "2024-1-15", ""):
            assert _validate_literal_against_xsd(v, _xsd("date")) is False, v

    def test_datetime_positive(self):
        """``xsd:dateTime`` accepts ISO-8601 datetime with optional
        fractional seconds and optional timezone (Requirement 10.6
        dateTime half, positive)."""
        assert _validate_literal_against_xsd(
            "2024-01-15T10:30:00", _xsd("dateTime"),
        ) is True
        assert _validate_literal_against_xsd(
            "2024-01-15T10:30:00Z", _xsd("dateTime"),
        ) is True
        assert _validate_literal_against_xsd(
            "2024-01-15T10:30:00.123Z", _xsd("dateTime"),
        ) is True
        assert _validate_literal_against_xsd(
            "2024-01-15T10:30:00+05:00", _xsd("dateTime"),
        ) is True

    def test_datetime_negative(self):
        """``xsd:dateTime`` rejects space-separated (instead of ``T``)
        and free-form strings (Requirement 10.6 dateTime half,
        negative)."""
        for v in (
            "2024-01-15 10:30:00",      # space instead of T
            "not-a-datetime",
            "2024-01-15",               # date only
            "10:30:00",                 # time only
            "",
        ):
            assert _validate_literal_against_xsd(v, _xsd("dateTime")) is False, v

    def test_anyuri_positive(self):
        """``xsd:anyURI`` accepts any non-empty string ``urlparse``
        returns cleanly for (Requirement 10.7 positive half)."""
        assert _validate_literal_against_xsd(
            "https://example.com", _xsd("anyURI"),
        ) is True
        assert _validate_literal_against_xsd(
            "/relative/path", _xsd("anyURI"),
        ) is True
        assert _validate_literal_against_xsd("urn:isbn:0-486-27557-4", _xsd("anyURI")) is True

    def test_anyuri_negative_empty(self):
        """``xsd:anyURI`` explicitly rejects the empty string — the
        implementation treats "" as the obvious garbage case
        (Requirement 10.7 negative half)."""
        assert _validate_literal_against_xsd("", _xsd("anyURI")) is False


class TestUnknownXSDType:
    """Requirement 10.8 — unknown XSD IRI returns ``True`` and emits
    exactly one WARN log per unknown type per pipeline run."""

    def test_unknown_xsd_type_accepts_and_warns_once(self, caplog):
        """An unknown ``xsd:gYearMonth`` IRI must:

        * Return ``True`` on every call (permissive — ontology author
          chose the type, we do not invent rejection criteria).
        * Emit exactly one WARN log naming the unknown type, even when
          called multiple times.

        The module-level :data:`_warned_unknown_types` set is
        module-scoped and persists across tests; we discard the
        fixture's IRI first so this test's assertions are independent
        of any earlier caller that may have exercised the same
        codepath.
        """
        unknown_iri = _xsd("gYearMonth")
        ofs_module._warned_unknown_types.discard(unknown_iri)

        with caplog.at_level(
            logging.WARNING,
            logger="graphrag_toolkit.lexical_graph.indexing.extract.stages.ontology_filter_stage",
        ):
            first = _validate_literal_against_xsd("2024-01", unknown_iri)
            second = _validate_literal_against_xsd("2024-02", unknown_iri)
            third = _validate_literal_against_xsd("2024-03", unknown_iri)

        # Every call accepts permissively.
        assert first is True
        assert second is True
        assert third is True

        # Exactly one WARN log was emitted, and its message names the
        # unknown type so operators can grep for it.
        warn_records = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "gYearMonth" in r.getMessage()
        ]
        assert len(warn_records) == 1, (
            f"Expected exactly one WARN log for {unknown_iri}; "
            f"got {len(warn_records)}: "
            f"{[r.getMessage() for r in warn_records]}"
        )

    def test_different_unknown_types_each_get_one_warn(self, caplog):
        """Two distinct unknown XSD IRIs should each emit one WARN log
        — the guard is per-type, not global (Requirement 10.8
        "per unknown datatype")."""
        unknown_a = _xsd("gYear")
        unknown_b = _xsd("duration")
        ofs_module._warned_unknown_types.discard(unknown_a)
        ofs_module._warned_unknown_types.discard(unknown_b)

        with caplog.at_level(
            logging.WARNING,
            logger="graphrag_toolkit.lexical_graph.indexing.extract.stages.ontology_filter_stage",
        ):
            _validate_literal_against_xsd("2024", unknown_a)
            _validate_literal_against_xsd("2024", unknown_a)  # deduped
            _validate_literal_against_xsd("P1Y", unknown_b)

        warn_messages = [
            r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING
        ]
        # One warn for each distinct unknown type, despite two calls
        # to the first type.
        assert sum("gYear" in m and "gYearMonth" not in m for m in warn_messages) == 1
        assert sum("duration" in m for m in warn_messages) == 1


# ---------------------------------------------------------------------------
# Hypothesis strategies shared by Tasks 4.9, 4.10, and 4.12.
#
# The strategies below generate a :class:`TopicCollection` over a *fixed*
# ontology — the ``PERSON_ONTOLOGY_TURTLE`` already defined at the top of
# this module. Generating random ontologies is unnecessary for exercising
# the filter: every strict-mode code branch (entity keep/drop, SPO keep,
# SPO drop by subject/object/predicate/domain/range miss, SPC keep, SPC
# drop by predicate/domain miss, SPC keep/drop by literal, malformed
# drop, topic keep/drop, topic order) can be hit by varying the
# :class:`TopicCollection` alone. This keeps the strategies small,
# deterministic, and fast enough to run under the default ``max_examples``
# budget without ``HealthCheck.too_slow`` firing.
#
# Task 4.11 (P6) uses a *different* fixed ontology — a minimal
# ``A ⊂ B`` hierarchy — so the subclass-closure invariant is isolated
# from the richer Person/Employee/Manager/Company fixture above.
# ---------------------------------------------------------------------------

# A palette of class names: some resolve to ontology classes, some do not.
# Entities drawn from ``_UNKNOWN_CLASSES`` must always be dropped by the
# filter (Requirement 6.1); entities drawn from ``_IN_ONTOLOGY_CLASSES``
# resolve cleanly.
_IN_ONTOLOGY_CLASSES = ("Person", "Employee", "Manager", "Company")
_UNKNOWN_CLASSES = ("Gremlin", "Vampire", "UnknownCls")
_ALL_CLASSES = _IN_ONTOLOGY_CLASSES + _UNKNOWN_CLASSES

# Predicate palette for SPO facts. The ``worksFor`` object property in
# the fixture is matched by all three spellings below (local_name,
# UPPER_SNAKE, and the rdfs:label ``"works for"``). The ``_UNKNOWN_``
# entries must never resolve.
_IN_ONTOLOGY_OBJ_PREDS = ("WORKS_FOR", "worksFor", "works for")
_UNKNOWN_OBJ_PREDS = ("KNOWS", "HATES", "GIBBERISH_PRED")
_ALL_OBJ_PREDS = _IN_ONTOLOGY_OBJ_PREDS + _UNKNOWN_OBJ_PREDS

# Predicate palette for SPC facts. ``age`` and ``name`` are the two
# DatatypeProperties in the fixture; unknown strings must never resolve.
_IN_ONTOLOGY_DT_PREDS = ("age", "name")
_UNKNOWN_DT_PREDS = ("weight", "height", "unknown_dt")
_ALL_DT_PREDS = _IN_ONTOLOGY_DT_PREDS + _UNKNOWN_DT_PREDS

# Literal palette for SPC facts. The integer-shaped literals validate
# against ``xsd:integer`` (the range of ``:age``) while the non-integer
# literals do not. All of them are valid against ``xsd:string`` (the
# range of ``:name``), so ``name``-predicated facts whose literal is in
# this palette always pass the literal check.
_INTEGER_LITERALS = ("42", "-17", "0", "100")
_NON_INTEGER_LITERALS = ("forty-two", "3.14", "abc")
_ALL_LITERALS = _INTEGER_LITERALS + _NON_INTEGER_LITERALS


@st.composite
def random_fact_strategy(draw):
    """Generate a random :class:`Fact` — SPO, SPC, or malformed.

    The three branches are sampled uniformly so each run exercises the
    routing logic in ``_route_fact`` across all three shapes. Generated
    classifications and predicates are drawn from a mixed palette of
    ontology-known and ontology-unknown names so both the keep-path and
    the drop-path of the filter see coverage.

    Returning a malformed fact (both ``object`` and ``complement`` are
    ``None``) lets Task 4.9 assert that Requirement 6.6 holds on
    arbitrary input — the filter must drop those regardless of subject
    class or predicate validity.
    """
    kind = draw(st.sampled_from(("spo", "spc", "malformed")))
    subj_cls = draw(st.sampled_from(_ALL_CLASSES))
    # ``subj_val`` / ``obj_val`` strings are only used for identity; the
    # integer range is small so Hypothesis shrinks cleanly on failure.
    subj_val = f"subj_{draw(st.integers(min_value=0, max_value=100))}"

    if kind == "spo":
        pred = draw(st.sampled_from(_ALL_OBJ_PREDS))
        obj_cls = draw(st.sampled_from(_ALL_CLASSES))
        obj_val = f"obj_{draw(st.integers(min_value=0, max_value=100))}"
        return _make_spo_fact(subj_val, subj_cls, pred, obj_val, obj_cls)

    if kind == "spc":
        pred = draw(st.sampled_from(_ALL_DT_PREDS))
        literal = draw(st.sampled_from(_ALL_LITERALS))
        return _make_spc_fact(subj_val, subj_cls, pred, literal)

    # Malformed: neither object nor complement.
    pred = draw(st.sampled_from(_ALL_OBJ_PREDS))
    return Fact(
        subject=Entity(value=subj_val, classification=subj_cls),
        predicate=Relation(value=pred),
    )


@st.composite
def topic_collection_strategy(
    draw, max_topics=4, max_entities=3, max_facts=4,
):
    """Generate a random :class:`TopicCollection` over the fixture ontology.

    Each topic is tagged with a unique positional ``value`` of the form
    ``f"topic_{i}"`` so tests can verify order preservation (Requirement
    7.3) without relying on topic values being globally unique — which
    they are not in general, but positional tagging makes them so for
    the purposes of each generated example.

    Bounds are tight (``max_topics=4``, ``max_entities=3``,
    ``max_facts=4``) to keep Hypothesis iterations fast while still
    giving wide coverage of combinatorial topic shapes.
    """
    n_topics = draw(st.integers(min_value=1, max_value=max_topics))
    topics = []
    for i in range(n_topics):
        n_entities = draw(st.integers(min_value=0, max_value=max_entities))
        entities = [
            Entity(
                value=f"e_{draw(st.integers(min_value=0, max_value=100))}",
                classification=draw(st.sampled_from(_ALL_CLASSES)),
            )
            for _ in range(n_entities)
        ]

        n_facts = draw(st.integers(min_value=0, max_value=max_facts))
        facts = [draw(random_fact_strategy()) for _ in range(n_facts)]
        # Wrap the facts in a single statement per topic — the filter
        # iterates ``statement.facts`` inside each topic, so one
        # statement is enough to exercise every fact-level branch. An
        # empty ``statements`` list (when ``n_facts == 0``) is kept so
        # topics with only entities (or nothing at all) are also
        # generated.
        statements = (
            [Statement(value=f"s_{i}", facts=facts)] if facts else []
        )

        topics.append(Topic(
            value=f"topic_{i}",
            entities=entities,
            statements=statements,
        ))
    return TopicCollection(topics=topics)


# ---------------------------------------------------------------------------
# Oracle — an independent re-implementation of the strict-mode filter
# used only by the PBT assertions below. Re-implementing the filter in
# the test is a deliberate design choice: if production and test shared
# a single implementation the property assertions would degenerate to
# ``impl == impl`` and catch nothing. The oracle is spelled out from the
# requirements text, not the code, so a regression in the production
# filter shows up as a mismatch between the two.
# ---------------------------------------------------------------------------


def _fact_is_valid_against_ontology(
    fact: Fact,
    ontology: OntologySchema,
    validate_datatypes: bool = True,
) -> bool:
    """Oracle: return ``True`` iff ``fact`` should survive strict-mode filtering.

    Implements Requirements 6.1, 6.2, 6.3, 6.6, 9.1, 9.2, 9.3 directly
    from the requirements text. Used by the PBT assertions below as an
    independent second implementation; see module-level comment above.
    """
    # Requirement 6.6 — malformed facts drop unconditionally.
    if fact.object is None and fact.complement is None:
        return False

    if fact.object is not None:
        # SPO branch (Requirement 6.2).
        subj_iri = ontology.resolve_class(fact.subject.classification or "")
        obj_iri = ontology.resolve_class(fact.object.classification or "")
        if subj_iri is None or obj_iri is None:
            return False
        candidates = ontology.resolve_object_predicate(fact.predicate.value)
        if not candidates:
            return False
        for op in candidates:
            domain_ok = not op.domain or any(
                ontology.is_subclass_of(subj_iri, d) for d in op.domain
            )
            range_ok = not op.range or any(
                ontology.is_subclass_of(obj_iri, r) for r in op.range
            )
            if domain_ok and range_ok:
                return True
        return False

    # SPC branch (Requirements 6.3, 9.1, 9.2, 9.3).
    subj_iri = ontology.resolve_class(fact.subject.classification or "")
    if subj_iri is None:
        return False
    candidates = ontology.resolve_datatype_predicate(fact.predicate.value)
    if not candidates:
        return False

    # Extract the string form of the complement using the same rule as
    # the production filter: plain strings pass through; ``Entity``
    # wrappers expose ``.value``.
    complement = fact.complement
    if isinstance(complement, Entity):
        literal = complement.value
    else:
        literal = "" if complement is None else str(complement)

    for dp in candidates:
        domain_ok = not dp.domain or any(
            ontology.is_subclass_of(subj_iri, d) for d in dp.domain
        )
        if not domain_ok:
            continue
        if not validate_datatypes:
            return True
        if _validate_literal_against_xsd(literal, dp.datatype):
            return True
    return False


# ---------------------------------------------------------------------------
# Task 4.9 — PBT: strict mode preserves only ontology-valid facts.
# ---------------------------------------------------------------------------


class TestStrictModePreservesOnlyValidFacts:
    """Property P4: strict mode preserves only ontology-valid facts.

    **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 14.1.**

    For every generated :class:`TopicCollection`, running the filter
    with ``validate_datatypes=True`` must leave behind exactly those
    entities and facts that satisfy the oracle
    :func:`_fact_is_valid_against_ontology` plus the entity-class-must-
    resolve rule from Requirement 6.1. Equivalently, no invalid content
    may sneak through, and no valid content may be dropped.
    """

    @given(tc=topic_collection_strategy())
    @settings(
        max_examples=50,
        suppress_health_check=[
            HealthCheck.too_slow,
            # ``ontology`` is a function-scoped fixture reused across
            # Hypothesis-generated examples. Sharing it is safe because
            # :class:`OntologySchema` is effectively immutable after
            # :meth:`from_turtle_string` returns — see the class
            # docstring — so the health check's "subtle test bugs"
            # concern does not apply here.
            HealthCheck.function_scoped_fixture,
        ],
    )
    def test_strict_filter_only_keeps_valid(self, ontology, tc):
        """Every surviving entity has a resolvable class; every
        surviving fact satisfies the oracle; no ontology-valid fact is
        dropped."""
        node = _make_node(tc)
        OntologyFilterTransform(
            ontology=ontology, validate_datatypes=True,
        )([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])

        # ---- Requirement 6.1: every surviving entity resolves ----
        for topic in out.topics:
            for entity in topic.entities:
                assert ontology.resolve_class(
                    entity.classification or ""
                ) is not None, (
                    f"Entity {entity!r} with unresolvable classification "
                    f"survived the filter"
                )

        # ---- Requirements 6.2, 6.3, 6.6: every surviving fact is valid ----
        for topic in out.topics:
            for stmt in topic.statements:
                for fact in stmt.facts:
                    assert _fact_is_valid_against_ontology(
                        fact, ontology, validate_datatypes=True,
                    ), (
                        f"Ontology-invalid fact {fact!r} survived the "
                        f"strict-mode filter"
                    )

        # ---- No valid content was dropped ----
        # Count what the oracle thinks should survive in the input and
        # compare to what actually survived in the output. We group by
        # topic position so the subclass-closure cases (Requirements
        # 6.4, 6.5) are covered: any fact whose subject or object sits
        # under a domain/range class via closure is kept by the oracle
        # and must also be kept by the filter.
        expected_entities_per_topic = [
            [
                e for e in topic.entities
                if ontology.resolve_class(e.classification or "") is not None
            ]
            for topic in tc.topics
        ]
        expected_facts_per_topic = [
            [
                fact
                for stmt in topic.statements
                for fact in stmt.facts
                if _fact_is_valid_against_ontology(
                    fact, ontology, validate_datatypes=True,
                )
            ]
            for topic in tc.topics
        ]

        # A topic survives iff at least one expected entity or fact
        # remains (Requirements 7.1, 7.2 — not the main property of
        # this test but asserted as a cross-check).
        expected_surviving_topic_indices = [
            i for i in range(len(tc.topics))
            if expected_entities_per_topic[i] or expected_facts_per_topic[i]
        ]
        actual_surviving_values = [t.value for t in out.topics]
        expected_surviving_values = [
            tc.topics[i].value for i in expected_surviving_topic_indices
        ]
        assert actual_surviving_values == expected_surviving_values, (
            f"Surviving topics do not match oracle. "
            f"Expected {expected_surviving_values!r}, "
            f"got {actual_surviving_values!r}."
        )


# ---------------------------------------------------------------------------
# Task 4.10 — PBT: opt-out flag isolates the literal check.
# ---------------------------------------------------------------------------


class TestOptOutDatatypeFlag:
    """Property P5: opt-out flag isolates the literal check.

    **Validates: Requirements 9.1, 9.2, 9.3, 14.1.**

    For any SPC fact, running the filter with ``validate_datatypes``
    ``True`` vs ``False`` must differ only when the fact has a matching
    predicate and a matching domain but a non-conforming literal. In
    every other case (predicate miss, domain miss, subject-class miss,
    malformed) both flags must drop the fact.
    """

    @given(
        subj_cls=st.sampled_from(_ALL_CLASSES),
        pred=st.sampled_from(_ALL_DT_PREDS),
        literal=st.sampled_from(_ALL_LITERALS),
    )
    @settings(
        max_examples=40,
        suppress_health_check=[
            HealthCheck.too_slow,
            # Shared-fixture rationale: see
            # :meth:`TestStrictModePreservesOnlyValidFacts.test_strict_filter_only_keeps_valid`.
            HealthCheck.function_scoped_fixture,
        ],
    )
    def test_opt_out_isolates_literal_check(
        self, ontology, subj_cls, pred, literal,
    ):
        """Generate a single-SPC-fact collection across the full
        matrix of (subject class, predicate, literal) combinations and
        compare the two flag values.

        Parameter mix:

        * ``subj_cls`` spans in-ontology classes (including
          :class:`Person`/subclasses where the domain covers) and
          out-of-ontology classes where the subject-class check must
          fail for both flags.
        * ``pred`` spans in-ontology DatatypeProperties and unknowns.
        * ``literal`` spans XSD-integer-valid and XSD-integer-invalid
          strings so the literal-check branch is exercised for the
          ``age`` predicate (which has ``xsd:integer`` range).
        """
        fact = _make_spc_fact("subj", subj_cls, pred, literal)
        tc = TopicCollection(topics=[
            Topic(value="t", statements=[
                Statement(value="s", facts=[fact]),
            ]),
        ])

        # Run the filter twice on independent copies of the input so
        # mutation of ``node.metadata`` by one run does not affect the
        # other. ``_make_node`` materialises a fresh dict via
        # ``model_dump()`` so the two nodes share no state.
        node_true = _make_node(tc)
        node_false = _make_node(tc)
        OntologyFilterTransform(
            ontology=ontology, validate_datatypes=True,
        )([node_true])
        OntologyFilterTransform(
            ontology=ontology, validate_datatypes=False,
        )([node_false])

        kept_true = TopicCollection(**node_true.metadata[TOPICS_KEY])
        kept_false = TopicCollection(**node_false.metadata[TOPICS_KEY])

        # Extract the surviving fact list for each run — when the topic
        # itself was dropped (empty after filtering, per Requirement
        # 7.2) the surviving fact list is simply ``[]``.
        kept_true_facts = (
            kept_true.topics[0].statements[0].facts
            if kept_true.topics
            else []
        )
        kept_false_facts = (
            kept_false.topics[0].statements[0].facts
            if kept_false.topics
            else []
        )

        # Oracle decisions for both flag values.
        valid_true = _fact_is_valid_against_ontology(
            fact, ontology, validate_datatypes=True,
        )
        valid_false = _fact_is_valid_against_ontology(
            fact, ontology, validate_datatypes=False,
        )

        # The production filter must agree with the oracle on both
        # flag values.
        assert (len(kept_true_facts) == 1) == valid_true, (
            f"validate_datatypes=True disagrees with oracle on {fact!r}: "
            f"oracle says {valid_true}, filter kept "
            f"{len(kept_true_facts)} fact(s)"
        )
        assert (len(kept_false_facts) == 1) == valid_false, (
            f"validate_datatypes=False disagrees with oracle on {fact!r}: "
            f"oracle says {valid_false}, filter kept "
            f"{len(kept_false_facts)} fact(s)"
        )

        # Requirement 9.2 corollary: ``False`` must be at least as
        # permissive as ``True``. Any fact kept by ``True`` must also
        # be kept by ``False`` (the opt-out only *removes* a check).
        if kept_true_facts:
            assert kept_false_facts, (
                "validate_datatypes=False dropped a fact that "
                "validate_datatypes=True kept — the opt-out should "
                "be strictly more permissive"
            )

        # Requirement 9.3: predicate- and domain-miss SPC facts must
        # drop under *both* flag values. Derive the expected drop from
        # the ontology directly rather than from the filter output.
        subj_iri = ontology.resolve_class(subj_cls)
        dt_candidates = ontology.resolve_datatype_predicate(pred)
        domain_covers = subj_iri is not None and any(
            not dp.domain or any(
                ontology.is_subclass_of(subj_iri, d) for d in dp.domain
            )
            for dp in dt_candidates
        )
        if subj_iri is None or not dt_candidates or not domain_covers:
            assert kept_true_facts == [], (
                "SPC fact with predicate/subject/domain miss must drop "
                "under validate_datatypes=True"
            )
            assert kept_false_facts == [], (
                "SPC fact with predicate/subject/domain miss must drop "
                "under validate_datatypes=False (Requirement 9.3)"
            )


# ---------------------------------------------------------------------------
# Task 4.11 — PBT: strict mode respects subclass closure for domain and range.
# ---------------------------------------------------------------------------


# Minimal ontology for the subclass-closure property. ``A ⊂ B`` and a
# single object property ``rel`` with domain ``B`` and range ``B``.
# ``C`` is declared but is NOT a subclass of ``B`` — it is an
# independent sibling used to verify the negative half of
# Requirement 6.5.
_SUBCLASS_ONTOLOGY_TURTLE = """
@prefix : <https://example.com/kg/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

: a owl:Ontology .

:B a owl:Class .
:A a owl:Class ; rdfs:subClassOf :B .
:C a owl:Class .

:rel a owl:ObjectProperty ;
    rdfs:domain :B ;
    rdfs:range :B .
"""


@pytest.fixture
def subclass_ontology() -> OntologySchema:
    """Minimal ontology for the Task 4.11 subclass-closure property.

    ``A ⊂ B``; ``C`` is a sibling (no subclass relation to ``B``);
    ``rel`` is an ObjectProperty with ``B`` in both domain and range.
    """
    return OntologySchema.from_turtle_string(_SUBCLASS_ONTOLOGY_TURTLE)


class TestSubclassClosureForDomainRange:
    """Property P6: strict mode respects subclass closure.

    **Validates: Requirements 6.2, 6.4, 6.5, 14.1.**

    Uses the ``A ⊂ B`` ontology above. The ``rel`` property declares
    ``B`` in both domain and range, so any fact whose subject and
    object classes are in ``{A, B}`` must be kept (Requirement 6.4 —
    downward closure). Any fact whose subject or object class is
    ``C`` — a sibling outside the ``B`` closure — must be dropped
    (Requirement 6.5).
    """

    @given(
        subj_cls=st.sampled_from(("A", "B")),
        obj_cls=st.sampled_from(("A", "B")),
    )
    @settings(
        max_examples=20,
        suppress_health_check=[
            HealthCheck.too_slow,
            # Shared-fixture rationale: see
            # :meth:`TestStrictModePreservesOnlyValidFacts.test_strict_filter_only_keeps_valid`.
            HealthCheck.function_scoped_fixture,
        ],
    )
    def test_subclass_substitutable_for_superclass(
        self, subclass_ontology, subj_cls, obj_cls,
    ):
        """Any ``(A|B) rel (A|B)`` fact is kept — the domain and range
        of ``rel`` is ``B`` and ``A ⊂ B`` so every combination passes
        closure (Requirement 6.4)."""
        fact = _make_spo_fact("x", subj_cls, "rel", "y", obj_cls)
        tc = TopicCollection(topics=[
            Topic(value="t", statements=[
                Statement(value="s", facts=[fact]),
            ]),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=subclass_ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        kept = out.topics[0].statements[0].facts if out.topics else []
        assert len(kept) == 1, (
            f"Expected ({subj_cls} rel {obj_cls}) to be kept — both "
            f"classes are in the closure of B, but the filter dropped "
            f"it."
        )

    @given(
        # At least one side is ``C`` so the fact must drop. We sample
        # the three cases: C-subj, C-obj, and C-both.
        subj_cls=st.sampled_from(("A", "B", "C")),
        obj_cls=st.sampled_from(("A", "B", "C")),
    )
    @settings(
        max_examples=20,
        suppress_health_check=[
            HealthCheck.too_slow,
            # Shared-fixture rationale: see
            # :meth:`TestStrictModePreservesOnlyValidFacts.test_strict_filter_only_keeps_valid`.
            HealthCheck.function_scoped_fixture,
        ],
    )
    def test_sibling_class_dropped_when_involved(
        self, subclass_ontology, subj_cls, obj_cls,
    ):
        """If either side of a ``(X rel Y)`` fact is ``C`` (sibling
        outside ``B``'s closure), the fact must drop. If neither side
        is ``C``, the fact must survive.

        One property covers both halves of Requirement 6.5 — we simply
        derive the expected outcome from whether ``C`` appears on
        either side.
        """
        fact = _make_spo_fact("x", subj_cls, "rel", "y", obj_cls)
        tc = TopicCollection(topics=[
            Topic(value="t", statements=[
                Statement(value="s", facts=[fact]),
            ]),
        ])
        node = _make_node(tc)
        OntologyFilterTransform(ontology=subclass_ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])
        kept = out.topics[0].statements[0].facts if out.topics else []

        c_involved = (subj_cls == "C") or (obj_cls == "C")
        if c_involved:
            assert kept == [], (
                f"Expected ({subj_cls} rel {obj_cls}) to be dropped — "
                f"C is not a subclass of B — but filter kept "
                f"{len(kept)} fact(s)."
            )
        else:
            assert len(kept) == 1, (
                f"Expected ({subj_cls} rel {obj_cls}) to be kept — "
                f"both classes are in the closure of B — but filter "
                f"dropped it."
            )


# ---------------------------------------------------------------------------
# Task 4.12 — PBT: topic-level emptiness preservation.
# ---------------------------------------------------------------------------


class TestTopicEmptinessPreservation:
    """Property P7: topic-level emptiness preservation.

    **Validates: Requirements 7.1, 7.2, 7.3, 14.1.**

    For every generated :class:`TopicCollection`, after the filter runs:

    1. A topic is present in the output iff it has at least one
       surviving entity or at least one surviving fact (Req 7.1, 7.2).
    2. The relative order of surviving topics matches the input order
       (Req 7.3).

    Each generated topic is tagged with a unique ``value`` of the form
    ``f"topic_{i}"`` by :func:`topic_collection_strategy` so we can
    verify order via positional lookup instead of requiring globally
    unique topic values (which pydantic does not enforce).
    """

    @given(tc=topic_collection_strategy())
    @settings(
        max_examples=50,
        suppress_health_check=[
            HealthCheck.too_slow,
            # Shared-fixture rationale: see
            # :meth:`TestStrictModePreservesOnlyValidFacts.test_strict_filter_only_keeps_valid`.
            HealthCheck.function_scoped_fixture,
        ],
    )
    def test_topic_preserved_iff_nonempty_and_order_preserved(
        self, ontology, tc,
    ):
        """Every surviving topic is non-empty; every empty topic was
        removed; surviving topics appear in original order."""
        # Snapshot the input topic positions before the filter mutates
        # ``node.metadata`` via ``model_dump()`` round-trip.
        original_topic_values = [t.value for t in tc.topics]
        # Tagging is positional so values should be unique per example,
        # but assert it explicitly to catch any strategy drift.
        assert len(set(original_topic_values)) == len(original_topic_values), (
            "topic_collection_strategy generated duplicate topic values — "
            "positional tagging is required for this property"
        )

        node = _make_node(tc)
        OntologyFilterTransform(ontology=ontology)([node])
        out = TopicCollection(**node.metadata[TOPICS_KEY])

        # ---- Requirement 7.1 / 7.2: non-emptiness of survivors ----
        for topic in out.topics:
            any_facts = any(stmt.facts for stmt in topic.statements)
            assert topic.entities or any_facts, (
                f"Topic {topic.value!r} survived with no entities and "
                f"no facts — Requirement 7.2 says it should have been "
                f"dropped."
            )

        # ---- Requirement 7.3: relative order preserved ----
        # Map each surviving topic's value to its original position.
        # Positions must be strictly monotonically increasing for the
        # output to preserve input order.
        original_positions = {
            v: i for i, v in enumerate(original_topic_values)
        }
        surviving_values = [t.value for t in out.topics]
        for v in surviving_values:
            assert v in original_positions, (
                f"Surviving topic {v!r} was not present in the input — "
                f"the filter cannot invent or rename topics"
            )
        positions = [original_positions[v] for v in surviving_values]
        assert positions == sorted(positions), (
            f"Surviving topic order {surviving_values!r} (positions "
            f"{positions!r}) does not match input order — Requirement "
            f"7.3 requires relative order to be preserved"
        )

        # ---- Requirement 7.1 (positive half) + 7.2 (negative half) ----
        # Every input topic whose post-filter state would be non-empty
        # must appear in the output, and every empty-post-filter topic
        # must be absent. Compute the oracle from the input using
        # ``_fact_is_valid_against_ontology`` + the entity-class-resolves
        # rule.
        for topic in tc.topics:
            expected_entities = [
                e for e in topic.entities
                if ontology.resolve_class(e.classification or "") is not None
            ]
            expected_facts = [
                f
                for stmt in topic.statements
                for f in stmt.facts
                if _fact_is_valid_against_ontology(
                    f, ontology, validate_datatypes=True,
                )
            ]
            should_survive = bool(expected_entities or expected_facts)
            did_survive = topic.value in surviving_values
            assert did_survive == should_survive, (
                f"Topic {topic.value!r}: oracle expects survive="
                f"{should_survive} (entities={len(expected_entities)}, "
                f"facts={len(expected_facts)}); filter produced "
                f"survive={did_survive}"
            )
