# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for ontology-guided extraction Turtle loading (suggestion-mode
surface only).

Covers Task 2.5 from the ontology-guided-extraction spec:

* Round-trip of the Section 5.1 ``Person``/``Employee``/``Manager``/``Company``
  fixture — class and property counts, labels, parents, domain/range, datatype.
* Reflexivity and transitivity of ``OntologyClass.ancestors`` (and
  ``OntologySchema.is_subclass_of``) across a three-level hierarchy.
* The six load-time error cases from Requirement 3: cyclic ``subClassOf``,
  dangling class references, properties declared as both
  ``owl:ObjectProperty`` and ``owl:DatatypeProperty``,
  ``DatatypeProperty`` with a non-XSD range, ``DatatypeProperty`` with no
  range, and missing base IRI.
* The Requirement 3.6 invariant that ``OntologyLoadError`` never produces a
  partial schema object visible to the caller.

All tests parse in-memory via ``OntologySchema.from_turtle_string`` so they
do not touch the filesystem. A single ``test_load_from_turtle_file`` test
exercises the disk-based ``from_turtle`` entry point via ``tmp_path``.

``rdflib`` is a soft dependency (Requirement 4 / NFR-1). The whole module is
skipped via ``pytest.importorskip`` so that the unit-test suite stays green
in environments where the optional ontology dependency is not installed.
"""

import pytest

# Soft-skip the module when rdflib is not installed — the feature is a soft
# dependency (Requirement 4.2) and the rest of the toolkit must import and
# run without it. importorskip raises ``pytest.skip`` at collection time,
# giving a clean "skipped" entry in the suite summary.
pytest.importorskip("rdflib")
# ``hypothesis`` is a test-only dependency (design §Dependencies). It is
# required by :class:`TestSubclassClosureProperties` at the bottom of this
# module. Rather than guarding just that class, we skip the whole module so
# the collection story is the same as for ``rdflib``: either everything in
# this file runs or everything is reported as skipped.
pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, given, settings, strategies as st

from graphrag_toolkit.lexical_graph.indexing.extract.ontology_schema import (
    OntologyLoadError,
    OntologySchema,
    XSD_NAMESPACE,
)
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_schema import (
    EntityTypeConfig,
    ExtractionSchema,
)


# ---------------------------------------------------------------------------
# Fixture: the Section 5.1 Person/Employee/Manager/Company Turtle document.
#
# This mirrors the shape shown in design §"The emitted prompt text"
# (Person / Employee subclass of Person / Company, worksFor object
# property, Person.age datatype property) plus one extra class —
# ``Manager`` as a subclass of ``Employee`` — which exercises the
# three-level hierarchy assertions called out by Task 2.5.
# ---------------------------------------------------------------------------

PERSON_EMPLOYEE_COMPANY_TURTLE = """
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

:name a owl:DatatypeProperty ;
    rdfs:label "name" ;
    rdfs:domain :Person ;
    rdfs:range xsd:string .

:age a owl:DatatypeProperty ;
    rdfs:label "age" ;
    rdfs:domain :Person ;
    rdfs:range xsd:integer .
"""


# IRIs are materialised as module-level constants so that assertions read as
# ``schema.classes[PERSON_IRI].label == "Person"`` rather than re-quoting the
# full IRI at every call site.
NAMESPACE = "https://example.com/kg/"
PERSON_IRI = NAMESPACE + "Person"
EMPLOYEE_IRI = NAMESPACE + "Employee"
MANAGER_IRI = NAMESPACE + "Manager"
COMPANY_IRI = NAMESPACE + "Company"
WORKS_FOR_IRI = NAMESPACE + "worksFor"
NAME_IRI = NAMESPACE + "name"
AGE_IRI = NAMESPACE + "age"


@pytest.fixture
def schema() -> OntologySchema:
    """Load the Person/Employee/Manager/Company fixture once per test.

    Using a fixture (rather than a module-level constant) lets each test
    mutate the returned instance safely — although the production API
    documents :class:`OntologySchema` as effectively immutable, keeping one
    instance per test makes failures more deterministic.
    """
    return OntologySchema.from_turtle_string(PERSON_EMPLOYEE_COMPANY_TURTLE)


# ---------------------------------------------------------------------------
# Round-trip tests (Requirement 1.1 – 1.6)
# ---------------------------------------------------------------------------


class TestTurtleRoundTrip:
    """Round-trip of the fixture into :class:`OntologySchema`."""

    def test_load_turtle_string_populates_counts(self, schema: OntologySchema) -> None:
        """Class and property counts match the fixture (Requirement 1.1)."""
        assert len(schema.classes) == 4
        assert len(schema.object_properties) == 1
        assert len(schema.datatype_properties) == 2

    def test_load_turtle_string_populates_namespace(self, schema: OntologySchema) -> None:
        """``namespace`` reflects the Turtle ``@prefix :`` declaration
        (Requirement 3.5 happy path)."""
        assert schema.namespace == NAMESPACE

    def test_load_turtle_populates_class_iris(self, schema: OntologySchema) -> None:
        """All four declared classes are keyed by their IRI in ``classes``
        (Requirement 1.1)."""
        assert set(schema.classes.keys()) == {
            PERSON_IRI,
            EMPLOYEE_IRI,
            MANAGER_IRI,
            COMPANY_IRI,
        }

    def test_load_turtle_populates_property_iris(self, schema: OntologySchema) -> None:
        """Object and datatype properties are keyed by IRI (Requirement 1.1)."""
        assert set(schema.object_properties.keys()) == {WORKS_FOR_IRI}
        assert set(schema.datatype_properties.keys()) == {NAME_IRI, AGE_IRI}

    def test_load_turtle_populates_labels(self, schema: OntologySchema) -> None:
        """``rdfs:label`` is copied verbatim onto the class (Requirement 1.2)."""
        assert schema.classes[PERSON_IRI].label == "Person"
        assert schema.classes[EMPLOYEE_IRI].label == "Employee"
        assert schema.classes[MANAGER_IRI].label == "Manager"
        assert schema.classes[COMPANY_IRI].label == "Company"

    def test_load_turtle_populates_local_names(self, schema: OntologySchema) -> None:
        """``local_name`` is the last ``/`` segment of the IRI (Requirement 1.3)."""
        assert schema.classes[PERSON_IRI].local_name == "Person"
        assert schema.classes[EMPLOYEE_IRI].local_name == "Employee"
        assert schema.classes[MANAGER_IRI].local_name == "Manager"
        assert schema.classes[COMPANY_IRI].local_name == "Company"
        assert schema.object_properties[WORKS_FOR_IRI].local_name == "worksFor"
        assert schema.datatype_properties[NAME_IRI].local_name == "name"
        assert schema.datatype_properties[AGE_IRI].local_name == "age"

    def test_load_turtle_populates_parents(self, schema: OntologySchema) -> None:
        """``parents`` holds direct ``rdfs:subClassOf`` IRIs in declaration
        order (Requirement 1.4). Root-level classes have empty ``parents``."""
        assert schema.classes[PERSON_IRI].parents == []
        assert schema.classes[EMPLOYEE_IRI].parents == [PERSON_IRI]
        assert schema.classes[MANAGER_IRI].parents == [EMPLOYEE_IRI]
        assert schema.classes[COMPANY_IRI].parents == []

    def test_load_turtle_populates_object_property_domain_range(
        self, schema: OntologySchema
    ) -> None:
        """``rdfs:domain``/``rdfs:range`` on object properties are copied as
        class-IRI lists (Requirement 1.4)."""
        works_for = schema.object_properties[WORKS_FOR_IRI]
        assert works_for.domain == [EMPLOYEE_IRI]
        assert works_for.range == [COMPANY_IRI]
        assert works_for.label == "works for"

    def test_load_turtle_populates_datatype_property_datatype(
        self, schema: OntologySchema
    ) -> None:
        """``DatatypeProperty.datatype`` is the single XSD IRI from
        ``rdfs:range`` (Requirement 1.5)."""
        name_prop = schema.datatype_properties[NAME_IRI]
        age_prop = schema.datatype_properties[AGE_IRI]
        assert name_prop.datatype == XSD_NAMESPACE + "string"
        assert name_prop.domain == [PERSON_IRI]
        assert age_prop.datatype == XSD_NAMESPACE + "integer"
        assert age_prop.domain == [PERSON_IRI]

    def test_load_from_turtle_file(self, tmp_path) -> None:
        """``from_turtle(path)`` accepts a filesystem path and produces the
        same schema as ``from_turtle_string`` (Requirement 1.6 contract
        sibling — ``from_turtle`` is the primary entry point)."""
        ttl_path = tmp_path / "example.ttl"
        ttl_path.write_text(PERSON_EMPLOYEE_COMPANY_TURTLE, encoding="utf-8")

        file_schema = OntologySchema.from_turtle(ttl_path)

        assert len(file_schema.classes) == 4
        assert len(file_schema.object_properties) == 1
        assert len(file_schema.datatype_properties) == 2
        assert file_schema.namespace == NAMESPACE
        # Cross-check one value from each category to prove disk-loaded state
        # is identical to in-memory-loaded state.
        assert file_schema.classes[MANAGER_IRI].parents == [EMPLOYEE_IRI]
        assert file_schema.object_properties[WORKS_FOR_IRI].range == [COMPANY_IRI]
        assert (
            file_schema.datatype_properties[AGE_IRI].datatype
            == XSD_NAMESPACE + "integer"
        )

    def test_load_from_turtle_accepts_str_path(self, tmp_path) -> None:
        """``from_turtle`` accepts ``str`` as well as ``pathlib.Path``. The
        docstring on ``from_turtle`` documents this explicitly; the loader
        normalises the argument via ``str(path)`` before handing off to
        rdflib."""
        ttl_path = tmp_path / "example.ttl"
        ttl_path.write_text(PERSON_EMPLOYEE_COMPANY_TURTLE, encoding="utf-8")

        file_schema = OntologySchema.from_turtle(str(ttl_path))
        assert file_schema.namespace == NAMESPACE


# ---------------------------------------------------------------------------
# Subclass closure — reflexivity and transitivity (Requirement 2.1 – 2.4)
# ---------------------------------------------------------------------------


class TestSubclassClosure:
    """Reflexive + transitive closure over ``Manager ⊂ Employee ⊂ Person``."""

    def test_ancestors_reflexive(self, schema: OntologySchema) -> None:
        """Every class is its own ancestor (Requirement 2.1, 2.3)."""
        for iri in schema.classes:
            assert iri in schema.classes[iri].ancestors, (
                f"{iri} missing from its own ancestor set (not reflexive)"
            )
            assert schema.is_subclass_of(iri, iri), (
                f"is_subclass_of({iri}, {iri}) should be True"
            )

    def test_ancestors_transitive_three_level(self, schema: OntologySchema) -> None:
        """Transitive closure over a three-level hierarchy (Requirement 2.4).

        ``Manager ⊂ Employee ⊂ Person`` implies ``Manager ⊂ Person``.
        """
        assert schema.is_subclass_of(MANAGER_IRI, EMPLOYEE_IRI) is True
        assert schema.is_subclass_of(MANAGER_IRI, PERSON_IRI) is True
        assert schema.is_subclass_of(EMPLOYEE_IRI, PERSON_IRI) is True

    def test_ancestors_not_upward(self, schema: OntologySchema) -> None:
        """Superclasses are not subclasses of their children (no spurious
        inversion — Requirement 2.2 negative direction)."""
        assert schema.is_subclass_of(PERSON_IRI, EMPLOYEE_IRI) is False
        assert schema.is_subclass_of(EMPLOYEE_IRI, MANAGER_IRI) is False
        assert schema.is_subclass_of(PERSON_IRI, MANAGER_IRI) is False

    def test_ancestors_unrelated_branches(self, schema: OntologySchema) -> None:
        """Sibling branches of the hierarchy have disjoint ancestry (except
        via an explicit shared ancestor — Requirement 2.2 negative direction).
        ``Company`` is a root class distinct from the ``Person`` branch so
        none of ``Manager``/``Employee``/``Person`` is a subclass of it."""
        assert schema.is_subclass_of(MANAGER_IRI, COMPANY_IRI) is False
        assert schema.is_subclass_of(EMPLOYEE_IRI, COMPANY_IRI) is False
        assert schema.is_subclass_of(PERSON_IRI, COMPANY_IRI) is False
        assert schema.is_subclass_of(COMPANY_IRI, PERSON_IRI) is False

    def test_ancestors_frozenset_contents(self, schema: OntologySchema) -> None:
        """``ancestors`` sets match the expected reflexive + transitive
        closure precisely."""
        assert schema.classes[PERSON_IRI].ancestors == frozenset({PERSON_IRI})
        assert schema.classes[EMPLOYEE_IRI].ancestors == frozenset(
            {EMPLOYEE_IRI, PERSON_IRI}
        )
        assert schema.classes[MANAGER_IRI].ancestors == frozenset(
            {MANAGER_IRI, EMPLOYEE_IRI, PERSON_IRI}
        )
        assert schema.classes[COMPANY_IRI].ancestors == frozenset({COMPANY_IRI})

    def test_is_subclass_of_unknown_child(self, schema: OntologySchema) -> None:
        """An IRI that is not a declared class yields ``False`` rather than
        raising (documented on ``is_subclass_of``)."""
        assert schema.is_subclass_of(NAMESPACE + "Unknown", PERSON_IRI) is False


# ---------------------------------------------------------------------------
# Error cases (Requirement 3)
#
# Each test uses a minimally-scoped Turtle fragment that isolates exactly
# one invariant violation. ``pytest.raises(OntologyLoadError)`` confirms
# the Requirement 3.6 contract: no partial schema object is returned, the
# loader raises cleanly. ``match=`` strings anchor on substrings of the
# error messages emitted by ``ontology_schema.py`` so an accidental
# message change is caught alongside a behavior regression.
# ---------------------------------------------------------------------------


class TestLoadErrors:
    """Load-time structural validation failures."""

    def test_rejects_cyclic_subclassof(self) -> None:
        """``A rdfs:subClassOf B; B rdfs:subClassOf A`` → ``OntologyLoadError``
        (Requirement 3.1)."""
        turtle = """
        @prefix : <https://example.com/kg/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        :A a owl:Class ;
            rdfs:subClassOf :B .
        :B a owl:Class ;
            rdfs:subClassOf :A .
        """
        with pytest.raises(OntologyLoadError, match="cycle"):
            OntologySchema.from_turtle_string(turtle)

    def test_rejects_dangling_class_reference(self) -> None:
        """``:A rdfs:subClassOf :NotDeclared`` where ``:NotDeclared`` is
        undeclared → ``OntologyLoadError`` naming the dangling IRI
        (Requirement 3.2)."""
        turtle = """
        @prefix : <https://example.com/kg/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        :A a owl:Class ;
            rdfs:subClassOf :NotDeclared .
        """
        with pytest.raises(OntologyLoadError, match="dangling"):
            OntologySchema.from_turtle_string(turtle)

    def test_rejects_dangling_object_property_domain(self) -> None:
        """Dangling ``rdfs:domain`` on an object property is also rejected
        (Requirement 3.2 applied to property slots)."""
        turtle = """
        @prefix : <https://example.com/kg/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        :A a owl:Class .
        :prop a owl:ObjectProperty ;
            rdfs:domain :NotDeclared ;
            rdfs:range :A .
        """
        with pytest.raises(OntologyLoadError, match="dangling"):
            OntologySchema.from_turtle_string(turtle)

    def test_rejects_property_in_both_categories(self) -> None:
        """A single IRI declared as both ``owl:ObjectProperty`` and
        ``owl:DatatypeProperty`` → ``OntologyLoadError`` (Requirement 3.3)."""
        turtle = """
        @prefix : <https://example.com/kg/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        :A a owl:Class .
        :prop a owl:ObjectProperty ;
            a owl:DatatypeProperty ;
            rdfs:domain :A ;
            rdfs:range xsd:string .
        """
        with pytest.raises(OntologyLoadError, match="polymorphic"):
            OntologySchema.from_turtle_string(turtle)

    def test_rejects_datatype_property_with_non_xsd_range(self) -> None:
        """``DatatypeProperty.rdfs:range`` outside the XSD namespace →
        ``OntologyLoadError`` (Requirement 3.4)."""
        turtle = """
        @prefix : <https://example.com/kg/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        :A a owl:Class .
        :prop a owl:DatatypeProperty ;
            rdfs:domain :A ;
            rdfs:range <http://example.com/NotXSD> .
        """
        with pytest.raises(OntologyLoadError, match="XSD"):
            OntologySchema.from_turtle_string(turtle)

    def test_rejects_datatype_property_with_no_range(self) -> None:
        """A ``DatatypeProperty`` with no ``rdfs:range`` at all →
        ``OntologyLoadError`` (Requirement 3.4 — every DatatypeProperty
        must declare exactly one range)."""
        turtle = """
        @prefix : <https://example.com/kg/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        :A a owl:Class .
        :prop a owl:DatatypeProperty ;
            rdfs:domain :A .
        """
        with pytest.raises(OntologyLoadError, match="no rdfs:range"):
            OntologySchema.from_turtle_string(turtle)

    def test_rejects_missing_base_iri(self) -> None:
        """Turtle with no default ``@prefix :``, no ``owl:Ontology`` subject,
        and no caller-supplied ``base_iri`` hint → ``OntologyLoadError``
        (Requirement 3.5)."""
        turtle = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix x: <http://example.com/x#> .
        x:A a owl:Class ;
            rdfs:label "A" .
        """
        with pytest.raises(OntologyLoadError, match="base IRI"):
            OntologySchema.from_turtle_string(turtle)

    def test_ontology_load_error_does_not_return_partial_schema(self) -> None:
        """Requirement 3.6 — when ``from_turtle_string`` raises
        ``OntologyLoadError``, no partial :class:`OntologySchema` instance
        is visible to the caller. We assert this by binding the result of
        the call to a local name whose value is initialised to a sentinel:
        because the exception propagates before the assignment completes,
        the sentinel is preserved and we can reason about it directly.
        """
        turtle = """
        @prefix : <https://example.com/kg/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        :A a owl:Class ;
            rdfs:subClassOf :B .
        :B a owl:Class ;
            rdfs:subClassOf :A .
        """
        sentinel = object()
        result = sentinel
        with pytest.raises(OntologyLoadError):
            # If the loader ever returned a partial schema before raising,
            # this assignment would land on the caller's binding before
            # the exception propagates.
            result = OntologySchema.from_turtle_string(turtle)

        assert result is sentinel, (
            "from_turtle_string leaked a partial schema to the caller; "
            "Requirement 3.6 demands the loader never return on error."
        )


# ---------------------------------------------------------------------------
# Property-based tests (Task 2.6) — subclass closure is reflexive,
# transitive, and acyclic.
#
# Design §Correctness Properties P2:
#
#   * ∀ c ∈ O.classes: O.is_subclass_of(c.iri, c.iri) == True (reflexive)
#   * ∀ a, b, c: O.is_subclass_of(a, b) ∧ O.is_subclass_of(b, c) ⟹
#     O.is_subclass_of(a, c) (transitive)
#   * A Turtle file containing a ``subClassOf`` cycle raises
#     :class:`OntologyLoadError` and produces no :class:`OntologySchema`.
#
# We also verify the equivalence between ``is_subclass_of(a, c)`` and the
# existence of a parent-path from ``a`` to ``c`` in the generated DAG,
# which ties the closure back to the input shape and pins down the
# semantics of ``is_subclass_of`` more precisely than reflexivity +
# transitivity alone would.
#
# Strategies
# ----------
#
# ``dag_ontology_strategy`` generates random DAG-shaped class hierarchies by
# labelling classes ``C0, C1, ..., Cn`` and only permitting a class ``Cj``
# to declare ``Ci`` as a parent when ``i < j``. This topological invariant
# guarantees the generated graph is acyclic by construction — no
# post-generation cycle filter is needed.
#
# ``cyclic_chain_ontology_strategy`` generates a minimal cyclic chain
# ``C0 -> C1 -> ... -> C(n-1) -> C0`` (every ``Ci`` has exactly one parent,
# ``C(i-1) mod n``). Every instance is guaranteed to contain a cycle, so
# the test is not probabilistic: every generated example must raise
# :class:`OntologyLoadError` when loaded.
#
# Validates: Requirements 2.1, 2.2, 2.3, 2.4, 3.1.
# ---------------------------------------------------------------------------


# The base IRI used by every Turtle document generated by these strategies.
# Matching the main fixture keeps the resolved IRIs consistent with the rest
# of the test module and lets us build IRI strings with simple concatenation.
_PBT_NAMESPACE = "https://example.com/kg/"


def _dag_ontology_turtle(classes_and_parents):
    """Render a ``{class_local_name: [parent_local_names]}`` map as Turtle.

    Every generated document contains:

    * ``@prefix :`` — so :func:`_extract_base_iri` can recover the base IRI
      (Requirement 3.5).
    * ``owl:`` and ``rdfs:`` prefixes for the ``owl:Class`` / ``rdfs:subClassOf``
      declarations.
    * One ``owl:Class`` declaration per entry in ``classes_and_parents``,
      followed by one ``rdfs:subClassOf :Parent`` line per declared parent.
      The parent ordering in the Turtle output matches the input list so
      that :attr:`OntologyClass.parents` is stable across runs.

    No object or datatype properties are emitted — the subclass-closure
    properties only exercise the class hierarchy, and keeping the Turtle
    minimal speeds up rdflib parsing across Hypothesis's many examples.
    """
    lines = [
        "@prefix : <" + _PBT_NAMESPACE + "> .",
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "",
    ]
    for cls, parents in classes_and_parents.items():
        # Build the predicate block as a single-subject `:Cls a owl:Class ;
        # rdfs:subClassOf :P1 ; rdfs:subClassOf :P2 .` statement so parent
        # order is preserved exactly as rdflib surfaces it back to us.
        block = [f":{cls} a owl:Class"]
        for p in parents:
            block.append(f"    rdfs:subClassOf :{p}")
        lines.append(" ;\n".join(block) + " .")
    return "\n".join(lines) + "\n"


@st.composite
def dag_ontology_strategy(draw, min_classes=1, max_classes=8):
    """Generate a DAG of class hierarchies — no cycles possible.

    Classes are named ``C0, C1, ..., C(n-1)``. Each class ``Cj`` may declare
    parents only from ``{C0, ..., C(j-1)}``. Because parent edges always
    point to strictly-earlier indices, the resulting parents graph admits a
    trivial topological order (``C0, C1, ...``), so no cycle is reachable.

    ``min_classes`` / ``max_classes`` bound the generated ontology size
    between 1 and 8 classes by default — small enough that the quadratic
    transitive-closure assertion below runs quickly across Hypothesis's
    50-example budget.

    Returns a dict ``{class_local_name: [parent_local_names]}`` that
    :func:`_dag_ontology_turtle` can render directly into a Turtle document.
    """
    n = draw(st.integers(min_value=min_classes, max_value=max_classes))
    classes_and_parents = {}
    for j in range(n):
        class_name = f"C{j}"
        if j == 0:
            # The first class has no earlier class it could point at.
            parents = []
        else:
            # Each class can have 0..min(j, 3) parents chosen from
            # {C0..C(j-1)}. Capping at 3 keeps each class narrow; fan-in
            # greater than that doesn't increase coverage of the closure
            # logic meaningfully and slows the test down.
            max_parents = min(j, 3)
            num_parents = draw(st.integers(min_value=0, max_value=max_parents))
            parent_indices = draw(
                st.lists(
                    st.integers(min_value=0, max_value=j - 1),
                    min_size=num_parents,
                    max_size=num_parents,
                    unique=True,
                )
            )
            parents = [f"C{i}" for i in parent_indices]
        classes_and_parents[class_name] = parents
    return classes_and_parents


@st.composite
def cyclic_chain_ontology_strategy(draw, min_classes=2, max_classes=5):
    """Generate a cyclic chain ontology ``C0 -> C1 -> ... -> C(n-1) -> C0``.

    Every generated instance is guaranteed to contain exactly one cycle —
    the ring closed by ``C0 rdfs:subClassOf C(n-1)`` combined with the
    descending chain ``C(i+1) rdfs:subClassOf Ci``. Because the cycle is
    inherent to the construction (not injected probabilistically), the
    assertion that ``OntologySchema.from_turtle_string`` raises
    :class:`OntologyLoadError` holds for every example — no conditional
    "did we actually create a cycle?" branching is needed in the test.

    ``n`` is drawn from ``[min_classes, max_classes]``; the default bounds
    (2..5) give enough variety to catch off-by-one issues in cycle
    detection without bloating the test runtime.

    Returns a dict ``{class_local_name: [parent_local_names]}`` with
    exactly one parent per class. The edge list closes the cycle at
    ``C0``, whose parent is ``C(n-1)``.
    """
    n = draw(st.integers(min_value=min_classes, max_value=max_classes))
    classes_and_parents = {}
    for i in range(n):
        # Parent is the *previous* class (wrapping around at C0 to close
        # the cycle via C(n-1)). With a single parent per class, the
        # parents graph is a simple ring.
        parent_index = (i - 1) % n
        classes_and_parents[f"C{i}"] = [f"C{parent_index}"]
    return classes_and_parents


def _is_ancestor_via_parents(
    start: str, target: str, dag_map: dict
) -> bool:
    """Return True iff ``target`` is reachable from ``start`` via parent edges.

    Performs an iterative depth-first walk over the ``parents`` graph so
    the reachability check matches the semantics that
    :func:`_compute_subclass_closure` implements — reflexive (``start ==
    target`` returns True immediately) and transitive through the
    ``parents`` chain, ignoring any names that are not declared classes.

    Used as an independent oracle for the correspondence assertion in
    :meth:`TestSubclassClosureProperties.test_closure_reflexive_transitive_on_random_dags`.
    """
    if start == target:
        return True
    visited: set = set()
    stack = [start]
    while stack:
        cur = stack.pop()
        if cur in visited:
            continue
        visited.add(cur)
        if cur == target:
            return True
        for parent in dag_map.get(cur, []):
            if parent not in visited:
                stack.append(parent)
    return False


class TestSubclassClosureProperties:
    """Hypothesis property-based tests for subclass closure (Property P2).

    Validates: Requirements 2.1, 2.2, 2.3, 2.4, 3.1.
    """

    @given(dag=dag_ontology_strategy())
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_closure_reflexive_transitive_on_random_dags(self, dag):
        """Reflexive, transitive, and correspondence-to-parent-path.

        For any DAG-shaped class hierarchy generated by
        :func:`dag_ontology_strategy`, :meth:`OntologySchema.is_subclass_of`
        must agree with an independent graph-walk oracle. The assertion
        spans three sub-checks in a single test because they all share the
        same generated example and running them together keeps the
        Hypothesis example budget compact.

        * **Reflexivity (Requirement 2.1):** every class is its own
          subclass.
        * **Transitivity (Requirement 2.4):** if ``a`` is a subclass of
          ``b`` and ``b`` of ``c``, then ``a`` is a subclass of ``c``.
        * **Correspondence (Requirements 2.2, 2.3):** ``is_subclass_of(a,
          c)`` is True iff there is a parent-path from ``a`` to ``c``
          (including the trivial ``a == c`` case).
        """
        turtle = _dag_ontology_turtle(dag)
        schema = OntologySchema.from_turtle_string(turtle)

        # Reflexivity: every class is its own ancestor. Equivalent to
        # ``self in classes[self].ancestors`` but goes through the public
        # API so we exercise the code path users actually call.
        for cls_name in dag:
            iri = _PBT_NAMESPACE + cls_name
            assert schema.is_subclass_of(iri, iri), (
                f"Reflexivity broken: is_subclass_of({iri!r}, {iri!r}) "
                f"returned False"
            )

        # Transitivity: if a -> b and b -> c are both recognised as
        # subclass relations, then a -> c must be too.
        for a in dag:
            a_iri = _PBT_NAMESPACE + a
            for b in dag:
                b_iri = _PBT_NAMESPACE + b
                if not schema.is_subclass_of(a_iri, b_iri):
                    continue
                for c in dag:
                    c_iri = _PBT_NAMESPACE + c
                    if schema.is_subclass_of(b_iri, c_iri):
                        assert schema.is_subclass_of(a_iri, c_iri), (
                            f"Transitivity broken: is_subclass_of("
                            f"{a_iri!r}, {b_iri!r}) and is_subclass_of("
                            f"{b_iri!r}, {c_iri!r}) both True but "
                            f"is_subclass_of({a_iri!r}, {c_iri!r}) False"
                        )

        # Correspondence with parent-path existence. The oracle walks the
        # generated DAG directly; the schema answer comes from the
        # pre-computed ancestor set. Any disagreement signals either a
        # bug in ``_compute_subclass_closure`` or a mismatch between the
        # Turtle rendering and the DAG structure we think we built.
        for a in dag:
            a_iri = _PBT_NAMESPACE + a
            for c in dag:
                c_iri = _PBT_NAMESPACE + c
                expected = _is_ancestor_via_parents(a, c, dag)
                actual = schema.is_subclass_of(a_iri, c_iri)
                assert actual == expected, (
                    f"is_subclass_of({a_iri!r}, {c_iri!r}) = {actual}, "
                    f"but parent-path oracle says {expected}. DAG was "
                    f"{dag!r}."
                )

    @given(cyclic_dag=cyclic_chain_ontology_strategy())
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_cyclic_ontologies_rejected(self, cyclic_dag):
        """A Turtle file containing a ``subClassOf`` cycle must raise.

        Every instance generated by
        :func:`cyclic_chain_ontology_strategy` has a guaranteed cycle in
        its parents graph, so :meth:`OntologySchema.from_turtle_string`
        must raise :class:`OntologyLoadError` for every example. The
        ``match="cycle"`` substring keeps the test loosely bound to the
        error message's wording ("rdfs:subClassOf cycle detected at ...")
        so a benign rephrasing of the message doesn't break the suite,
        but a regression that changes the error class or silently
        accepts the cyclic input does.

        Validates Requirement 3.1 (cycle detection must raise
        :class:`OntologyLoadError` at load time, no partial schema
        returned per Requirement 3.6).
        """
        turtle = _dag_ontology_turtle(cyclic_dag)
        with pytest.raises(OntologyLoadError, match="cycle"):
            OntologySchema.from_turtle_string(turtle)

# ---------------------------------------------------------------------------
# Task 3.7 — Resolvers, bridge, and prompt-rendering tests.
#
# These tests exercise the suggestion-mode surface of ``OntologySchema`` on
# the same Person/Employee/Manager/Company fixture used above:
#
#   * :class:`TestResolvers` — ``resolve_class``,
#     ``resolve_object_predicate``, ``resolve_datatype_predicate``,
#     ``allowed_object_predicates``, ``allowed_datatype_predicates``
#     (Requirements 1.4, 1.5, 2.3, 2.4, 6.1 resolver contract).
#   * :class:`TestAsExtractionSchema` — the bridge to the flat
#     :class:`ExtractionSchema`, including the ``format_as_prompt_constraint``
#     rebind that makes the LLM see the ontology's richer prompt
#     (Requirements 5.1, 5.2, 11.1, 11.2, 11.4, 11.5, 11.6).
#   * :class:`TestFormatAsPromptConstraint` — completeness and determinism
#     of the rendered prompt text (Requirements 12.1, 12.2, 12.3, 12.4,
#     12.5, 13.1, 13.2, 13.3, 13.4, NFR-6).
#   * :class:`TestSchemaFilterStageBackwardCompat` — the bridged
#     :class:`ExtractionSchema` is shape-equivalent to a hand-built one
#     with the same ``entity_types`` / ``relationship_types`` / ``strict``
#     payload so that existing :class:`SchemaFilterStage` call sites work
#     unchanged (Requirement 11.5).
# ---------------------------------------------------------------------------


# A Turtle fixture whose labels deliberately differ from local names so
# ``resolve_class`` by-label can be distinguished from by-local-name. The
# primary fixture above uses labels equal to local names (so both paths
# match the same query), which is useful for most tests but masks the
# label-only path.
#
# Here, ``Car`` and ``Truck`` carry distinctive labels ("Automobile" and
# "Lorry") that have no overlap with their local names — so a lookup by
# "Automobile" can only succeed via the ``_by_label`` index. ``Car`` is a
# subclass of ``Vehicle`` to keep a small hierarchy available for other
# label-path tests that may want it.
DISTINCT_LABEL_TURTLE = """
@prefix : <https://example.com/vehicles/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

: a owl:Ontology .

:Vehicle a owl:Class ;
    rdfs:label "Vehicle" .

:Car a owl:Class ;
    rdfs:subClassOf :Vehicle ;
    rdfs:label "Automobile" .

:Truck a owl:Class ;
    rdfs:subClassOf :Vehicle ;
    rdfs:label "Lorry" .
"""

_VEHICLES_NS = "https://example.com/vehicles/"
CAR_IRI = _VEHICLES_NS + "Car"
TRUCK_IRI = _VEHICLES_NS + "Truck"


@pytest.fixture
def label_schema() -> OntologySchema:
    """Schema whose class labels are deliberately distinct from local names.

    Used by label-only ``resolve_class`` tests so that a successful
    match by ``"Automobile"`` or ``"Lorry"`` is unambiguous evidence
    that the ``_by_label`` fallback fired — there is no way for the
    local-name index to resolve those strings.
    """
    return OntologySchema.from_turtle_string(DISTINCT_LABEL_TURTLE)


class TestResolvers:
    """Resolver behaviour — class, object-predicate, datatype-predicate,
    and subclass-aware ``allowed_*`` walks.

    Validates: Requirements 1.4, 1.5, 2.3, 2.4, 6.1 (resolver contract)."""

    # ------------------------------------------------------------------
    # resolve_class
    # ------------------------------------------------------------------

    def test_resolve_class_exact_iri(self, schema: OntologySchema) -> None:
        """An exact IRI round-trips unchanged (first branch in the match
        order documented on :meth:`resolve_class`)."""
        assert schema.resolve_class(PERSON_IRI) == PERSON_IRI
        assert schema.resolve_class(EMPLOYEE_IRI) == EMPLOYEE_IRI
        assert schema.resolve_class(MANAGER_IRI) == MANAGER_IRI
        assert schema.resolve_class(COMPANY_IRI) == COMPANY_IRI

    def test_resolve_class_by_local_name(self, schema: OntologySchema) -> None:
        """Exact-case ``local_name`` resolves via the ``_by_local_name``
        index (second branch in the match order)."""
        assert schema.resolve_class("Person") == PERSON_IRI
        assert schema.resolve_class("Employee") == EMPLOYEE_IRI
        assert schema.resolve_class("Manager") == MANAGER_IRI
        assert schema.resolve_class("Company") == COMPANY_IRI

    def test_resolve_class_by_local_name_case_insensitive(
        self, schema: OntologySchema
    ) -> None:
        """Local-name lookup is case-insensitive — lowercasing, uppercasing,
        and mixed-case spellings all resolve to the same IRI."""
        assert schema.resolve_class("person") == PERSON_IRI
        assert schema.resolve_class("PERSON") == PERSON_IRI
        assert schema.resolve_class("PeRsOn") == PERSON_IRI
        assert schema.resolve_class("employee") == EMPLOYEE_IRI
        assert schema.resolve_class("MANAGER") == MANAGER_IRI

    def test_resolve_class_by_label_only(self, label_schema: OntologySchema) -> None:
        """When the label differs from the local name, ``resolve_class``
        falls through to the ``_by_label`` index (third branch in the
        match order). Uses the ``label_schema`` fixture whose ``Car`` is
        labelled ``"Automobile"`` — a string that cannot hit the
        local-name index."""
        assert label_schema.resolve_class("Automobile") == CAR_IRI
        assert label_schema.resolve_class("automobile") == CAR_IRI
        assert label_schema.resolve_class("AUTOMOBILE") == CAR_IRI
        assert label_schema.resolve_class("Lorry") == TRUCK_IRI
        assert label_schema.resolve_class("LORRY") == TRUCK_IRI

    def test_resolve_class_miss(self, schema: OntologySchema) -> None:
        """Unknown names, empty strings, and ``None`` all return ``None``
        rather than raising — ``resolve_class`` is the load-bearing guard
        for callers that pass raw LLM output straight in."""
        assert schema.resolve_class("Unknown") is None
        assert schema.resolve_class("NotAClass") is None
        # Empty string and None short-circuit via ``if not name:``.
        assert schema.resolve_class("") is None
        assert schema.resolve_class(None) is None  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # resolve_object_predicate
    # ------------------------------------------------------------------

    def test_resolve_object_predicate_upper_snake(
        self, schema: OntologySchema
    ) -> None:
        """UPPER_SNAKE_CASE (what the extraction prompt emits) resolves to
        the declared ``:worksFor`` via the camelCase fold in
        :func:`_normalize_predicate_name`."""
        results = schema.resolve_object_predicate("WORKS_FOR")
        assert len(results) == 1
        assert results[0].iri == WORKS_FOR_IRI

    def test_resolve_object_predicate_camel_case(
        self, schema: OntologySchema
    ) -> None:
        """The original camelCase ``local_name`` resolves as-is — this is
        the happy path for ontologies whose authors follow Turtle
        conventions."""
        results = schema.resolve_object_predicate("worksFor")
        assert len(results) == 1
        assert results[0].iri == WORKS_FOR_IRI
        # Case-insensitive, so all-lowercase and all-uppercase forms of
        # the camelCase ``local_name`` also hit.
        assert schema.resolve_object_predicate("worksfor")[0].iri == WORKS_FOR_IRI
        assert schema.resolve_object_predicate("WORKSFOR")[0].iri == WORKS_FOR_IRI

    def test_resolve_object_predicate_with_space(
        self, schema: OntologySchema
    ) -> None:
        """An ``rdfs:label`` with a space (``"works for"``) resolves
        through the ``_obj_predicate_index`` label key."""
        results = schema.resolve_object_predicate("works for")
        assert len(results) == 1
        assert results[0].iri == WORKS_FOR_IRI
        # Label lookup is case-insensitive.
        assert schema.resolve_object_predicate("Works For")[0].iri == WORKS_FOR_IRI
        assert schema.resolve_object_predicate("WORKS FOR")[0].iri == WORKS_FOR_IRI

    def test_resolve_object_predicate_miss(
        self, schema: OntologySchema
    ) -> None:
        """Unknown predicate names and empty strings return an empty list
        (not ``None``) so callers can iterate without a guard."""
        assert schema.resolve_object_predicate("gibberish") == []
        assert schema.resolve_object_predicate("IS_NOT_DECLARED") == []
        assert schema.resolve_object_predicate("") == []

    # ------------------------------------------------------------------
    # resolve_datatype_predicate
    # ------------------------------------------------------------------

    def test_resolve_datatype_predicate_by_local_name(
        self, schema: OntologySchema
    ) -> None:
        """``age`` is declared as ``owl:DatatypeProperty`` with range
        ``xsd:integer``; the resolver finds it via the
        ``_dt_predicate_index``."""
        results = schema.resolve_datatype_predicate("age")
        assert len(results) == 1
        assert results[0].iri == AGE_IRI

    def test_resolve_datatype_predicate_case_insensitive_and_snake(
        self, schema: OntologySchema
    ) -> None:
        """Datatype predicates accept UPPER_SNAKE and mixed-case variants
        for the same reason as object predicates — the prompt-driven LLM
        convention emits UPPER_SNAKE and the resolver must fold it."""
        assert schema.resolve_datatype_predicate("AGE")[0].iri == AGE_IRI
        assert schema.resolve_datatype_predicate("Age")[0].iri == AGE_IRI
        # ``name`` has a single-word local name, so upper-snake is the
        # same as upper-case; both round-trip to the same property.
        assert schema.resolve_datatype_predicate("NAME")[0].iri == NAME_IRI
        assert schema.resolve_datatype_predicate("name")[0].iri == NAME_IRI

    def test_resolve_datatype_predicate_miss(
        self, schema: OntologySchema
    ) -> None:
        """Unknown and empty inputs return ``[]``."""
        assert schema.resolve_datatype_predicate("weight") == []
        assert schema.resolve_datatype_predicate("") == []

    # ------------------------------------------------------------------
    # allowed_object_predicates — subclass-aware domain / range walks
    # ------------------------------------------------------------------

    def test_allowed_object_predicates_direct_match(
        self, schema: OntologySchema
    ) -> None:
        """``Employee -> Company`` is the direct domain/range declared
        on ``:worksFor`` — the exact pair returns a single candidate."""
        results = schema.allowed_object_predicates(EMPLOYEE_IRI, COMPANY_IRI)
        assert len(results) == 1
        assert results[0].iri == WORKS_FOR_IRI

    def test_allowed_object_predicates_via_subclass_closure(
        self, schema: OntologySchema
    ) -> None:
        """``Manager`` inherits from ``Employee``, so
        ``Manager -> Company`` must still resolve to ``:worksFor`` via
        ``is_subclass_of`` (Requirement 2.4 applied to the resolver)."""
        results = schema.allowed_object_predicates(MANAGER_IRI, COMPANY_IRI)
        assert len(results) == 1
        assert results[0].iri == WORKS_FOR_IRI

    def test_allowed_object_predicates_no_match(
        self, schema: OntologySchema
    ) -> None:
        """``Person`` is not a subclass of ``Employee``, so
        ``Person -> Company`` does NOT match ``:worksFor``. The wrong
        direction (``Company -> Person``) also fails because
        ``Company`` is not a subclass of ``Employee`` and ``Person`` is
        not a subclass of ``Company``."""
        # Wrong subject class: Person is a supertype of Employee, not a
        # subtype of it, so Person cannot satisfy a domain of Employee.
        assert schema.allowed_object_predicates(PERSON_IRI, COMPANY_IRI) == []
        # Reversed direction: Company is not in the domain set.
        assert schema.allowed_object_predicates(COMPANY_IRI, PERSON_IRI) == []
        # Same-class both sides: Person -> Person doesn't match any
        # declared object property.
        assert schema.allowed_object_predicates(PERSON_IRI, PERSON_IRI) == []

    # ------------------------------------------------------------------
    # allowed_datatype_predicates — subclass-aware domain walks (no range)
    # ------------------------------------------------------------------

    def test_allowed_datatype_predicates_direct_match(
        self, schema: OntologySchema
    ) -> None:
        """Both ``:name`` and ``:age`` are declared with domain
        ``:Person``; querying with ``PERSON_IRI`` returns both."""
        results = schema.allowed_datatype_predicates(PERSON_IRI)
        iris = {dp.iri for dp in results}
        assert iris == {NAME_IRI, AGE_IRI}

    def test_allowed_datatype_predicates_via_subclass_closure(
        self, schema: OntologySchema
    ) -> None:
        """``Manager ⊂ Employee ⊂ Person`` so both datatype properties
        declared on ``:Person`` must also resolve for ``Manager``
        (Requirement 2.4 applied to datatype predicates)."""
        results = schema.allowed_datatype_predicates(MANAGER_IRI)
        iris = {dp.iri for dp in results}
        assert iris == {NAME_IRI, AGE_IRI}
        # Same for Employee (the intermediate class).
        results = schema.allowed_datatype_predicates(EMPLOYEE_IRI)
        iris = {dp.iri for dp in results}
        assert iris == {NAME_IRI, AGE_IRI}

    def test_allowed_datatype_predicates_no_match(
        self, schema: OntologySchema
    ) -> None:
        """``Company`` is not a subclass of ``Person``, so neither
        datatype property's domain is covered — the result is empty."""
        assert schema.allowed_datatype_predicates(COMPANY_IRI) == []


class TestAsExtractionSchema:
    """Bridge from :class:`OntologySchema` to the flat
    :class:`ExtractionSchema` (Requirement 11).

    Verifies every acceptance criterion of Requirement 11 that touches
    the bridged surface: the return type is a real
    :class:`ExtractionSchema`, ``entity_types`` keys use ``local_name``,
    ``relationship_types`` uses ``UPPER_SNAKE_CASE``, ``strict=False``,
    aliases include the IRI and lowercased label, and the returned
    instance's ``format_as_prompt_constraint`` is rebound so LLM call
    sites see the richer ontology prompt rather than the flat
    :class:`ExtractionSchema` rendering (Requirement 5.2).

    Validates: Requirements 5.1, 5.2, 11.1, 11.2, 11.4, 11.5, 11.6.
    """

    def test_as_extraction_schema_returns_extraction_schema(
        self, schema: OntologySchema
    ) -> None:
        """``isinstance(...)`` holds — Requirement 11.4 contract.

        This is load-bearing for backward compatibility: existing call
        sites that run ``isinstance`` checks or pydantic validators on
        ``ExtractionSchema`` instances must accept the bridged value
        without a shim."""
        es = schema.as_extraction_schema()
        assert isinstance(es, ExtractionSchema)

    def test_entity_types_keys_match_class_local_names(
        self, schema: OntologySchema
    ) -> None:
        """``entity_types`` keys are exactly the set of ``local_name``
        values of every :class:`OntologyClass` (Requirement 11.1)."""
        es = schema.as_extraction_schema()
        assert set(es.entity_types.keys()) == {
            "Person",
            "Employee",
            "Manager",
            "Company",
        }

    def test_entity_type_description_is_label(
        self, schema: OntologySchema
    ) -> None:
        """Each :class:`EntityTypeConfig` carries the class's
        ``rdfs:label`` as its ``description`` — preserves the
        human-readable text the ontology author wrote."""
        es = schema.as_extraction_schema()
        assert es.entity_types["Person"].description == "Person"
        assert es.entity_types["Employee"].description == "Employee"
        assert es.entity_types["Manager"].description == "Manager"
        assert es.entity_types["Company"].description == "Company"

    def test_entity_type_aliases_contain_iri(
        self, schema: OntologySchema
    ) -> None:
        """The class's full IRI is in the ``aliases`` list so
        :class:`SchemaFilterStage` can match LLM output that emits IRIs
        as classifications (the bridge's whole point)."""
        es = schema.as_extraction_schema()
        assert PERSON_IRI in es.entity_types["Person"].aliases
        assert EMPLOYEE_IRI in es.entity_types["Employee"].aliases
        assert MANAGER_IRI in es.entity_types["Manager"].aliases
        assert COMPANY_IRI in es.entity_types["Company"].aliases

    def test_entity_type_aliases_contain_lowercased_label(
        self, schema: OntologySchema
    ) -> None:
        """The lowercased ``rdfs:label`` is in ``aliases`` so downstream
        case-insensitive matching in :class:`SchemaFilterStage` works
        without every caller having to re-lower each alias."""
        es = schema.as_extraction_schema()
        assert "person" in es.entity_types["Person"].aliases
        assert "employee" in es.entity_types["Employee"].aliases
        assert "manager" in es.entity_types["Manager"].aliases
        assert "company" in es.entity_types["Company"].aliases

    def test_relationship_types_are_upper_snake(
        self, schema: OntologySchema
    ) -> None:
        """``relationship_types`` is ``UPPER_SNAKE_CASE`` of every
        :class:`ObjectProperty.local_name` (Requirement 11.2)."""
        es = schema.as_extraction_schema()
        assert set(es.relationship_types) == {"WORKS_FOR"}

    def test_strict_is_false(self, schema: OntologySchema) -> None:
        """``strict=False`` always on the bridged schema — strict mode
        lives in :class:`OntologyFilterStage`, not in
        ``ExtractionSchema.strict`` (Requirement 11.3)."""
        es = schema.as_extraction_schema()
        assert es.strict is False

    def test_format_delegates_to_ontology(
        self, schema: OntologySchema
    ) -> None:
        """The rebind on the returned instance routes
        ``format_as_prompt_constraint()`` through
        :meth:`OntologySchema.format_as_prompt_constraint` so the LLM
        sees the ontology's richer prompt (Requirement 5.2).

        We assert the output contains markers only the ontology prompt
        renders — the ``"# Ontology-guided extraction"`` header and the
        ``Employee -> Company`` domain/range arrow — which the flat
        :class:`ExtractionSchema` format would not produce."""
        es = schema.as_extraction_schema()
        text = es.format_as_prompt_constraint()
        # Header is only emitted by :func:`_render_prompt_constraint`.
        assert "# Ontology-guided extraction" in text
        # Class local names appear in both renderings, but the arrow
        # formatting is ontology-specific.
        assert "Person" in text
        assert "Employee" in text
        assert "worksFor" in text
        assert "Employee -> Company" in text

    def test_format_delegate_matches_ontology_directly(
        self, schema: OntologySchema
    ) -> None:
        """The rebind preserves byte-equality with
        ``schema.format_as_prompt_constraint()`` (the default
        ``strict_prompt=True`` is what the closure uses). A divergence
        here would mean the LLM prompt depends on which method is
        called — a violation of Requirement 13.1 (determinism)."""
        es = schema.as_extraction_schema()
        assert es.format_as_prompt_constraint() == schema.format_as_prompt_constraint()


class TestFormatAsPromptConstraint:
    """Completeness and determinism of :meth:`format_as_prompt_constraint`.

    Validates: Requirements 12.1, 12.2, 12.3, 12.4, 12.5, 13.1, 13.2,
    13.3, 13.4, NFR-6.
    """

    # ------------------------------------------------------------------
    # Completeness (Requirement 12) — every vocabulary element surfaces.
    # ------------------------------------------------------------------

    def test_prompt_contains_all_class_local_names(
        self, schema: OntologySchema
    ) -> None:
        """Every :class:`OntologyClass.local_name` substring appears in
        the rendered prompt (Requirement 12.1)."""
        text = schema.format_as_prompt_constraint()
        for name in ("Person", "Employee", "Manager", "Company"):
            assert name in text, f"{name!r} missing from prompt output"

    def test_prompt_contains_object_property_with_arrow(
        self, schema: OntologySchema
    ) -> None:
        """The rendered prompt contains a ``domain -> range`` line for
        every :class:`ObjectProperty` (Requirement 12.2). We assert the
        substring rather than a full-line match so reformatting of the
        surrounding whitespace does not break this check."""
        text = schema.format_as_prompt_constraint()
        # The local_name appears on the line.
        assert "worksFor" in text
        # The explicit ``domain -> range`` substring appears.
        assert "Employee -> Company" in text

    def test_prompt_contains_datatype_property_with_xsd(
        self, schema: OntologySchema
    ) -> None:
        """Every ``(Class.attribute, xsd-type)`` pair surfaces for every
        ``d ∈ dp.domain`` — Requirement 12.3. In our fixture both
        ``:name`` and ``:age`` have domain ``:Person``, so the expected
        substrings are ``Person.name : string`` / ``Person.age :
        integer`` (the renderer uses the xsd-local name, not the full
        IRI)."""
        text = schema.format_as_prompt_constraint()
        assert "Person.age" in text
        assert "integer" in text
        assert "Person.name" in text
        assert "string" in text

    def test_prompt_contains_also_called_for_distinct_labels(
        self, label_schema: OntologySchema
    ) -> None:
        """When a class's ``rdfs:label`` differs from its ``local_name``,
        the renderer emits an ``(also called: …)`` block next to the
        class or property (Requirement 12.4).

        Our label fixture has ``Car`` labelled ``"Automobile"`` and
        ``Truck`` labelled ``"Lorry"`` — both must appear in the
        rendered prompt as ``(also called: "Automobile")`` style
        blocks (or, equivalently, in the ``(label: …)`` line the
        classes section emits)."""
        text = label_schema.format_as_prompt_constraint()
        # Labels surface via either an ``(also called: …)`` block or
        # the classes section's ``(label: …)`` line — both signals are
        # acceptable per the Requirement 12.4 "labels appear somewhere
        # next to the corresponding element" shape.
        assert "Automobile" in text
        assert "Lorry" in text

    def test_prompt_contains_predicate_spelling_protocol(
        self, schema: OntologySchema
    ) -> None:
        """The verbatim predicate-spelling protocol block is present in
        both prompt variants (Requirement 12.5 — the LLM must be told
        that exact matching wins and modifiers like ``KNOWS_WELL`` are
        rejected)."""
        text = schema.format_as_prompt_constraint()
        # Anchoring on the heading keeps this test resilient to
        # small-prose changes while still catching a regression that
        # removes the block entirely.
        assert "Predicate spelling protocol" in text
        # One of the documented rejection cases mentioned in the block.
        assert "KNOWS_WELL" in text

    # ------------------------------------------------------------------
    # Determinism (Requirement 13, NFR-6) — byte-identical outputs.
    # ------------------------------------------------------------------

    def test_prompt_is_deterministic_same_instance(
        self, schema: OntologySchema
    ) -> None:
        """Two calls on the same instance with the same ``strict_prompt``
        produce byte-identical output (Requirement 13.1, NFR-6) so the
        LLM prompt cache keys stably."""
        a = schema.format_as_prompt_constraint()
        b = schema.format_as_prompt_constraint()
        assert a == b
        # Same for ``strict_prompt=False``.
        a = schema.format_as_prompt_constraint(strict_prompt=False)
        b = schema.format_as_prompt_constraint(strict_prompt=False)
        assert a == b

    def test_prompt_is_deterministic_across_instances(self) -> None:
        """Two :class:`OntologySchema` instances loaded from the same
        Turtle produce byte-identical prompts (Requirement 13.2). This
        pins down that the rendering is a pure function of the ontology
        content — no hidden dependence on dict iteration order, PYTHONHASHSEED,
        or load-time memo state."""
        a = OntologySchema.from_turtle_string(PERSON_EMPLOYEE_COMPANY_TURTLE)
        b = OntologySchema.from_turtle_string(PERSON_EMPLOYEE_COMPANY_TURTLE)
        assert a.format_as_prompt_constraint() == b.format_as_prompt_constraint()
        # And for suggestion mode.
        assert (
            a.format_as_prompt_constraint(strict_prompt=False)
            == b.format_as_prompt_constraint(strict_prompt=False)
        )

    def test_prompt_sorted_by_local_name(self, schema: OntologySchema) -> None:
        """Requirement 13.3 — classes, object properties, and datatype
        properties are emitted sorted by ``local_name`` within each
        section. We assert ordering on the datatype-property section
        (``age`` must appear before ``name`` alphabetically in the
        rendered block) and on the classes section
        (``Company < Employee < Manager < Person``)."""
        text = schema.format_as_prompt_constraint()

        # Datatype properties: ``Person.age`` must appear before
        # ``Person.name`` because the renderer sorts by ``local_name``.
        age_idx = text.index("Person.age")
        name_idx = text.index("Person.name")
        assert age_idx < name_idx, (
            "Expected Person.age to appear before Person.name (sort by "
            f"local_name); got age at {age_idx}, name at {name_idx}"
        )

        # Classes: alphabetical ordering within the classes block means
        # the first occurrence of each class-name substring (there is
        # exactly one class name per class line in the block) is in
        # alphabetical order.
        company_idx = text.index("Company")
        employee_idx = text.index("Employee")
        manager_idx = text.index("Manager")
        # ``Person`` appears both as a class name and inside
        # ``Person.age`` / ``Person.name`` — use the first occurrence,
        # which must be in the classes block since that block is
        # rendered before the datatype-properties block.
        person_idx = text.index("Person")
        assert company_idx < employee_idx < manager_idx < person_idx, (
            "Classes section not sorted by local_name: "
            f"Company={company_idx}, Employee={employee_idx}, "
            f"Manager={manager_idx}, Person={person_idx}"
        )

    # ------------------------------------------------------------------
    # Mode-locality of differences (Requirement 13.4).
    # ------------------------------------------------------------------

    def test_strict_vs_suggestion_only_differ_in_final_paragraph(
        self, schema: OntologySchema
    ) -> None:
        """The only text that differs between ``strict_prompt=True`` and
        ``strict_prompt=False`` is the final-paragraph STRICT/NOTE block
        (Requirement 13.4). Every substring required by Requirement 12
        must appear in both outputs."""
        strict = schema.format_as_prompt_constraint(strict_prompt=True)
        suggest = schema.format_as_prompt_constraint(strict_prompt=False)

        # The outputs differ somewhere.
        assert strict != suggest

        # STRICT MODE marker: in strict only.
        assert "STRICT MODE" in strict
        assert "STRICT MODE" not in suggest

        # NOTE marker: in suggestion only.
        assert "NOTE:" in suggest
        assert "NOTE:" not in strict

        # Every Requirement-12 substring must be in both outputs.
        for required_substring in (
            # Classes (Requirement 12.1)
            "Person",
            "Employee",
            "Manager",
            "Company",
            # Object properties with arrow (Requirement 12.2)
            "worksFor",
            "Employee -> Company",
            # Datatype properties with xsd local name (Requirement 12.3)
            "Person.age",
            "integer",
            "Person.name",
            "string",
            # Predicate-spelling protocol (Requirement 12.5)
            "Predicate spelling protocol",
        ):
            assert required_substring in strict, (
                f"{required_substring!r} missing from strict-mode prompt"
            )
            assert required_substring in suggest, (
                f"{required_substring!r} missing from suggestion-mode prompt"
            )

    def test_strict_vs_suggestion_share_prefix_up_to_final_paragraph(
        self, schema: OntologySchema
    ) -> None:
        """The prefix up to (but not including) the final STRICT/NOTE
        paragraph is byte-identical between the two variants — this is
        the structural corollary of Requirement 13.4. We find the split
        point by locating ``STRICT MODE`` in the strict output and
        ``NOTE:`` in the suggestion output and verify everything before
        each marker matches."""
        strict = schema.format_as_prompt_constraint(strict_prompt=True)
        suggest = schema.format_as_prompt_constraint(strict_prompt=False)

        strict_prefix = strict.split("STRICT MODE", 1)[0]
        suggest_prefix = suggest.split("NOTE:", 1)[0]
        assert strict_prefix == suggest_prefix, (
            "Text before the final paragraph differs between strict and "
            "suggestion prompts; Requirement 13.4 requires only the "
            "final paragraph to differ."
        )


class TestSchemaFilterStageBackwardCompat:
    """Shape-equivalence of the bridged :class:`ExtractionSchema` with a
    hand-built one (Requirement 11.5).

    The bridge must be drop-in for existing call sites — in particular,
    :class:`SchemaFilterStage` must behave identically when handed
    ``ontology.as_extraction_schema()`` vs. a hand-built
    :class:`ExtractionSchema` carrying the same ``entity_types``,
    ``relationship_types``, and ``strict=False`` payload.

    The bridged instance rebinds ``format_as_prompt_constraint`` so the
    LLM prompt *intentionally* differs from the flat schema's rendering
    — that rebind is Requirement 5.2 and is covered by
    :class:`TestAsExtractionSchema`. The invariants asserted here are
    the *other* surfaces :class:`SchemaFilterStage` reads from:
    ``entity_type_names()``, ``relationship_types``, and ``strict``.
    """

    def test_bridged_and_handbuilt_schemas_have_same_shape(
        self, schema: OntologySchema
    ) -> None:
        """Same ``entity_type_names()``, same
        ``set(relationship_types)``, same ``strict`` — the three fields
        :class:`SchemaFilter` reads when deciding whether to filter."""
        es_bridged = schema.as_extraction_schema()
        es_handbuilt = ExtractionSchema(
            entity_types={
                "Person": EntityTypeConfig(description="Person"),
                "Employee": EntityTypeConfig(description="Employee"),
                "Manager": EntityTypeConfig(description="Manager"),
                "Company": EntityTypeConfig(description="Company"),
            },
            relationship_types=["WORKS_FOR"],
            strict=False,
        )
        assert es_bridged.entity_type_names() == es_handbuilt.entity_type_names()
        assert set(es_bridged.relationship_types) == set(
            es_handbuilt.relationship_types
        )
        assert es_bridged.strict == es_handbuilt.strict

    def test_bridged_schema_feeds_schema_filter_stage_identically(
        self, schema: OntologySchema
    ) -> None:
        """End-to-end: hand the bridged schema to :class:`SchemaFilter`
        and compare its output against :class:`SchemaFilter` with a
        hand-built :class:`ExtractionSchema`. Both must produce the
        same pass-through behaviour on non-strict mode — no entities
        or facts are dropped from a crafted :class:`TopicCollection`.

        We import :class:`SchemaFilter` / :class:`SchemaFilterStage`
        inside the test so any future test-collection reordering does
        not accidentally couple this module to a particular import
        order. The import is the same one ``test_schema_filter_stage.py``
        uses, so breakage here would be a real regression of the
        bridge surface."""
        # Local imports — see docstring.
        from llama_index.core.schema import TextNode

        from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
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

        entities = [
            Entity(value="John", classification="Person"),
            Entity(value="Acme", classification="Company"),
        ]
        facts = [
            Fact(
                subject=Entity(value="John", classification="Person"),
                predicate=Relation(value="WORKS_FOR"),
                object=Entity(value="Acme", classification="Company"),
            ),
        ]
        tc = TopicCollection(
            topics=[
                Topic(
                    value="test topic",
                    entities=entities,
                    statements=[Statement(value="test stmt", facts=facts)],
                )
            ]
        )

        def _make_node() -> TextNode:
            # Build a fresh node per schema so the two ``SchemaFilter``
            # calls cannot accidentally share mutable state via
            # ``node.metadata``.
            node = TextNode(text="test")
            node.metadata[TOPICS_KEY] = tc.model_dump()
            return node

        bridged_node = _make_node()
        handbuilt_node = _make_node()

        es_bridged = schema.as_extraction_schema()
        es_handbuilt = ExtractionSchema(
            entity_types={
                "Person": EntityTypeConfig(description="Person"),
                "Employee": EntityTypeConfig(description="Employee"),
                "Manager": EntityTypeConfig(description="Manager"),
                "Company": EntityTypeConfig(description="Company"),
            },
            relationship_types=["WORKS_FOR"],
            strict=False,
        )

        bridged_result = SchemaFilter(extraction_schema=es_bridged)([bridged_node])
        handbuilt_result = SchemaFilter(extraction_schema=es_handbuilt)(
            [handbuilt_node]
        )

        # Both schemas have ``strict=False``, so :class:`SchemaFilter`
        # short-circuits and returns the nodes unchanged — the two
        # output collections must therefore be byte-equivalent.
        bridged_tc = TopicCollection(**bridged_result[0].metadata[TOPICS_KEY])
        handbuilt_tc = TopicCollection(
            **handbuilt_result[0].metadata[TOPICS_KEY]
        )
        assert bridged_tc.model_dump() == handbuilt_tc.model_dump()


# ---------------------------------------------------------------------------
# Property-based tests — Tasks 3.8, 3.9, 3.10
#
# The three sub-sections below all rest on a single richer Hypothesis
# strategy, ``ontology_strategy``, that extends the DAG generator from
# Task 2.6 with object and datatype properties. A matching Turtle-rendering
# helper (``_full_ontology_turtle``) turns the generated dict into a valid
# Turtle document that ``OntologySchema.from_turtle_string`` can parse.
#
# Tasks 3.8 (Property P1) and 3.10 (Property P9) are pure
# :class:`OntologySchema` properties. Task 3.9 (Property P8) crosses the
# bridge into :class:`SchemaFilter` to verify that the bridged
# :class:`ExtractionSchema` is observationally identical to a hand-built
# one with the same shape — critical for drop-in backward compat with
# existing ``SchemaFilterStage`` call sites.
#
# Design §"Correctness Properties":
#
#   * P1 (3.8): ``as_extraction_schema`` round-trip preserves class and
#     predicate names.
#   * P8 (3.9): ``SchemaFilter(bridged)`` ≡ ``SchemaFilter(handbuilt)``
#     over any topic collection (strict=False pass-through case, which
#     is the only case the bridged schema supports per Requirement 11.3).
#   * P9 (3.10): prompt surfaces the full ontology vocabulary and is
#     deterministic; strict-vs-suggestion diffs are confined to the
#     final paragraph.
# ---------------------------------------------------------------------------


# Valid XSD local names recognised by ``_validate_literal_against_xsd`` and
# rendered directly by ``_render_datatype_properties_block`` via
# ``_xsd_local_name_of``. Keeping the palette small and fixed guarantees
# every generated ontology loads successfully (Requirement 3.4) and keeps
# the datatype-property rendering predictable so Task 3.10's substring
# assertions are stable.
_XSD_TYPES = [
    "string",
    "integer",
    "boolean",
    "decimal",
    "date",
    "dateTime",
]


@st.composite
def ontology_strategy(
    draw,
    min_classes: int = 1,
    max_classes: int = 5,
    max_op: int = 3,
    max_dp: int = 3,
):
    """Generate a valid :class:`OntologySchema` shape with classes,
    object properties, and datatype properties.

    Returns a dict with three keys:

    * ``classes`` — as produced by :func:`dag_ontology_strategy`, i.e.
      ``{class_local_name: [parent_local_names]}``.
    * ``obj_props`` — ``{op_local_name: (domain_list, range_list)}``.
      Each entry declares exactly one domain class and one range
      class drawn from ``classes`` (the cartesian product is the
      minimum structure the prompt renderer and
      ``SchemaFilter`` both need to exercise non-trivially).
    * ``dt_props`` — ``{dp_local_name: (domain_list, xsd_local_name)}``.
      Domain is a single class drawn from ``classes``; the XSD range
      is drawn from :data:`_XSD_TYPES`.

    Generation rules:

    * At least one class always exists (``min_classes=1``), so every
      object and datatype property can pick a valid domain/range.
    * Object and datatype properties may be absent entirely (counts
      drawn from ``[0, max_*]``) so the prompt-emptiness paths still
      see coverage.
    * Object-property local names are ``op0, op1, ...`` and
      datatype-property local names are ``dp0, dp1, ...`` — these
      cannot collide with class names (``C0, C1, ...``), nor can
      they collide across the object/datatype split. Unique lowercase
      local names guarantee the ``_by_local_name`` / predicate
      indexes do not mask any property, which in turn makes the
      round-trip assertions in Task 3.8 exact.

    The three counts (classes, op, dp) are capped so that the
    quadratic assertions in Task 3.10 (class substring checks,
    domain/range arrow checks, datatype-property checks) run quickly
    across Hypothesis's 30–50-example budgets.
    """
    # Reuse the DAG strategy from Task 2.6 for the class hierarchy —
    # this keeps the class-generation logic in one place and ensures
    # the shared ``_dag_ontology_turtle`` / ``_full_ontology_turtle``
    # helpers render the class portion identically.
    classes_and_parents = draw(
        dag_ontology_strategy(
            min_classes=min_classes, max_classes=max_classes
        )
    )
    class_names = list(classes_and_parents.keys())

    # Object properties. Each declares one domain class and one range
    # class drawn from ``class_names``. Cardinality of 1/1 is
    # deliberately minimal: the prompt renderer emits one
    # ``{d} -> {r}`` line per (d, r) pair and Task 3.10 only needs at
    # least one such pair per property to be present.
    n_op = draw(st.integers(min_value=0, max_value=min(max_op, 3)))
    obj_props: Dict[str, Tuple[List[str], List[str]]] = {}
    for i in range(n_op):
        op_name = f"op{i}"
        domain = [draw(st.sampled_from(class_names))]
        range_ = [draw(st.sampled_from(class_names))]
        obj_props[op_name] = (domain, range_)

    # Datatype properties. Each declares one domain class and exactly
    # one XSD type from the fixed palette. The fixed palette keeps
    # the xsd-local-name substring assertions in Task 3.10 stable —
    # every generated ontology will emit exactly one of six xsd
    # names, all of which are safe substrings.
    n_dp = draw(st.integers(min_value=0, max_value=min(max_dp, 3)))
    dt_props: Dict[str, Tuple[List[str], str]] = {}
    for i in range(n_dp):
        dp_name = f"dp{i}"
        domain = [draw(st.sampled_from(class_names))]
        xsd_type = draw(st.sampled_from(_XSD_TYPES))
        dt_props[dp_name] = (domain, xsd_type)

    return {
        "classes": classes_and_parents,
        "obj_props": obj_props,
        "dt_props": dt_props,
    }


def _full_ontology_turtle(ontology_dict) -> str:
    """Render the ``ontology_strategy`` dict as a Turtle document.

    Extends :func:`_dag_ontology_turtle` with object-property and
    datatype-property blocks. The document always declares the ``owl``,
    ``rdfs``, and ``xsd`` prefixes so the datatype-property ranges
    (``xsd:string``, ``xsd:integer``, etc.) resolve during rdflib
    parsing (Requirement 3.4 — DatatypeProperty ranges must be in the
    XSD namespace).

    The produced Turtle is deterministic given the input dict:
    classes, object properties, and datatype properties are emitted in
    the dict's insertion order. Python 3.7+ dict iteration order is
    insertion order, so repeated invocations with the same input
    produce byte-identical output — important for Task 3.10's
    deterministic-across-instances assertion, which loads the same
    Turtle text twice.
    """
    lines = [
        f"@prefix : <{_PBT_NAMESPACE}> .",
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "",
    ]

    # Classes — identical shape to _dag_ontology_turtle's block per
    # class, with direct ``rdfs:subClassOf`` declarations in input
    # order. Blank-node parents are impossible here: every parent is
    # a plain class local name.
    for cls, parents in ontology_dict["classes"].items():
        block = [f":{cls} a owl:Class"]
        for p in parents:
            block.append(f"    rdfs:subClassOf :{p}")
        lines.append(" ;\n".join(block) + " .")

    # Object properties. One domain and range declaration per entry in
    # the corresponding lists (the strategy only generates single-entry
    # lists in v1, but the loop form is portable if the strategy
    # widens later).
    for op_name, (domain, range_) in ontology_dict["obj_props"].items():
        block = [f":{op_name} a owl:ObjectProperty"]
        for d in domain:
            block.append(f"    rdfs:domain :{d}")
        for r in range_:
            block.append(f"    rdfs:range :{r}")
        lines.append(" ;\n".join(block) + " .")

    # Datatype properties. Exactly one XSD range per property — the
    # strategy never generates multi-range datatype properties because
    # v1 only supports single-range (Requirement 3.4).
    for dp_name, (domain, xsd_type) in ontology_dict["dt_props"].items():
        block = [f":{dp_name} a owl:DatatypeProperty"]
        for d in domain:
            block.append(f"    rdfs:domain :{d}")
        block.append(f"    rdfs:range xsd:{xsd_type}")
        lines.append(" ;\n".join(block) + " .")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Task 3.8 — Property P1: as_extraction_schema round-trip.
# ---------------------------------------------------------------------------


class TestAsExtractionSchemaProperties:
    """Property P1: ``as_extraction_schema`` round-trip preserves class
    and predicate names.

    Validates: Requirements 11.1, 11.2.

    For any generated ontology, the bridged :class:`ExtractionSchema`
    must expose ``entity_types`` keys equal to ``{c.local_name for c in
    O.classes.values()}`` (Requirement 11.1) and ``relationship_types``
    equal to ``{UPPER_SNAKE(op.local_name) for op in
    O.object_properties.values()}`` (Requirement 11.2). These two
    invariants together prove the bridge never silently drops a class
    or a property, regardless of ontology shape.
    """

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_entity_types_keys_are_class_local_names(self, ontology_dict):
        """Bridged ``entity_types`` keys ≡ class local names (Req 11.1)."""
        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        bridged = schema.as_extraction_schema()

        expected_class_names = {c.local_name for c in schema.classes.values()}
        assert set(bridged.entity_types.keys()) == expected_class_names, (
            f"entity_types keys = {set(bridged.entity_types.keys())!r}, "
            f"expected {expected_class_names!r}"
        )

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_relationship_types_are_upper_snake(self, ontology_dict):
        """Bridged ``relationship_types`` ≡ ``UPPER_SNAKE(op.local_name)``
        for every declared object property (Req 11.2).

        Imported inside the test to keep the top of the module free of
        production-side helpers — the suite is split between
        production-path tests (which do import these helpers at module
        scope) and the PBT section here, which deliberately localises
        helper imports to make it obvious which property each test
        depends on.
        """
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology_schema import (
            _camel_to_upper_snake,
        )

        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        bridged = schema.as_extraction_schema()

        expected_rel = {
            _camel_to_upper_snake(op.local_name)
            for op in schema.object_properties.values()
        }
        assert set(bridged.relationship_types) == expected_rel, (
            f"relationship_types = {set(bridged.relationship_types)!r}, "
            f"expected {expected_rel!r}"
        )


# ---------------------------------------------------------------------------
# Task 3.9 — Property P8: ExtractionSchema backward compatibility.
# ---------------------------------------------------------------------------


class TestSchemaFilterBackwardCompatProperties:
    """Property P8: :class:`ExtractionSchema` backward compatibility.

    Validates: Requirement 11.5.

    For any generated ontology, running ``SchemaFilter`` over a fixed
    :class:`TopicCollection` with ``ontology.as_extraction_schema()``
    must produce the same output as running ``SchemaFilter`` with a
    hand-built :class:`ExtractionSchema` carrying the same
    ``entity_types`` keys, the same ``relationship_types``, and
    ``strict=False``. The bridged schema always has ``strict=False``
    per Requirement 11.3, so :class:`SchemaFilter` short-circuits to a
    pass-through in both runs — but asserting the pass-through
    equivalence across arbitrarily-shaped ontologies pins down that
    the bridge does not introduce any side-effect (stray metadata
    mutation, entity rewriting, etc.) that a hand-built schema would
    not.

    The stronger "strict=True" equivalence is not assertable at this
    level because the bridged schema's ``strict`` is hard-coded to
    ``False`` (Requirement 11.3 — strict enforcement lives in
    :class:`OntologyFilterStage`).
    """

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_schema_filter_equivalence(self, ontology_dict):
        """``SchemaFilter(bridged)`` ≡ ``SchemaFilter(handbuilt)`` on a
        fixed :class:`TopicCollection` input (Req 11.5)."""
        from llama_index.core.schema import TextNode

        from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology_schema import (
            _camel_to_upper_snake,
        )
        from graphrag_toolkit.lexical_graph.indexing.extract.stages.schema_filter_stage import (
            SchemaFilter,
        )
        from graphrag_toolkit.lexical_graph.indexing.model import (
            Entity,
            Statement,
            Topic,
            TopicCollection,
        )

        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        bridged = schema.as_extraction_schema()

        # Hand-built schema with the same shape but minimal
        # ``EntityTypeConfig`` (no description/aliases) — this
        # deliberately differs from the bridged instance on the *shape*
        # of the EntityTypeConfig values and only agrees on the
        # ``entity_types`` keys, ``relationship_types`` set, and
        # ``strict=False``. That minimal shape is exactly what
        # :class:`SchemaFilter` reads (via ``entity_type_names()``,
        # ``relationship_types``, and ``strict``), so asserting
        # observational equivalence pins down the invariant without
        # accidentally asserting deeper structural equality.
        handbuilt = ExtractionSchema(
            entity_types={
                cls_.local_name: EntityTypeConfig()
                for cls_ in schema.classes.values()
            },
            relationship_types=[
                _camel_to_upper_snake(op.local_name)
                for op in schema.object_properties.values()
            ],
            strict=False,
        )

        # Fixed :class:`TopicCollection` input. We pick one arbitrary
        # class from the ontology (if any) to use as an entity
        # classification so the input is not entirely trivial — but
        # :class:`SchemaFilter` with ``strict=False`` short-circuits
        # regardless, so the content of the collection only matters
        # for shaping a realistic pass-through. An empty classes set
        # would only happen if ``min_classes=0``; our strategy
        # forces ``min_classes=1`` so the ``next(iter(...))`` is safe.
        first_class_name = next(iter(schema.classes.values())).local_name
        tc = TopicCollection(
            topics=[
                Topic(
                    value="t",
                    entities=[
                        Entity(value="E", classification=first_class_name)
                    ],
                    statements=[Statement(value="s", facts=[])],
                )
            ]
        )

        def _make_node():
            # Build a fresh node per schema so the two
            # :class:`SchemaFilter` calls cannot accidentally share
            # mutable state via ``node.metadata``.
            node = TextNode(text="t")
            node.metadata[TOPICS_KEY] = tc.model_dump()
            return node

        bridged_result = SchemaFilter(extraction_schema=bridged)([_make_node()])
        handbuilt_result = SchemaFilter(extraction_schema=handbuilt)(
            [_make_node()]
        )

        # Asserting on the ``TOPICS_KEY`` payload directly catches any
        # entity / statement / fact mutation either filter might
        # perform; comparing the full ``node.metadata`` would also
        # catch key drift, but :class:`SchemaFilter` never writes any
        # other key so the topics-only comparison is sufficient and
        # closer to the requirement's wording.
        assert (
            bridged_result[0].metadata[TOPICS_KEY]
            == handbuilt_result[0].metadata[TOPICS_KEY]
        )


# ---------------------------------------------------------------------------
# Task 3.10 — Property P9: prompt vocabulary completeness and determinism.
# ---------------------------------------------------------------------------


class TestPromptPropertyPreservation:
    """Property P9: prompt surfaces the full ontology vocabulary
    deterministically.

    Validates: Requirements 12.1, 12.2, 12.3, 12.4, 13.1, 13.2, 13.3, 13.4.

    Split across several focused tests so Hypothesis's shrinker can
    localise failures to the specific property that broke. Each test
    draws a fresh ontology, loads it, and asserts one invariant:

    * Class local-name completeness (Req 12.1).
    * Object-property ``local_name`` and domain→range arrow
      completeness (Req 12.2).
    * Datatype-property ``{Class.attribute, xsd-type}`` completeness
      (Req 12.3).
    * Byte-identical output across repeated calls on the same instance
      (Req 13.1, NFR-6).
    * Byte-identical output across independently-loaded instances of
      the same Turtle text (Req 13.2).
    * Mode-locality of the strict-vs-suggestion difference (Req 13.4).

    Requirement 12.4 (``also called`` block) is not directly asserted
    here because the strategy does not emit labels; the unit-test
    fixture :class:`TestFormatAsPromptConstraint` in the preceding
    section covers that case on a concrete label-carrying ontology.
    Requirement 13.3 (sort order) is covered indirectly — the
    deterministic-output assertions would fail if the renderer did not
    use a stable sort, because Python dict iteration order is
    insertion-order-preserving but not stable across independent
    parses of the same Turtle text in rdflib.
    """

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=40, suppress_health_check=[HealthCheck.too_slow])
    def test_prompt_contains_all_class_local_names(self, ontology_dict):
        """Every class's ``local_name`` appears as a substring of the
        rendered prompt (Req 12.1)."""
        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        text = schema.format_as_prompt_constraint()
        for cls_ in schema.classes.values():
            assert cls_.local_name in text, (
                f"Class local_name {cls_.local_name!r} missing from "
                f"prompt output"
            )

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=40, suppress_health_check=[HealthCheck.too_slow])
    def test_prompt_contains_all_object_property_arrows(self, ontology_dict):
        """Every :class:`ObjectProperty` surfaces its ``local_name`` and
        at least one ``{d_local} -> {r_local}`` pair derived from its
        domain/range after local-name resolution (Req 12.2).

        The renderer emits one line per ``(d, r) ∈ domain × range``;
        asserting on *at least one* such line is equivalent to
        Requirement 12.2's "at least one (d, r) pair" wording. Empty
        domain / range fall back to ``owl:Thing`` in the rendered
        prompt — our strategy never generates empty domains/ranges so
        that fallback path is not exercised here (the unit-test
        fixture in :class:`TestFormatAsPromptConstraint` covers
        distinct concrete cases).
        """
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology_schema import (
            _local_name_of,
        )

        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        text = schema.format_as_prompt_constraint()
        for op in schema.object_properties.values():
            # The bare local_name appears (it is the line's predicate
            # column in ``_render_object_properties_block``).
            assert op.local_name in text, (
                f"ObjectProperty local_name {op.local_name!r} missing"
            )
            # At least one domain→range pair via local names. Empty
            # domain/range fall back to the ``owl:Thing`` literal,
            # matching the renderer's fallback behaviour so the
            # substring is still present for pathological ontologies.
            domains = [_local_name_of(d) for d in op.domain] or ["owl:Thing"]
            ranges = [_local_name_of(r) for r in op.range] or ["owl:Thing"]
            found = any(
                f"{d} -> {r}" in text for d in domains for r in ranges
            )
            assert found, (
                f"No {{d}} -> {{r}} pair from {op.local_name!r} in "
                f"prompt; domains={domains!r}, ranges={ranges!r}"
            )

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=40, suppress_health_check=[HealthCheck.too_slow])
    def test_prompt_contains_all_datatype_properties(self, ontology_dict):
        """Every :class:`DatatypeProperty` surfaces
        ``{d_local}.{dp.local_name}`` and its xsd-local-name for every
        ``d ∈ dp.domain`` (Req 12.3).

        The renderer emits one ``{d_local}.{dp.local_name} :
        {xsd_local}`` line per domain class; we assert both substrings
        appear in the output text. Because our strategy always
        generates at least one domain per datatype property, the
        ``owl:Thing`` fallback path is not exercised here — that path
        is covered by the unit-test fixture in
        :class:`TestFormatAsPromptConstraint`.
        """
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology_schema import (
            _local_name_of,
        )

        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        text = schema.format_as_prompt_constraint()
        for dp in schema.datatype_properties.values():
            xsd_local = _local_name_of(dp.datatype)
            domains = [_local_name_of(d) for d in dp.domain] or ["owl:Thing"]
            for d_local in domains:
                # ``{d_local}.{dp.local_name}`` is the left side of the
                # rendered datatype-property line.
                expected = f"{d_local}.{dp.local_name}"
                assert expected in text, (
                    f"{expected!r} missing from prompt"
                )
            # The xsd-local-name is on the same line as at least one
            # of the ``{d_local}.{dp.local_name}`` substrings. The
            # exact "same-line" check is harder to make generic without
            # splitting the text, so we assert the xsd-local-name is
            # simply present — a regression that moves it off the
            # datatype-property lines would also show up in the
            # unit-test fixture's assertions.
            assert xsd_local in text, (
                f"XSD local name {xsd_local!r} for {dp.local_name!r} "
                f"missing from prompt"
            )

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_prompt_deterministic_same_instance(self, ontology_dict):
        """Two calls on the same :class:`OntologySchema` instance with
        the same ``strict_prompt`` produce byte-identical output
        (Req 13.1, NFR-6).

        Both ``strict_prompt=True`` and ``strict_prompt=False`` are
        checked in the same test so a regression in either branch is
        surfaced by the same counterexample.
        """
        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        assert (
            schema.format_as_prompt_constraint()
            == schema.format_as_prompt_constraint()
        )
        assert (
            schema.format_as_prompt_constraint(strict_prompt=False)
            == schema.format_as_prompt_constraint(strict_prompt=False)
        )

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_prompt_deterministic_across_instances(self, ontology_dict):
        """Two independently-loaded instances of the same Turtle produce
        byte-identical prompts (Req 13.2).

        Indirectly validates the stable-sort guarantee in
        Requirement 13.3: because rdflib's internal triple-set does
        not guarantee iteration order across independent parses of the
        same document, the rendered prompt can only be byte-identical
        if the renderer sorts every section by ``local_name`` before
        emitting it.
        """
        turtle = _full_ontology_turtle(ontology_dict)
        a = OntologySchema.from_turtle_string(turtle)
        b = OntologySchema.from_turtle_string(turtle)
        assert a.format_as_prompt_constraint() == b.format_as_prompt_constraint()
        assert (
            a.format_as_prompt_constraint(strict_prompt=False)
            == b.format_as_prompt_constraint(strict_prompt=False)
        )

    @given(ontology_dict=ontology_strategy())
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_strict_vs_suggestion_differ_only_in_final_paragraph(
        self, ontology_dict
    ):
        """The prefix up to the final paragraph is byte-identical
        between ``strict_prompt=True`` and ``strict_prompt=False``
        outputs (Req 13.4).

        We find the split point by locating ``"STRICT MODE"`` in the
        strict output and ``"NOTE:"`` in the suggestion output — both
        markers are module-level constants in
        :mod:`ontology_schema` (``_STRICT_MODE_FINAL_PARAGRAPH`` and
        ``_SUGGESTION_MODE_FINAL_PARAGRAPH`` respectively) and neither
        marker appears anywhere else in the rendered prompt, so the
        split is unambiguous.
        """
        turtle = _full_ontology_turtle(ontology_dict)
        schema = OntologySchema.from_turtle_string(turtle)
        strict = schema.format_as_prompt_constraint(strict_prompt=True)
        suggest = schema.format_as_prompt_constraint(strict_prompt=False)

        strict_prefix = strict.split("STRICT MODE", 1)[0]
        suggest_prefix = suggest.split("NOTE:", 1)[0]
        assert strict_prefix == suggest_prefix, (
            "Text before the final paragraph differs between strict "
            "and suggestion prompts; Requirement 13.4 requires only "
            "the final paragraph to differ."
        )
