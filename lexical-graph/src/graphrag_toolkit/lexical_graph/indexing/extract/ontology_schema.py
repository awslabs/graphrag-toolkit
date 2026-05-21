# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Ontology schema module for ontology-guided extraction (suggestion mode).

This module holds the **suggestion-mode-only** surface of the
ontology-guided-extraction feature. It exposes the ``OntologySchema`` dataclass
(to be added in subsequent tasks) along with its lightweight building blocks
(``OntologyClass``, ``ObjectProperty``, ``DatatypeProperty``) and the
``OntologyLoadError`` exception raised when a user-provided Turtle file is
structurally invalid.

Everything in this module is safe to import and use without activating
strict-mode enforcement. Strict-mode behaviour (post-extraction filtering of
entities and facts, XSD literal validation, the ``validate_datatypes``
opt-out) lives exclusively in the sibling ``stages/ontology_filter_stage.py``
module. The separation is deliberate: per Requirement 14 (NFR-7), strict-mode
code must be removable or disable-able as a self-contained unit without
touching this file.

Design constraints honoured here:

* ``rdflib`` is a **soft dependency** (Requirement 4, design §D1). It MUST NOT
  be imported at module top level. The loader class methods
  (``OntologySchema.from_turtle`` and ``OntologySchema.from_turtle_string``,
  added in later tasks) will import ``rdflib`` lazily inside their own bodies
  so that callers who never parse a Turtle file do not need ``rdflib``
  installed for the rest of the toolkit to import and run.
* This module MUST NOT import from ``ontology_filter_stage`` or any
  strict-mode helper (Requirement 14 criterion 3).
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Optional, Set, Tuple, Union

if TYPE_CHECKING:  # pragma: no cover - typing aid only, no runtime rdflib import
    import rdflib

# Module-level logger used by ``_build_name_indexes`` to report first-declared-
# wins collisions on class local-name and label indexes at DEBUG level
# (Requirement 1.3). Kept at module scope so callers can attach handlers or
# raise the verbosity for this module without monkey-patching.
logger = logging.getLogger(__name__)

# XSD namespace used for DatatypeProperty range validation and for rendering
# xsd-local names in the prompt constraint block (design §"Algorithm:
# datatype literal validation", Requirement 10).
XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema#"


class OntologyLoadError(Exception):
    """Raised when a user-provided Turtle file cannot be loaded as an ontology.

    Signals a structural problem detected at load time: ``rdfs:subClassOf``
    cycles, dangling class or property references, a property declared as both
    ``owl:ObjectProperty`` and ``owl:DatatypeProperty``, a ``DatatypeProperty``
    with a missing or non-XSD ``rdfs:range``, or an undeterminable base IRI.
    When raised, no partial ``OntologySchema`` instance is returned to the
    caller (Requirement 3.6).
    """


@dataclass
class OntologyClass:
    """An OWL class declared in the user-provided Turtle file.

    Fields:
        iri: The full class IRI, e.g. ``"https://example.com/kg/Person"``.
        local_name: The last path segment of ``iri``, split on ``#`` or ``/``.
            Used by :meth:`OntologySchema.resolve_class` for case-insensitive
            matching against LLM-emitted classification strings.
        label: The ``rdfs:label`` literal value, if declared. ``None`` when no
            label was present on the class in the Turtle file.
        parents: Direct ``rdfs:subClassOf`` target IRIs in declaration order.
            Blank-node parents are filtered out at load time so every entry
            here is the IRI of a declared :class:`OntologyClass`
            (Requirement 1.4).
        ancestors: Transitive closure of ``parents`` including ``iri`` itself
            (reflexive). Populated at load time by
            ``_compute_subclass_closure`` so that
            :meth:`OntologySchema.is_subclass_of` is an O(1) frozenset
            containment check (Requirement 2, NFR-2). Defaults to an empty
            frozenset so constructors invoked before closure computation do
            not need to pass it.
    """

    iri: str
    local_name: str
    label: Optional[str]
    parents: List[str] = field(default_factory=list)
    ancestors: FrozenSet[str] = field(default_factory=frozenset)


@dataclass
class ObjectProperty:
    """An ``owl:ObjectProperty`` (entity-to-entity predicate) from the ontology.

    Fields:
        iri: The full property IRI.
        local_name: The last path segment of ``iri``; used for predicate
            matching by :meth:`OntologySchema.resolve_object_predicate`.
        label: The ``rdfs:label`` literal value, if declared.
        domain: Declared ``rdfs:domain`` class IRIs in declaration order. An
            empty list is treated as ``owl:Thing`` by the strict-mode filter
            (matches any subject class).
        range: Declared ``rdfs:range`` class IRIs in declaration order. An
            empty list is treated as ``owl:Thing`` (matches any object class).
    """

    iri: str
    local_name: str
    label: Optional[str]
    domain: List[str] = field(default_factory=list)
    range: List[str] = field(default_factory=list)


@dataclass
class DatatypeProperty:
    """An ``owl:DatatypeProperty`` (entity-to-literal predicate) from the ontology.

    Fields:
        iri: The full property IRI.
        local_name: The last path segment of ``iri``; used for predicate
            matching by :meth:`OntologySchema.resolve_datatype_predicate`.
        label: The ``rdfs:label`` literal value, if declared.
        domain: Declared ``rdfs:domain`` class IRIs in declaration order.
        datatype: A single XSD IRI (in the ``XSD_NAMESPACE`` namespace) that
            specifies the expected literal type of the complement in an SPC
            fact. Exactly one datatype per property is supported in v1;
            multi-range semantics are out of scope (design §"Algorithm:
            Turtle parsing", Requirement 3.4).
    """

    iri: str
    local_name: str
    label: Optional[str]
    domain: List[str] = field(default_factory=list)
    datatype: str = ""


@dataclass
class OntologySchema:
    """In-memory model of a parsed OWL/Turtle ontology.

    This dataclass is the single source of truth for ontology state within a
    pipeline run. Instances are produced by ``from_turtle`` /
    ``from_turtle_string`` (added in later tasks) and are effectively
    immutable afterwards — callers should not mutate any field.

    Public fields:
        namespace: The base IRI detected from the Turtle ``@prefix :``
            declaration, ``rdflib.namespace.DefaultNamespace``, or the first
            ``owl:Ontology`` subject. A non-empty value is required; absence
            raises :class:`OntologyLoadError` at load time (Requirement 3.5).
        classes: Map keyed by class IRI to :class:`OntologyClass`.
        object_properties: Map keyed by property IRI to
            :class:`ObjectProperty`.
        datatype_properties: Map keyed by property IRI to
            :class:`DatatypeProperty`.

    Internal (load-time) indexes, all defaulted so callers can construct
    minimally and have ``from_turtle*`` populate them:
        _by_local_name: Lowercase ``local_name`` → class IRI. Drives
            :meth:`resolve_class` case-insensitive lookup.
        _by_label: Lowercase ``rdfs:label`` → class IRI. Second fallback for
            :meth:`resolve_class`.
        _obj_predicate_index: Lowercase local_name / normalized predicate
            name → list of :class:`ObjectProperty` IRIs that match. Drives
            :meth:`resolve_object_predicate`.
        _dt_predicate_index: Lowercase local_name / normalized predicate name
            → list of :class:`DatatypeProperty` IRIs that match. Drives
            :meth:`resolve_datatype_predicate`.
    """

    namespace: str = ""
    classes: Dict[str, OntologyClass] = field(default_factory=dict)
    object_properties: Dict[str, ObjectProperty] = field(default_factory=dict)
    datatype_properties: Dict[str, DatatypeProperty] = field(default_factory=dict)

    # Internal indexes — populated by ``_build_name_indexes`` at load time.
    _by_local_name: Dict[str, str] = field(default_factory=dict)
    _by_label: Dict[str, str] = field(default_factory=dict)
    _obj_predicate_index: Dict[str, List[str]] = field(default_factory=dict)
    _dt_predicate_index: Dict[str, List[str]] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Loaders (soft rdflib import — Requirement 4 / design §D1)
    # ------------------------------------------------------------------

    @classmethod
    def from_turtle(cls, path: Union[str, Path]) -> "OntologySchema":
        """Parse an OWL/Turtle file from disk into an :class:`OntologySchema`.

        This is the primary entry point for ontology-guided extraction. The
        method lazily imports ``rdflib`` inside its body so that callers who
        never touch ontologies do not need ``rdflib`` installed for the rest
        of the toolkit to import and run (Requirement 4.1, 4.2, NFR-1).

        Args:
            path: Filesystem path to a Turtle file. Accepts ``str`` or
                :class:`pathlib.Path`.

        Returns:
            A fully-populated :class:`OntologySchema` whose ``classes``,
            ``object_properties``, ``datatype_properties``, derived
            ``ancestors``, and name indexes are all set.

        Raises:
            ImportError: If ``rdflib`` is not installed. The message names
                the dependency and how to install it (Requirement 4.3).
            OntologyLoadError: If the Turtle file is structurally invalid
                per this feature's rules (Requirement 3). No partial
                :class:`OntologySchema` instance is returned on error
                (Requirement 3.6).
        """
        try:
            import rdflib
        except ImportError as exc:  # pragma: no cover - exercised in integration env
            raise ImportError(
                "rdflib is required to load an ontology from Turtle. "
                "Install it with: pip install 'rdflib>=6.0,<8.0'"
            ) from exc

        rdf_graph = rdflib.Graph()
        # rdflib accepts str or os.PathLike for `source`; normalize to str so
        # Path instances work on all supported Python versions.
        try:
            rdf_graph.parse(source=str(path), format="turtle")
        except OntologyLoadError:
            raise
        except Exception as exc:
            raise OntologyLoadError(
                f"Failed to parse Turtle file {path!s}: {exc}"
            ) from exc

        return cls._parse_graph(rdf_graph)

    @classmethod
    def from_turtle_string(
        cls, turtle: str, base_iri: Optional[str] = None
    ) -> "OntologySchema":
        """Parse an OWL/Turtle document from an in-memory string.

        Equivalent to :meth:`from_turtle` but accepts the Turtle text
        directly. Useful for tests and for callers that already hold the
        Turtle content (e.g. fetched from a catalog service).

        Args:
            turtle: The Turtle document text.
            base_iri: Optional base IRI to pass through to ``rdflib`` as
                ``publicID``. When provided, this is used both to resolve
                relative IRIs during parsing and as a fallback base-IRI
                hint when Turtle ``@prefix :`` and ``owl:Ontology`` subjects
                are absent.

        Returns:
            A fully-populated :class:`OntologySchema`.

        Raises:
            ImportError: If ``rdflib`` is not installed (Requirement 4.3).
            OntologyLoadError: If the Turtle is structurally invalid
                (Requirement 3); no partial instance is returned.
        """
        try:
            import rdflib
        except ImportError as exc:  # pragma: no cover - exercised in integration env
            raise ImportError(
                "rdflib is required to load an ontology from Turtle. "
                "Install it with: pip install 'rdflib>=6.0,<8.0'"
            ) from exc

        rdf_graph = rdflib.Graph()
        try:
            rdf_graph.parse(data=turtle, format="turtle", publicID=base_iri)
        except OntologyLoadError:
            raise
        except Exception as exc:
            raise OntologyLoadError(
                f"Failed to parse Turtle string: {exc}"
            ) from exc

        return cls._parse_graph(rdf_graph, base_iri_hint=base_iri)

    # ------------------------------------------------------------------
    # Internal parse pipeline
    # ------------------------------------------------------------------

    @classmethod
    def _parse_graph(
        cls,
        rdf_graph: "rdflib.Graph",
        base_iri_hint: Optional[str] = None,
    ) -> "OntologySchema":
        """Build an :class:`OntologySchema` from a parsed ``rdflib.Graph``.

        Follows the pseudocode under design §"Algorithm: Turtle parsing
        (`from_turtle`)". All steps are performed in sequence; any raised
        :class:`OntologyLoadError` aborts the build before the name-index
        step runs so that callers never receive a partially-populated
        schema (Requirement 3.6).

        Task-scoping note: this task (2.1) implements steps 1–5 (load,
        base-IRI detection, class / object-property / datatype-property
        collection). Steps 6–9 (subclass closure, dangling-ref
        validation, name-index building) are wired through to
        module-level stubs that later tasks (2.2 – 2.4) will replace.
        """
        # rdflib is already imported by the public entry points; fetch it
        # from sys.modules to avoid a second top-level import.
        import rdflib  # type: ignore[import-not-found]
        from rdflib import BNode

        OWL = rdflib.Namespace("http://www.w3.org/2002/07/owl#")
        RDFS = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
        RDF = rdflib.RDF

        # ---- Step 2: determine namespace (base IRI) ----
        namespace = _extract_base_iri(rdf_graph, base_iri_hint=base_iri_hint)
        if not namespace:
            raise OntologyLoadError(
                "Could not determine base IRI: Turtle has no '@prefix :', no "
                "rdflib default namespace, and no owl:Ontology subject."
            )

        def _label_of(subj) -> Optional[str]:
            lit = next(iter(rdf_graph.objects(subj, RDFS.label)), None)
            return str(lit) if lit is not None else None

        # ---- Step 3: collect classes ----
        classes: Dict[str, OntologyClass] = {}
        for subj in rdf_graph.subjects(RDF.type, OWL.Class):
            if isinstance(subj, BNode):
                # Anonymous class axioms (e.g. owl:Restriction bodies) are not
                # user-declared named classes; skip them so we only surface
                # named classes the user can reference from domain/range.
                continue
            iri = str(subj)
            # Parents in declaration order; blank-node parents filtered out
            # per Requirement 1.4 / design "Step 3".
            parents: List[str] = [
                str(obj)
                for obj in rdf_graph.objects(subj, RDFS.subClassOf)
                if not isinstance(obj, BNode)
            ]
            classes[iri] = OntologyClass(
                iri=iri,
                local_name=_local_name_of(iri),
                label=_label_of(subj),
                parents=parents,
                # ancestors stays empty until step 7 (Task 2.2) fills it in.
            )

        # ---- Step 4: collect object properties ----
        object_properties: Dict[str, ObjectProperty] = {}
        for subj in rdf_graph.subjects(RDF.type, OWL.ObjectProperty):
            if isinstance(subj, BNode):
                continue
            iri = str(subj)
            domain = [
                str(obj)
                for obj in rdf_graph.objects(subj, RDFS.domain)
                if not isinstance(obj, BNode)
            ]
            range_ = [
                str(obj)
                for obj in rdf_graph.objects(subj, RDFS.range)
                if not isinstance(obj, BNode)
            ]
            object_properties[iri] = ObjectProperty(
                iri=iri,
                local_name=_local_name_of(iri),
                label=_label_of(subj),
                domain=domain,
                range=range_,
            )

        # ---- Step 5: collect datatype properties ----
        # The XSD-namespace check on ``rdfs:range`` and the rule that
        # every owl:DatatypeProperty must declare exactly one range
        # live in :func:`_validate_no_dangling_refs`, which runs after
        # ``_compute_subclass_closure`` below. We store the first range
        # IRI if any, or ``""`` otherwise, so the dataclass invariant
        # (single datatype per property) holds even before validation;
        # ``""`` is rejected by the validator as a missing range
        # (Requirement 3.4).
        datatype_properties: Dict[str, DatatypeProperty] = {}
        for subj in rdf_graph.subjects(RDF.type, OWL.DatatypeProperty):
            if isinstance(subj, BNode):
                continue
            iri = str(subj)
            domain = [
                str(obj)
                for obj in rdf_graph.objects(subj, RDFS.domain)
                if not isinstance(obj, BNode)
            ]
            ranges = [
                str(obj)
                for obj in rdf_graph.objects(subj, RDFS.range)
                if not isinstance(obj, BNode)
            ]
            datatype_properties[iri] = DatatypeProperty(
                iri=iri,
                local_name=_local_name_of(iri),
                label=_label_of(subj),
                domain=domain,
                datatype=ranges[0] if ranges else "",
            )

        # ---- Step 6: subclass closure (stub until Task 2.2) ----
        ancestors_map = _compute_subclass_closure(classes)
        for iri, cls_ in classes.items():
            cls_.ancestors = ancestors_map.get(iri, frozenset({iri}))

        # ---- Step 7: dangling-ref / polymorphic / XSD-range validation ----
        _validate_no_dangling_refs(classes, object_properties, datatype_properties)

        # ---- Step 8: case-insensitive name indexes (stub until Task 2.4) ----
        by_local, by_label, obj_pred_idx, dt_pred_idx = _build_name_indexes(
            classes, object_properties, datatype_properties
        )

        # ---- Step 9: construct and return the schema ----
        return cls(
            namespace=namespace,
            classes=classes,
            object_properties=object_properties,
            datatype_properties=datatype_properties,
            _by_local_name=by_local,
            _by_label=by_label,
            _obj_predicate_index=obj_pred_idx,
            _dt_predicate_index=dt_pred_idx,
        )

    # ------------------------------------------------------------------
    # Public query API
    # ------------------------------------------------------------------

    def is_subclass_of(self, child_iri: str, parent_iri: str) -> bool:
        """Return True iff ``parent_iri`` is in the precomputed ancestors of ``child_iri``.

        Reflexive: ``is_subclass_of(c, c)`` is True for every declared class
        ``c``. Constant time — uses the ``frozenset`` precomputed at load
        time by :func:`_compute_subclass_closure` (NFR-2).

        Args:
            child_iri: The candidate subclass IRI. Unknown IRIs (not in
                :attr:`classes`) yield ``False``.
            parent_iri: The candidate superclass IRI.

        Returns:
            ``True`` iff ``parent_iri`` appears in ``classes[child_iri].ancestors``.
        """
        cls_ = self.classes.get(child_iri)
        if cls_ is None:
            return False
        return parent_iri in cls_.ancestors

    # ------------------------------------------------------------------
    # Resolvers — map LLM-emitted strings back to ontology IRIs / objects
    # ------------------------------------------------------------------

    def resolve_class(self, name: str) -> Optional[str]:
        """Resolve a classification string to a class IRI.

        Match order (Requirement 6.1, design §"Component 1: OntologySchema"):

        1. Exact IRI match against :attr:`classes`.
        2. Case-insensitive ``local_name`` match via
           :attr:`_by_local_name` (first-declared wins on collision, per
           :func:`_build_name_indexes`).
        3. Case-insensitive ``rdfs:label`` match via :attr:`_by_label`.

        Returns the class IRI on match, ``None`` on miss. Empty / ``None``
        inputs short-circuit to ``None`` so callers do not need to guard
        their ``Entity.classification`` accesses — ``resolve_class`` is
        the load-bearing guard.
        """
        if not name:
            return None
        # (1) Exact IRI match — lets the LLM round-trip full IRIs when it
        # has them, and lets tests assert by IRI without relying on the
        # case-insensitive indexes.
        if name in self.classes:
            return name
        # (2) / (3) Case-insensitive local_name and label via the
        # precomputed indexes. O(1) each.
        key = name.lower()
        hit = self._by_local_name.get(key)
        if hit is not None:
            return hit
        hit = self._by_label.get(key)
        if hit is not None:
            return hit
        return None

    def resolve_object_predicate(self, name: str) -> List[ObjectProperty]:
        """Resolve a predicate string to candidate :class:`ObjectProperty`.

        Matches against ``local_name``, ``rdfs:label``, and the normalized
        form (UPPER_SNAKE_CASE → camelCase via
        :func:`_normalize_predicate_name`), all case-insensitive.
        :func:`_build_name_indexes` already stores each of those three
        forms as a key in :attr:`_obj_predicate_index` so the lookup is
        O(1) per form (design §"Component 1: OntologySchema",
        Requirement 1.4).

        Returns an empty list on miss — never ``None`` — so callers can
        iterate without a ``None``-check. Duplicates are suppressed so
        each distinct :class:`ObjectProperty` appears at most once in
        the result even when multiple query keys point at the same IRI
        (e.g. ``local_name`` ``worksFor`` and normalized ``WORKS_FOR``
        fold to the same lowercase key, but a property whose label also
        hits is only returned once).
        """
        if not name:
            return []
        # Two keys: the literal lowercase form and the UPPER_SNAKE →
        # camelCase fold. The index stores labels and the normalized
        # local_name form under their lowercase keys, so both variants
        # are checked by this single pair of lookups.
        keys: Set[str] = {name.lower()}
        normalized = _normalize_predicate_name(name)
        if normalized:
            keys.add(normalized.lower())

        seen_iris: Set[str] = set()
        results: List[ObjectProperty] = []
        for key in keys:
            for iri in self._obj_predicate_index.get(key, []):
                if iri in seen_iris:
                    continue
                seen_iris.add(iri)
                results.append(self.object_properties[iri])
        return results

    def resolve_datatype_predicate(self, name: str) -> List[DatatypeProperty]:
        """Resolve a predicate string to candidate :class:`DatatypeProperty`.

        Identical matching strategy to :meth:`resolve_object_predicate`
        but against :attr:`_dt_predicate_index`. Returns an empty list
        on miss (Requirement 1.5).
        """
        if not name:
            return []
        keys: Set[str] = {name.lower()}
        normalized = _normalize_predicate_name(name)
        if normalized:
            keys.add(normalized.lower())

        seen_iris: Set[str] = set()
        results: List[DatatypeProperty] = []
        for key in keys:
            for iri in self._dt_predicate_index.get(key, []):
                if iri in seen_iris:
                    continue
                seen_iris.add(iri)
                results.append(self.datatype_properties[iri])
        return results

    def allowed_object_predicates(
        self, subj_iri: str, obj_iri: str
    ) -> List[ObjectProperty]:
        """Return :class:`ObjectProperty` whose domain covers ``subj_iri``
        and whose range covers ``obj_iri`` via subclass closure.

        An empty ``domain`` / ``range`` is treated as ``owl:Thing`` —
        matches any class — per the field docstrings on
        :class:`ObjectProperty` and design §"Data Models". The subclass
        test itself is :meth:`is_subclass_of`, which is O(1) via the
        precomputed ancestor frozenset (NFR-2), so walking every object
        property here is the O(P) upper bound on the total cost.

        Used by the strict-mode filter's SPO fact check (Requirements
        2.3, 2.4): a fact is kept iff at least one candidate returned
        here matches the predicate string emitted by the LLM.
        """
        results: List[ObjectProperty] = []
        for op in self.object_properties.values():
            # Empty domain = owl:Thing, so it covers everything.
            domain_ok = not op.domain or any(
                self.is_subclass_of(subj_iri, d) for d in op.domain
            )
            if not domain_ok:
                continue
            range_ok = not op.range or any(
                self.is_subclass_of(obj_iri, r) for r in op.range
            )
            if not range_ok:
                continue
            results.append(op)
        return results

    def allowed_datatype_predicates(
        self, subj_iri: str
    ) -> List[DatatypeProperty]:
        """Return :class:`DatatypeProperty` whose domain covers ``subj_iri``
        via subclass closure.

        Mirrors :meth:`allowed_object_predicates` but without a range
        check — datatype properties carry a single XSD range that is
        validated separately (in strict mode) by
        :func:`_validate_literal_against_xsd`, not here. Empty
        ``domain`` is treated as ``owl:Thing``.
        """
        results: List[DatatypeProperty] = []
        for dp in self.datatype_properties.values():
            domain_ok = not dp.domain or any(
                self.is_subclass_of(subj_iri, d) for d in dp.domain
            )
            if domain_ok:
                results.append(dp)
        return results

    # ------------------------------------------------------------------
    # Prompt rendering (Requirement 12, 13; NFR-6)
    # ------------------------------------------------------------------

    def format_as_prompt_constraint(self, strict_prompt: bool = True) -> str:
        """Render the ontology as structured prompt text for the LLM.

        Produces the block documented in design §"The emitted prompt
        text": a header, the class-hierarchy list, the object-property
        list with ``subject-class -> object-class`` arrows, the
        datatype-property list with XSD types, a verbatim
        predicate-spelling protocol block, and a closing paragraph
        selected by ``strict_prompt`` (``STRICT MODE:`` when ``True``,
        ``NOTE:`` when ``False``).

        Determinism (Requirement 13.1, NFR-6): the output is a pure
        function of ``(self, strict_prompt)``. All iteration orders are
        stabilised via ``sorted(..., key=lambda x: x.local_name)`` so
        repeated calls with the same arguments are byte-identical.

        Completeness (Requirements 12.1–12.5): every class's
        ``local_name`` appears in the classes section; every object
        property's ``local_name`` appears on a line containing
        ``f"{d} -> {r}"`` for at least one ``(d, r)`` pair derived from
        its ``domain × range`` (with ``owl:Thing`` used when a side is
        empty); every datatype property emits one
        ``{d_local}.{local_name} : {xsd_local}`` line per
        ``d ∈ dp.domain`` (or one line with ``owl:Thing`` as the domain
        fallback when ``dp.domain`` is empty); ``rdfs:label`` values are
        surfaced in ``(also called: …)`` or ``(label: …)`` blocks.

        Mode locality (Requirement 13.4): only the final paragraph
        differs between ``strict_prompt=True`` and ``False``; every
        other substring is present in both variants.

        Args:
            strict_prompt: When ``True`` (default), emits the
                ``STRICT MODE:`` paragraph — appropriate when the
                pipeline includes :class:`OntologyFilterStage` and
                out-of-vocab predicates will be dropped. When ``False``,
                emits the ``NOTE:`` paragraph — appropriate for
                suggestion mode where the ontology is a preference, not
                a hard constraint.

        Returns:
            A deterministic prompt-text block suitable for injection
            into the ``{schema_constraints}`` slot of
            ``EXTRACT_TOPICS_PROMPT``.
        """
        return _render_prompt_constraint(self, strict_prompt=strict_prompt)

    # ------------------------------------------------------------------
    # Bridge to the existing ExtractionSchema (Requirement 11, design §D3)
    # ------------------------------------------------------------------

    def as_extraction_schema(self) -> "ExtractionSchema":
        """Bridge to the flat :class:`ExtractionSchema` for backward compatibility.

        Produces an :class:`ExtractionSchema` whose:

        * ``entity_types`` keys are the ``local_name`` of every
          :class:`OntologyClass`, with ``description = cls.label`` and
          ``aliases = [cls.iri, cls.label.lower()]`` deduplicated
          case-insensitively (Requirement 11.1).
        * ``relationship_types`` is
          ``UPPER_SNAKE_CASE(op.local_name)`` for every
          :class:`ObjectProperty` (e.g. ``worksFor`` → ``WORKS_FOR``),
          matching the convention the extraction prompt emits
          (Requirement 11.2).
        * ``strict=False`` — strict enforcement lives exclusively in
          :class:`OntologyFilterStage`, never in
          :attr:`ExtractionSchema.strict` (design §D3,
          Requirement 11.3).

        The returned instance has ``format_as_prompt_constraint``
        rebound so that call sites receive the richer
        :class:`OntologySchema` prompt text (class hierarchy,
        domain/range, datatype properties) rather than the flat
        :class:`ExtractionSchema` rendering (design §D3,
        Requirement 5.2). The rebind is per-instance; other
        :class:`ExtractionSchema` objects are unaffected.

        Datatype properties are intentionally **not** surfaced as
        entity attributes on the bridged schema — the bridge is lossy
        (Requirement 11.6). Datatype validation is only available when
        :class:`OntologyFilterStage` is present in the pipeline.

        Returns:
            An :class:`ExtractionSchema` instance suitable for
            ``ExtractionConfig.from_stages(..., schema=...)`` and any
            existing call site that consumes
            :class:`ExtractionSchema` today.
        """
        # Local import to avoid top-level coupling to the
        # ExtractionSchema module — keeps ``ontology_schema`` importable
        # without dragging in the flat schema for callers who only need
        # the ontology parser. The "ExtractionSchema" in the return
        # annotation is a forward-reference string, so the type hint
        # does not force an import at module load time either.
        from .extraction_schema import EntityTypeConfig, ExtractionSchema

        entity_types: Dict[str, EntityTypeConfig] = {}
        for cls_ in self.classes.values():
            # Deduplicate aliases case-insensitively while preserving
            # order: IRI first, then the lowercased label. Callers that
            # feed the bridged schema into SchemaFilterStage rely on
            # aliases lining up with what the LLM emits, so the
            # lowercased label is the more-useful alias than the raw
            # label casing.
            aliases: List[str] = []
            seen_alias_keys: Set[str] = set()
            for candidate in (cls_.iri, (cls_.label or "").lower()):
                if not candidate:
                    continue
                key = candidate.lower()
                if key in seen_alias_keys:
                    continue
                seen_alias_keys.add(key)
                aliases.append(candidate)
            entity_types[cls_.local_name] = EntityTypeConfig(
                description=cls_.label,
                aliases=aliases,
            )

        relationship_types: List[str] = [
            _camel_to_upper_snake(op.local_name)
            for op in self.object_properties.values()
        ]

        bridged = ExtractionSchema(
            entity_types=entity_types,
            relationship_types=relationship_types,
            strict=False,
        )

        # Rebind ``format_as_prompt_constraint`` on this instance only
        # so the LLM sees the ontology's richer prompt text — class
        # hierarchy, domain/range, datatype properties — rather than
        # the flat ExtractionSchema rendering (Requirement 5.2).
        # ``LLMTopicExtractionStage`` invokes the method with no
        # arguments, so we wrap ``self.format_as_prompt_constraint`` in
        # a zero-arg closure that lets the method's ``strict_prompt``
        # default apply. The flat ExtractionSchema surface carries no
        # strict-mode semantics — those live in OntologyFilterStage, so
        # "strict" in this closure refers only to the prompt paragraph,
        # not to any post-filter behaviour.
        bridged.format_as_prompt_constraint = (  # type: ignore[method-assign]
            lambda: self.format_as_prompt_constraint()
        )
        return bridged


def _local_name_of(iri: str) -> str:
    """Extract the last path segment from an IRI.

    Splits on ``#`` first (OWL/RDF fragment delimiter), then ``/`` as a
    fallback. Returns the original string unchanged if neither delimiter is
    present (e.g. for blank nodes or pre-extracted local names).
    """
    if "#" in iri:
        return iri.rsplit("#", 1)[-1]
    if "/" in iri:
        return iri.rsplit("/", 1)[-1]
    return iri


def _normalize_predicate_name(name: str) -> str:
    """Fold UPPER_SNAKE_CASE to camelCase for case-insensitive predicate lookup.

    The extraction prompt emits relationships in UPPER_SNAKE_CASE (e.g.
    ``WORKS_FOR``), while ontologies typically declare predicates in camelCase
    (e.g. ``worksFor``). This helper produces the camelCase form so that
    ``resolve_object_predicate("WORKS_FOR")`` finds ``:worksFor`` via the
    predicate indexes.

    Names that do not contain underscores are returned unchanged so they still
    map consistently (case-insensitive lookup is done downstream). Empty
    strings map to empty strings.
    """
    if not name:
        return name
    if "_" not in name:
        return name
    parts = [p for p in name.split("_") if p]
    if not parts:
        return ""
    first = parts[0].lower()
    rest = "".join(p.capitalize() for p in parts[1:])
    return first + rest


def _camel_to_upper_snake(name: str) -> str:
    """Convert a camelCase or PascalCase name to ``UPPER_SNAKE_CASE``.

    The inverse of :func:`_normalize_predicate_name`. Used by
    :meth:`OntologySchema.as_extraction_schema` to map every
    :class:`ObjectProperty` local name onto the convention the
    extraction prompt emits for relationship types (Requirement 11.2):

    * ``worksFor``    → ``WORKS_FOR``
    * ``isFriendOf``  → ``IS_FRIEND_OF``
    * ``A``           → ``A``
    * ``HTTPRequest`` → ``HTTPREQUEST`` — acronyms stay glued because
      every character is uppercase and the helper only inserts an
      underscore at lower→upper transitions.

    An underscore is inserted before every uppercase letter that
    follows a non-uppercase character; the result is then
    uppercased wholesale. Non-alphanumeric characters pass through
    verbatim (no-op) so pre-snaked names round-trip unchanged. Empty
    strings map to empty strings.
    """
    if not name:
        return name
    result: List[str] = []
    for i, ch in enumerate(name):
        if i > 0 and ch.isupper() and not name[i - 1].isupper():
            result.append("_")
        result.append(ch.upper())
    return "".join(result)


def _xsd_local_name_of(xsd_iri: str) -> str:
    """Return the local name of an XSD IRI for prompt rendering.

    The XSD namespace uses ``#`` as its fragment delimiter, so
    ``http://www.w3.org/2001/XMLSchema#integer`` has local name
    ``"integer"``. We reuse :func:`_local_name_of` for the actual
    split — this helper exists as a named wrapper so call sites in
    :func:`_render_prompt_constraint` read intention-first
    (``_xsd_local_name_of(dp.datatype)``) rather than ``_local_name_of``
    which would be ambiguous next to class-IRI splits on the same line.

    An empty or non-XSD IRI falls through to :func:`_local_name_of` —
    the renderer will still produce a usable line rather than raising.
    """
    return _local_name_of(xsd_iri)


# ---------------------------------------------------------------------------
# Prompt rendering (Requirement 12, 13; NFR-6)
#
# ``_render_prompt_constraint`` is factored out of the
# :meth:`OntologySchema.format_as_prompt_constraint` method body so that
# the long, literal string blocks (the header, the predicate-spelling
# protocol, and the two final-paragraph variants) live next to the
# per-section renderers at module scope — rather than bloating the
# method body with ~80 lines of static Markdown. The split has no
# functional consequence: the method is still the only caller.
# ---------------------------------------------------------------------------


# The verbatim predicate-spelling protocol block. This text is the same
# across every ontology (it is instruction prose, not ontology content)
# and is therefore a module-level literal. Kept trailing-newline-free so
# the assembly code in :func:`_render_prompt_constraint` controls
# inter-section spacing consistently.
_PREDICATE_SPELLING_PROTOCOL_BLOCK = """\
## Predicate spelling protocol (critical)

When naming a relationship or attribute in your output, use the exact string
from the vocabulary above. Matching is case-insensitive but otherwise literal:

  Ontology declares    LLM output "KNOWS"           → MATCH
  :knows               LLM output "knows"           → MATCH
                       LLM output "Knows"           → MATCH
                       LLM output "KNOWS_WELL"      → REJECTED — use "knows"
                       LLM output "IS_KNOWN_BY"     → REJECTED — not in vocab
                       LLM output "ACQUAINTED_WITH" → REJECTED — not in vocab"""


# Final paragraph variants — the only content that differs between
# ``strict_prompt=True`` and ``strict_prompt=False`` outputs
# (Requirement 13.4). Every other substring in the rendered prompt is
# identical across the two variants.
_STRICT_MODE_FINAL_PARAGRAPH = """\
STRICT MODE: Entities and relationships outside the vocabulary above will be
discarded from the final output. Prefer skipping over inventing."""

_SUGGESTION_MODE_FINAL_PARAGRAPH = """\
NOTE: This vocabulary is a strong preference, not a hard constraint. If the
text genuinely requires a predicate not listed above, you may emit it — but
prefer the listed vocabulary whenever the meaning is close."""


# Fixed header / preamble blocks. Kept as module-level literals for the
# same reasons as the predicate-spelling block above.
_PROMPT_HEADER = """\
# Ontology-guided extraction

You MUST extract entities and relationships using the controlled vocabulary
defined below. This vocabulary is exhaustive — entities and predicates outside
this list will be rejected."""

_CLASSES_PREAMBLE = "## Allowed entity types (class hierarchy)"

_CLASSES_POSTAMBLE = """\
When classifying an entity, you MUST use one of the type names above (case-
insensitive). Use the most specific subclass that applies (e.g. Employee
over Person when the entity is employed)."""

_OBJECT_PROPERTIES_PREAMBLE = """\
## Allowed entity-entity relationships

Format: predicate : subject-class -> object-class
You MUST pick a predicate EXACTLY as spelled below. Do NOT invent synonyms,
do not append modifiers (e.g. "knows_well", "closely_knows"), do not combine
two predicates. Spelling and case will be matched exactly."""

_OBJECT_PROPERTIES_POSTAMBLE = """\
If a sentence expresses a relationship that is close to but not exactly one
of the predicates above:
  - If the intended meaning is clearly covered by one of the listed predicates
    (e.g. "X knows Y well" covers the same relationship as :knows), use that
    listed predicate WITHOUT the modifier.
  - If no listed predicate fits, do NOT emit the relationship at all. Skip it.
  - Do NOT invent a new predicate name."""

_DATATYPE_PROPERTIES_PREAMBLE = """\
## Allowed entity attributes (datatype properties)

Format: subject-class.attribute : xsd-type"""

_DATATYPE_PROPERTIES_POSTAMBLE = """\
Attribute values must be parseable as the listed XSD type. For integer, emit
digits only (e.g. "42" not "forty-two"). For boolean, emit "true" or "false".
For dateTime, emit ISO-8601."""


def _render_prompt_constraint(
    schema: "OntologySchema", *, strict_prompt: bool
) -> str:
    """Assemble the prompt-constraint block for ``schema``.

    See :meth:`OntologySchema.format_as_prompt_constraint` for the
    contract; this function is the implementation. Broken out so the
    method body stays short and the (long, mostly-literal) block
    rendering lives next to the module-level string constants.

    Determinism: every ``for`` loop over the ontology is wrapped in
    ``sorted(..., key=lambda x: x.local_name)`` (Requirement 13.3), all
    label lists are deduplicated case-insensitively with a deterministic
    order, and all literal blocks are module-level constants. The
    resulting string is a pure function of ``(schema, strict_prompt)``
    (Requirement 13.1, NFR-6).
    """
    sections: List[str] = [_PROMPT_HEADER]

    # ---- Classes section ------------------------------------------------
    sections.append(_CLASSES_PREAMBLE)
    classes_block = _render_classes_block(schema)
    if classes_block:
        sections.append(classes_block)
    sections.append(_CLASSES_POSTAMBLE)

    # ---- Object properties section -------------------------------------
    sections.append(_OBJECT_PROPERTIES_PREAMBLE)
    obj_props_block = _render_object_properties_block(schema)
    if obj_props_block:
        sections.append(obj_props_block)
    sections.append(_OBJECT_PROPERTIES_POSTAMBLE)

    # ---- Datatype properties section -----------------------------------
    sections.append(_DATATYPE_PROPERTIES_PREAMBLE)
    dt_props_block = _render_datatype_properties_block(schema)
    if dt_props_block:
        sections.append(dt_props_block)
    sections.append(_DATATYPE_PROPERTIES_POSTAMBLE)

    # ---- Verbatim predicate-spelling protocol --------------------------
    sections.append(_PREDICATE_SPELLING_PROTOCOL_BLOCK)

    # ---- Mode-dependent final paragraph (Requirement 13.4) -------------
    sections.append(
        _STRICT_MODE_FINAL_PARAGRAPH
        if strict_prompt
        else _SUGGESTION_MODE_FINAL_PARAGRAPH
    )

    # Double-newline between top-level sections mirrors the Markdown
    # style in design §"The emitted prompt text". A trailing newline
    # keeps the output friendly to concatenation with further prompt
    # content without forcing callers to remember to append one.
    return "\n\n".join(sections) + "\n"


def _render_classes_block(schema: "OntologySchema") -> str:
    """Render the indented class-hierarchy list.

    Classes are emitted sorted by ``local_name`` (Requirement 13.3).
    Indentation reflects each class's depth in the hierarchy so the
    structure is visible even though ordering is alphabetical:
    ``depth = max(0, len(cls.ancestors) - 1)`` — ``ancestors`` is
    reflexive (includes self), so subtracting one yields the number of
    proper ancestors, i.e. the minimum chain length from a root. Root
    classes (no parents) end up at depth 0 regardless of whether they
    declare themselves as subclasses of external vocabulary terms.

    Each class produces two lines:

    * ``{indent}{local_name}`` — the class name, indented.
    * ``{label_indent}(label: "X", "y", …)`` — the label block,
      containing (a) the ``rdfs:label`` if declared and (b) the
      lowercased ``local_name``. Duplicates are removed
      case-insensitively while preserving first-occurrence order.

    Indent widths chosen to match design §"The emitted prompt text":
    two-space base indent plus two spaces per depth level for the class
    name; one extra level of indentation for the label line.
    """
    lines: List[str] = []
    for cls_ in sorted(schema.classes.values(), key=lambda c: c.local_name):
        depth = max(0, len(cls_.ancestors) - 1)
        # Two-space base indent + two spaces per depth level. Matches the
        # Person/Employee example in design §"The emitted prompt text":
        # Person at depth 0 → "  Person", Employee at depth 1 →
        # "    Employee", Manager at depth 2 → "      Manager".
        indent = "  " + ("  " * depth)
        lines.append(f"{indent}{cls_.local_name}")

        # Label line sits one indent level deeper than the class name.
        label_indent = indent + "  "
        label_values = _dedup_case_insensitive(
            [cls_.label] if cls_.label else [],
            cls_.local_name.lower(),
        )
        labels_rendered = ", ".join(f'"{v}"' for v in label_values)
        lines.append(f"{label_indent}(label: {labels_rendered})")

    return "\n".join(lines)


def _render_object_properties_block(schema: "OntologySchema") -> str:
    """Render the object-property list.

    For each :class:`ObjectProperty` (sorted by ``local_name``), emit
    one line per ``(d, r)`` pair drawn from ``domain × range`` (with
    ``owl:Thing`` substituted for empty sides — Requirement 12.2). The
    line format is ``  {name:<11} : {d_local} -> {r_local}``, matching
    the alignment in design §"The emitted prompt text" where
    ``knows       : Person -> Person`` and
    ``worksFor    : Employee -> Company`` line up.

    When the property declares an ``rdfs:label`` distinct from its
    ``local_name``, a single ``(also called: "…")`` line follows the
    last ``(d, r)`` rendering for that property. The "also called"
    line is indented to align under the type columns.
    """
    lines: List[str] = []
    # Width of the local-name column, chosen so short predicate names
    # (e.g. "knows") line up with longer ones (e.g. "worksFor"). Names
    # longer than this width push the colon rightward — the format
    # still parses because there is always at least one space before
    # the colon.
    _NAME_WIDTH = 11

    for op in sorted(schema.object_properties.values(), key=lambda p: p.local_name):
        # Empty domain/range fall back to "owl:Thing" so the line still
        # contains the ``{d} -> {r}`` substring required by
        # Requirement 12.2.
        domains = [_local_name_of(d) for d in op.domain] or ["owl:Thing"]
        ranges = [_local_name_of(r) for r in op.range] or ["owl:Thing"]

        # One line per (domain, range) pair. For the typical case of a
        # single declared domain and a single declared range this
        # produces a single line; multiple-domain or multiple-range
        # declarations produce the cartesian product so the LLM sees
        # every valid pairing explicitly.
        for d_local in domains:
            for r_local in ranges:
                lines.append(
                    f"  {op.local_name:<{_NAME_WIDTH}} : {d_local} -> {r_local}"
                )

        # "(also called: …)" line — only when the label adds something
        # beyond the local_name (case-insensitive comparison). Aligned
        # under the type column for readability.
        if op.label:
            also_called = _dedup_case_insensitive(
                [op.label], op.local_name.lower()
            )
            # Remove the local_name from the "also called" set — the
            # label block is meant to surface the *additional* synonyms
            # beyond the predicate name itself. If no labels remain
            # after stripping the local_name, skip the line entirely.
            also_called = [
                v for v in also_called if v.lower() != op.local_name.lower()
            ]
            if also_called:
                rendered = ", ".join(f'"{v}"' for v in also_called)
                # Indent the "(also called: …)" line so it sits under
                # the type column, matching design §"The emitted prompt
                # text" where "(also called: …)" aligns with "Person".
                align_indent = " " * (2 + _NAME_WIDTH + 3)  # "  " + name + " : "
                lines.append(f"{align_indent}(also called: {rendered})")

    return "\n".join(lines)


def _render_datatype_properties_block(schema: "OntologySchema") -> str:
    """Render the datatype-property list.

    For each :class:`DatatypeProperty` (sorted by ``local_name``),
    emit one line per ``d ∈ dp.domain``:

        ``  {d_local}.{dp.local_name} : {xsd_local}``

    matching the ``Person.age : integer`` example in design §"The
    emitted prompt text".

    When ``dp.domain`` is empty, emit a single line with ``owl:Thing``
    as the domain fallback — this keeps the property visible in the
    rendered prompt (Requirement 12.3 does not specify a fallback for
    empty domains, but a silently-missing datatype property would
    regress completeness).
    """
    lines: List[str] = []
    for dp in sorted(schema.datatype_properties.values(), key=lambda p: p.local_name):
        xsd_local = _xsd_local_name_of(dp.datatype) if dp.datatype else ""
        domains = [_local_name_of(d) for d in dp.domain] or ["owl:Thing"]
        for d_local in domains:
            lines.append(f"  {d_local}.{dp.local_name} : {xsd_local}")

    return "\n".join(lines)


def _dedup_case_insensitive(
    primary: List[str], *extras: str
) -> List[str]:
    """Return a de-duplicated (case-insensitive) list preserving first-occurrence order.

    ``primary`` holds labels that should appear first in the output
    (e.g. ``rdfs:label`` values); ``extras`` are positional arguments
    that get appended after the primary labels (e.g. the lowercased
    ``local_name`` of a class). The case-insensitive comparison uses
    ``str.lower()`` for Unicode consistency with how Python compares
    ASCII identifiers in the rest of this module.

    Empty strings are skipped silently so a caller can pass
    ``""`` without special-casing it upstream.
    """
    seen: Set[str] = set()
    out: List[str] = []
    for value in list(primary) + list(extras):
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


# ---------------------------------------------------------------------------
# Module-level helpers used by ``OntologySchema._parse_graph``.
#
# Task 2.1 scope: ``_extract_base_iri`` is fully implemented because
# Requirement 3.5 requires `from_turtle` to reject ontologies with no
# determinable base IRI at load time. The three validators / index builders
# below are intentional stubs with trivial behaviour; Tasks 2.2, 2.3, 2.4
# replace them in turn. Keeping the call-site wiring in ``_parse_graph``
# identical now means later tasks only change the body of these helpers,
# not the parse pipeline.
# ---------------------------------------------------------------------------


def _extract_base_iri(
    rdf_graph: "rdflib.Graph", base_iri_hint: Optional[str] = None
) -> Optional[str]:
    """Determine the ontology's base IRI (Requirement 3.5).

    Detection order mirrors the requirement text:

    1. The Turtle ``@prefix : <...>`` declaration, surfaced by ``rdflib`` as
       the ``""`` (empty-string) prefix in the graph's namespace manager.
    2. ``rdflib.namespace.DefaultNamespace`` — the parser-level default that
       ``rdflib`` populates when a Turtle file uses ``@base`` or contains a
       default prefix. We fall through to this when (1) is absent.
    3. The first ``owl:Ontology`` subject IRI — conventionally the document's
       self-identifier.
    4. Finally, the explicit ``base_iri_hint`` passed by
       :meth:`OntologySchema.from_turtle_string` (``publicID`` echoed back).
       This is lowest-priority so an in-file declaration always wins over a
       caller-supplied fallback.

    Returns the detected base IRI as a string, or ``None`` if nothing
    resolves — callers convert this to :class:`OntologyLoadError` at the
    parse-pipeline level (design §"Algorithm: Turtle parsing" Step 2).
    """
    import rdflib  # type: ignore[import-not-found]

    OWL = rdflib.Namespace("http://www.w3.org/2002/07/owl#")
    RDF = rdflib.RDF

    # (1) '@prefix : <...>' — rdflib exposes this as the empty-string prefix.
    try:
        for prefix, ns in rdf_graph.namespaces():
            if prefix == "":
                ns_str = str(ns)
                if ns_str:
                    return ns_str
    except Exception:  # pragma: no cover - defensive; rdflib always yields
        pass

    # (2) rdflib's parser-level DefaultNamespace, if the parser exposed one.
    default_ns = getattr(rdf_graph, "default_namespace", None)
    if default_ns:
        ns_str = str(default_ns)
        if ns_str:
            return ns_str

    # (3) First ``owl:Ontology`` subject IRI.
    for subj in rdf_graph.subjects(RDF.type, OWL.Ontology):
        subj_str = str(subj)
        if subj_str:
            return subj_str

    # (4) Caller-supplied hint (publicID from ``from_turtle_string``).
    if base_iri_hint:
        return base_iri_hint

    return None


def _compute_subclass_closure(
    classes: Dict[str, OntologyClass],
) -> Dict[str, FrozenSet[str]]:
    """Compute the transitive (reflexive) ``rdfs:subClassOf`` closure.

    Implements the pseudocode in design §"Algorithm: subclass closure":
    a memoized depth-first walk over the ``parents`` graph that records
    each class's set of ancestors (including itself) as a ``frozenset``.
    A ``visiting`` set detects ``rdfs:subClassOf`` cycles and raises
    :class:`OntologyLoadError` naming the IRI at which the cycle was
    first re-entered (Requirement 3.1).

    Parents that point to an IRI not declared as a class in ``classes``
    are treated as a dangling reference and are *silently skipped here*
    — :func:`_validate_no_dangling_refs` is the single place responsible
    for raising on such cases, and including an unknown parent in
    ``accum`` would pollute ``OntologyClass.ancestors`` with non-class
    IRIs.

    Args:
        classes: Map from class IRI to :class:`OntologyClass` with
            populated ``parents``. Not mutated.

    Returns:
        A map from every IRI in ``classes`` to its reflexive transitive
        closure as a ``frozenset``. Callers assign each entry to the
        corresponding ``OntologyClass.ancestors`` field.

    Raises:
        OntologyLoadError: If an ``rdfs:subClassOf`` cycle is detected.

    Complexity:
        O(C × E) with memoization, where C is the number of classes and
        E is the average parent fan-in. Built once at load time so that
        :meth:`OntologySchema.is_subclass_of` is constant time (NFR-2).
    """
    ancestors_map: Dict[str, FrozenSet[str]] = {}
    visiting: Set[str] = set()
    memo: Dict[str, FrozenSet[str]] = {}

    def compute(iri: str) -> FrozenSet[str]:
        if iri in memo:
            return memo[iri]
        if iri in visiting:
            raise OntologyLoadError(
                f"rdfs:subClassOf cycle detected at {iri}"
            )
        visiting.add(iri)

        accum: Set[str] = {iri}  # reflexive
        cls_ = classes.get(iri)
        if cls_ is not None:
            for parent_iri in cls_.parents:
                # Only recurse into known classes; unknown parents are a
                # dangling reference and are handled by
                # :func:`_validate_no_dangling_refs`. Skipping them here
                # keeps ``ancestors`` restricted to declared class IRIs.
                if parent_iri in classes:
                    accum |= compute(parent_iri)

        visiting.discard(iri)
        result = frozenset(accum)
        memo[iri] = result
        return result

    for iri in classes:
        ancestors_map[iri] = compute(iri)

    return ancestors_map


def _validate_no_dangling_refs(
    classes: Dict[str, OntologyClass],
    object_properties: Dict[str, ObjectProperty],
    datatype_properties: Dict[str, DatatypeProperty],
) -> None:
    """Run the load-time structural validators required by Requirement 3.

    This helper performs three non-closure validators and raises
    :class:`OntologyLoadError` on the first failure so that callers never
    see a partially-valid :class:`OntologySchema` (Requirement 3.6). The
    related base-IRI check (Requirement 3.5) lives in
    :func:`_extract_base_iri`; cycle detection (Requirement 3.1) is
    surfaced from :func:`_compute_subclass_closure`. Together those
    three call sites cover Requirements 3.1 – 3.5.

    Checks run, in order (cheapest first so a broken ontology fails as
    quickly as possible):

    1. **Polymorphic-property check (Requirement 3.3).** A single IRI
       declared as both ``owl:ObjectProperty`` and
       ``owl:DatatypeProperty`` is rejected — v1 does not support
       polymorphic properties (design §"Data Models"). Detected via set
       intersection of the two property-map key sets.

    2. **DatatypeProperty range check (Requirement 3.4).** Every
       ``owl:DatatypeProperty`` must declare exactly one ``rdfs:range``
       IRI in the ``XSD_NAMESPACE`` namespace. A missing range (``""``
       as stored by :meth:`OntologySchema._parse_graph` when no triple
       was found) and any non-XSD range IRI are both errors. Running
       this before the dangling-ref sweep lets callers fix their
       ontology in an order that matches the requirement's reading
       order (``3.2 → 3.3 → 3.4``).

    3. **Dangling-reference check (Requirement 3.2).** Every IRI that
       appears in an ``rdfs:subClassOf`` (class parents),
       ``rdfs:domain`` (object- or datatype-property), or ``rdfs:range``
       (object-property) slot must be declared as the matching kind in
       the same Turtle file. For object-property and datatype-property
       ``domain``/``range`` slots the expected kind is always
       :class:`OntologyClass`; the polymorphic check above already
       guarantees class IRIs are never also property IRIs.

    Iteration order is stabilised via ``sorted(...)`` so the IRI named
    in the error message is deterministic across Python versions and
    dict orderings — important for test snapshots.

    Args:
        classes: Map from class IRI to :class:`OntologyClass` populated
            by :meth:`OntologySchema._parse_graph`.
        object_properties: Map from property IRI to
            :class:`ObjectProperty`.
        datatype_properties: Map from property IRI to
            :class:`DatatypeProperty`.

    Raises:
        OntologyLoadError: On the first detected violation, naming the
            offending IRI (Requirement 3.6 — no partial schema is
            returned).
    """
    # ---- 1. Polymorphic-property check (Requirement 3.3) -------------
    polymorphic = set(object_properties.keys()) & set(datatype_properties.keys())
    if polymorphic:
        # Deterministic picking — sorted() is cheap, the set is tiny.
        offender = sorted(polymorphic)[0]
        raise OntologyLoadError(
            f"Property {offender} is declared as both owl:ObjectProperty "
            f"and owl:DatatypeProperty; v1 does not support polymorphic "
            f"properties."
        )

    # ---- 2. DatatypeProperty range check (Requirement 3.4) -----------
    # ``_parse_graph`` stores an empty string when no ``rdfs:range`` triple
    # was found for a datatype property. Handle that as "no range" — the
    # more actionable error — before the XSD-namespace check so users
    # see the root cause instead of a misleading "range '' is not in XSD".
    for iri, dp in sorted(datatype_properties.items()):
        if not dp.datatype:
            raise OntologyLoadError(
                f"DatatypeProperty {iri} has no rdfs:range; every "
                f"owl:DatatypeProperty must declare exactly one XSD "
                f"range IRI."
            )
        if not dp.datatype.startswith(XSD_NAMESPACE):
            raise OntologyLoadError(
                f"DatatypeProperty {iri} has rdfs:range {dp.datatype} "
                f"which is not in the XSD namespace "
                f"({XSD_NAMESPACE}); only XSD datatypes are supported "
                f"in v1."
            )

    # ---- 3. Dangling-reference check (Requirement 3.2) ---------------
    # Class ``parents`` slots — ``_compute_subclass_closure`` silently
    # skips parents not in ``classes`` (so the closure stays restricted
    # to declared class IRIs), which makes THIS function the sole
    # enforcement point for Requirement 3.2 on subClassOf.
    for iri, cls_ in sorted(classes.items()):
        for parent in cls_.parents:
            if parent not in classes:
                raise OntologyLoadError(
                    f"Class {iri} has a dangling rdfs:subClassOf "
                    f"reference to {parent}; {parent} is not declared "
                    f"as an owl:Class in this ontology."
                )

    # ObjectProperty domain / range — every IRI must be a declared class.
    for iri, op in sorted(object_properties.items()):
        for d_iri in op.domain:
            if d_iri not in classes:
                raise OntologyLoadError(
                    f"ObjectProperty {iri} has a dangling rdfs:domain "
                    f"reference to {d_iri}; {d_iri} is not declared as "
                    f"an owl:Class in this ontology."
                )
        for r_iri in op.range:
            if r_iri not in classes:
                raise OntologyLoadError(
                    f"ObjectProperty {iri} has a dangling rdfs:range "
                    f"reference to {r_iri}; {r_iri} is not declared as "
                    f"an owl:Class in this ontology."
                )

    # DatatypeProperty domain — range is XSD-only and is validated above.
    for iri, dp in sorted(datatype_properties.items()):
        for d_iri in dp.domain:
            if d_iri not in classes:
                raise OntologyLoadError(
                    f"DatatypeProperty {iri} has a dangling rdfs:domain "
                    f"reference to {d_iri}; {d_iri} is not declared as "
                    f"an owl:Class in this ontology."
                )


def _build_name_indexes(
    classes: Dict[str, OntologyClass],
    object_properties: Dict[str, ObjectProperty],
    datatype_properties: Dict[str, DatatypeProperty],
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, List[str]], Dict[str, List[str]]]:
    """Build the four case-insensitive name indexes used by resolvers.

    Implements the post-parse indexing pass from design §"Key function
    signatures" and Requirement 1.3. All four returned maps share the
    convention of lowercasing keys so that :meth:`OntologySchema.resolve_class`,
    :meth:`OntologySchema.resolve_object_predicate`, and
    :meth:`OntologySchema.resolve_datatype_predicate` can do a single
    ``name.lower()`` lookup at query time.

    The class indexes (``_by_local_name``, ``_by_label``) follow a
    **first-declared-wins** policy on collision: the first class whose
    lowercase ``local_name`` (or ``rdfs:label``) produces a given key
    claims that key, and any subsequent class that collides on the same
    key is logged at DEBUG and skipped. Iteration order reflects the
    insertion order of the ``classes`` dict, which in turn reflects the
    order in which :meth:`OntologySchema._parse_graph` encountered the
    ``owl:Class`` triples — deterministic enough for test snapshots while
    remaining easy to reason about. Classes with no label (or a
    whitespace-only label) contribute no entry to ``_by_label``.

    The predicate indexes (``_obj_predicate_index``,
    ``_dt_predicate_index``) are **multi-valued** on purpose: different
    properties can legitimately collide on the same lowercase key (for
    example, ``worksFor`` and a hypothetical ``WORKS_FOR`` both normalize
    to ``"worksfor"``), and the downstream strict-mode filter picks
    whichever candidate matches the domain/range of the concrete fact.
    For each property we index three keys:

    1. ``local_name.lower()`` — direct match against the declared
       property name.
    2. ``label.lower()`` when an ``rdfs:label`` is declared — lets the
       LLM emit human-readable labels.
    3. ``_normalize_predicate_name(local_name).lower()`` — so that
       UPPER_SNAKE_CASE forms emitted by the extraction prompt (such as
       ``WORKS_FOR``) fold to the camelCase form stored in the ontology
       (``worksFor``) and still hit the index.

    A ``set`` is used to de-duplicate the three keys for a single
    property so a property whose local_name is already camelCase (where
    the normalized form equals the local_name) is not appended to its
    own list twice. Across properties we retain duplicates by
    ``setdefault``-then-``append`` so every distinct matching property
    appears exactly once in the candidate list.

    Args:
        classes: All :class:`OntologyClass` instances keyed by IRI.
            Not mutated.
        object_properties: All :class:`ObjectProperty` instances keyed
            by IRI. Not mutated.
        datatype_properties: All :class:`DatatypeProperty` instances
            keyed by IRI. Not mutated.

    Returns:
        A 4-tuple ``(by_local, by_label, obj_pred_idx, dt_pred_idx)``:

        * ``by_local``: lowercase class ``local_name`` → class IRI.
        * ``by_label``: lowercase class ``rdfs:label`` → class IRI.
        * ``obj_pred_idx``: lowercase predicate key → list of
          :class:`ObjectProperty` IRIs whose ``local_name`` / label /
          normalized name produces that key.
        * ``dt_pred_idx``: same shape for :class:`DatatypeProperty`.
    """
    # ---- Class local-name index (first-declared wins) --------------------
    by_local: Dict[str, str] = {}
    for iri, cls_ in classes.items():
        key = cls_.local_name.lower()
        if not key:
            # Defensive: a class IRI that produced an empty local_name is
            # already odd; skip it here rather than claim the empty key.
            continue
        existing = by_local.get(key)
        if existing is not None:
            logger.debug(
                "Class local-name collision on %r: %s already claims "
                "this key, skipping %s (first-declared wins).",
                key, existing, iri,
            )
            continue
        by_local[key] = iri

    # ---- Class label index (first-declared wins) -------------------------
    by_label: Dict[str, str] = {}
    for iri, cls_ in classes.items():
        if cls_.label is None:
            continue
        stripped = cls_.label.strip()
        if not stripped:
            # Whitespace-only labels contribute no useful key.
            continue
        key = stripped.lower()
        existing = by_label.get(key)
        if existing is not None:
            logger.debug(
                "Class label collision on %r: %s already claims this "
                "key, skipping %s (first-declared wins).",
                key, existing, iri,
            )
            continue
        by_label[key] = iri

    # ---- Object-property predicate index (multi-valued) ------------------
    obj_pred_idx: Dict[str, List[str]] = {}
    for iri, op in object_properties.items():
        keys: Set[str] = set()
        if op.local_name:
            keys.add(op.local_name.lower())
            # UPPER_SNAKE → camelCase folding so prompt-style emissions
            # (e.g. WORKS_FOR) hit the index for :worksFor.
            normalized = _normalize_predicate_name(op.local_name)
            if normalized:
                keys.add(normalized.lower())
        if op.label:
            stripped = op.label.strip()
            if stripped:
                keys.add(stripped.lower())
        for key in keys:
            obj_pred_idx.setdefault(key, []).append(iri)

    # ---- Datatype-property predicate index (multi-valued) ----------------
    dt_pred_idx: Dict[str, List[str]] = {}
    for iri, dp in datatype_properties.items():
        keys = set()
        if dp.local_name:
            keys.add(dp.local_name.lower())
            normalized = _normalize_predicate_name(dp.local_name)
            if normalized:
                keys.add(normalized.lower())
        if dp.label:
            stripped = dp.label.strip()
            if stripped:
                keys.add(stripped.lower())
        for key in keys:
            dt_pred_idx.setdefault(key, []).append(iri)

    return by_local, by_label, obj_pred_idx, dt_pred_idx
