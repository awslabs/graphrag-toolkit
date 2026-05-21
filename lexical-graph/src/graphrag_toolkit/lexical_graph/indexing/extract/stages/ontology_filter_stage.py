# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Ontology filter stage — strict-mode post-extraction filtering.

This module is the **strict-mode counterpart** to the suggestion-mode
``ontology_schema`` module. It provides :class:`OntologyFilterStage`, an
:class:`ExtractionStage` that post-processes the ``TopicCollection``
emitted by :class:`LLMTopicExtractionStage` and drops entities and facts
that do not conform to the ontology.

Mode selection is pipeline composition, not configuration:

* When ``OntologyFilterStage`` is **absent** from the stage list, the
  pipeline runs in suggestion mode — the ontology reaches the LLM as
  prompt guidance via ``OntologySchema.as_extraction_schema()`` but
  nothing filters afterwards.
* When ``OntologyFilterStage`` is **present**, every surviving entity
  and fact in the output ``TopicCollection`` conforms to the ontology.

Separability (Requirement 14, NFR-7):

* This module lives in a file separate from ``ontology_schema.py`` so
  that strict-mode code can be removed or disabled as a self-contained
  unit without touching suggestion-mode code.
* The reverse import (``ontology_schema.py`` importing from this
  module) MUST NOT exist. We import :class:`OntologySchema` from the
  suggestion-mode module, not the other way around.
* Deleting this file must leave suggestion-mode code working. The
  conditional re-export in ``stages/__init__.py`` (Task 4.6) and
  ``extract/__init__.py`` guards against the ``ImportError`` that
  absence would otherwise produce.
* :data:`ENABLE_ONTOLOGY_FILTER_STAGE` is a runtime kill-switch for
  operators who want to disable strict mode without deleting the
  module — setting it to ``False`` causes
  :class:`OntologyFilterStage.__init__` to raise ``RuntimeError``
  naming this constant (Requirement 14.6).
"""

from ..ontology_schema import OntologySchema

# Runtime kill-switch for strict-mode filtering (Requirement 14.6).
#
# Default: ``True`` — strict mode is active when ``OntologyFilterStage``
# is in the pipeline.
#
# When set to ``False``, :class:`OntologyFilterStage.__init__` raises
# ``RuntimeError`` with a message naming this constant. Operators can
# flip this flag at runtime to disable strict mode without code changes
# or redeployment.
ENABLE_ONTOLOGY_FILTER_STAGE: bool = True


# --- Datatype literal validation (Requirement 10) -------------------
#
# The ``_validate_literal_against_xsd`` helper below is the single
# source of truth for "does this string parse as this XSD type under
# strict semantics". It is the predicate behind the
# ``validate_datatypes=True`` path of ``OntologyFilterStage``; the
# ``validate_datatypes=False`` opt-out simply bypasses the call.
#
# The dispatch table is a literal transcription of design
# §"Algorithm: datatype literal validation" — no type promotion, no
# coercion, no "close enough" matching. Mis-typed literals drop; users
# who need laxer behaviour opt out via ``validate_datatypes=False``.

import logging
import re
from urllib.parse import urlparse

from ..ontology_schema import XSD_NAMESPACE

logger = logging.getLogger(__name__)

# Module-level guard so each unknown XSD type logs exactly one WARN per
# pipeline run (Requirement 10.8). The guard state is tied to the
# Python process — a fresh interpreter reset clears it, which matches
# the "per pipeline run" wording in the requirement.
_warned_unknown_types: "set[str]" = set()

# Integer subtype ranges per XSD 1.1 (signed two's-complement widths).
# ``xsd:integer`` itself is unbounded and is therefore handled
# separately in the dispatch below.
_INTEGER_SUBTYPE_RANGES = {
    "byte": (-128, 127),
    "short": (-32768, 32767),
    "int": (-(2**31), 2**31 - 1),
    "long": (-(2**63), 2**63 - 1),
}

_INTEGER_REGEX = re.compile(r"^-?\d+$")
_DECIMAL_REGEX = re.compile(r"^-?\d+(\.\d+)?$")

# Permissive ISO-8601 regexes. rdflib's native datatype coercion is
# stricter than the XSD 1.1 grammar in places, so we keep these loose
# enough to accept what rdflib would round-trip (optional timezone,
# optional fractional seconds on dateTime) without over-rejecting.
_ISO_DATE_REGEX = re.compile(r"^-?\d{4}-\d{2}-\d{2}(Z|[+-]\d{2}:\d{2})?$")
_ISO_DATETIME_REGEX = re.compile(
    r"^-?\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"
)


def _validate_literal_against_xsd(literal: str, xsd_datatype: str) -> bool:
    """Return ``True`` iff ``literal`` parses as ``xsd_datatype`` under strict XSD semantics.

    Implements design §"Algorithm: datatype literal validation" exactly.
    Called from ``OntologyFilterTransform._filter_spc_fact`` when
    ``validate_datatypes`` is ``True``. Bypassed entirely when the
    caller opts out via ``validate_datatypes=False`` — see Requirement
    9.2.

    Supported types:

    * ``xsd:string`` — any string (Requirement 10.1).
    * ``xsd:boolean`` — one of ``"true"``, ``"false"``, ``"1"``, ``"0"``
      (Requirement 10.2).
    * ``xsd:integer`` / ``xsd:int`` / ``xsd:long`` / ``xsd:short`` /
      ``xsd:byte`` — matches ``^-?\\d+$`` and (for subtypes) falls
      within the signed range of the type (Requirement 10.3).
    * ``xsd:decimal`` — matches ``^-?\\d+(\\.\\d+)?$`` (Requirement 10.4).
    * ``xsd:float`` / ``xsd:double`` — parses via ``float(literal)``
      without raising (Requirement 10.5).
    * ``xsd:date`` — ISO-8601 date ``YYYY-MM-DD`` with optional
      timezone suffix (Requirement 10.6).
    * ``xsd:dateTime`` — ISO-8601 datetime with optional fractional
      seconds and optional timezone (Requirement 10.6).
    * ``xsd:anyURI`` — parses via ``urlparse`` without raising and is
      non-empty (Requirement 10.7).

    Any XSD IRI outside the set above is accepted permissively: the
    function returns ``True`` and emits exactly one WARN log per
    unknown datatype per pipeline run (Requirement 10.8). The same
    branch handles the defensive case of an IRI that is not in the
    XSD namespace at all — ontologies are validated at load time so
    this is unreachable via the normal path, but we accept it rather
    than raise.

    Args:
        literal: The string form of the SPC complement as produced by
            the LLM.
        xsd_datatype: The XSD IRI declared by the matching
            ``DatatypeProperty`` (for example
            ``"http://www.w3.org/2001/XMLSchema#integer"``).

    Returns:
        ``True`` if ``literal`` conforms to ``xsd_datatype`` (or is
        permissively accepted for an unknown type); ``False``
        otherwise.
    """
    # Non-XSD IRIs are defensive-only: ``OntologySchema.from_turtle``
    # already rejects DatatypeProperties whose range is outside the XSD
    # namespace (Requirement 3.4). If we ever see one here we still
    # don't want to crash — fall through to the unknown-type path.
    if not xsd_datatype.startswith(XSD_NAMESPACE):
        _warn_unknown_type_once(xsd_datatype)
        return True

    type_name = xsd_datatype[len(XSD_NAMESPACE):]

    if type_name == "string":
        return True

    if type_name == "boolean":
        return literal in ("true", "false", "1", "0")

    if type_name == "integer":
        # Unbounded integer — just shape.
        return bool(_INTEGER_REGEX.match(literal))

    if type_name in _INTEGER_SUBTYPE_RANGES:
        if not _INTEGER_REGEX.match(literal):
            return False
        # Python ``int`` is arbitrary-precision, so we must compare
        # against the declared signed range of the subtype.
        try:
            value = int(literal)
        except ValueError:  # pragma: no cover - regex already guards
            return False
        low, high = _INTEGER_SUBTYPE_RANGES[type_name]
        return low <= value <= high

    if type_name == "decimal":
        return bool(_DECIMAL_REGEX.match(literal))

    if type_name in ("float", "double"):
        try:
            float(literal)
            return True
        except (ValueError, TypeError):
            return False

    if type_name == "date":
        return bool(_ISO_DATE_REGEX.match(literal))

    if type_name == "dateTime":
        return bool(_ISO_DATETIME_REGEX.match(literal))

    if type_name == "anyURI":
        # ``urlparse`` is extremely permissive — it accepts almost any
        # string without raising. We explicitly reject the empty
        # string to catch the obvious garbage case; everything else
        # passes. Users who need strict URI validation should opt out
        # via ``validate_datatypes=False`` and validate externally.
        if not literal:
            return False
        try:
            urlparse(literal)
            return True
        except Exception:  # pragma: no cover - urlparse rarely raises
            return False

    # Unknown XSD type: permissive per Requirement 10.8.
    _warn_unknown_type_once(xsd_datatype)
    return True


def _warn_unknown_type_once(xsd_datatype: str) -> None:
    """Emit exactly one WARN log per unknown XSD type per pipeline run.

    Uses the module-level :data:`_warned_unknown_types` set as a
    simple memoization guard. The guard is tied to the Python process
    lifetime: a fresh interpreter starts with an empty set, which
    matches the "per pipeline run" wording in Requirement 10.8.
    Callers who exercise the same unknown type repeatedly within one
    run will therefore see one log line total, not one per call.
    """
    if xsd_datatype in _warned_unknown_types:
        return
    _warned_unknown_types.add(xsd_datatype)
    logger.warning(
        "Unknown XSD datatype %s encountered in ontology-guided "
        "extraction; accepting all literals for this type. Add it to "
        "_validate_literal_against_xsd for strict validation or opt "
        "out via OntologyFilterStage(ontology, validate_datatypes="
        "False).",
        xsd_datatype,
    )


# --- OntologyFilterTransform + OntologyFilterStage (Tasks 4.3–4.5) ----
#
# These two classes are the post-extraction enforcement surface of
# strict mode. ``OntologyFilterTransform`` is the
# ``TransformComponent`` that the ingestion pipeline actually invokes;
# ``OntologyFilterStage`` is the :class:`ExtractionStage` wrapper that
# composes into ``ExtractionConfig.from_stages(...)`` alongside the
# existing stages.
#
# Structurally this mirrors ``SchemaFilter`` /
# ``SchemaFilterStage`` in ``schema_filter_stage.py`` — same pydantic
# Field descriptor shape, same ``arbitrary_types_allowed=True`` config
# (since :class:`OntologySchema` is a plain ``@dataclass`` rather than
# a pydantic model), and the same single-pass rewrite of
# ``node.metadata[TOPICS_KEY]`` with every other metadata key left
# untouched (Requirement 7.4).

from typing import List, Optional, Sequence

from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import BaseNode, TransformComponent
from pydantic import ConfigDict

from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import (
    ExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.model import (
    Entity,
    Fact,
    Topic,
    TopicCollection,
)


class OntologyFilterTransform(TransformComponent):
    """TransformComponent that filters extracted topics against an :class:`OntologySchema`.

    Implements the main loop from design §"Algorithm: OntologyFilterStage
    main loop". For every node in the input, deserialize the
    ``TopicCollection`` at ``metadata[TOPICS_KEY]``, drop every entity
    whose classification does not resolve to an ontology class
    (Requirement 6.1), drop every SPO / SPC fact that does not match a
    declared ``ObjectProperty`` / ``DatatypeProperty`` with
    subclass-closure-aware domain and range coverage (Requirements
    6.2–6.5, 9.1–9.3), drop any malformed fact that has neither an
    object nor a complement (Requirement 6.6), and drop any topic left
    with zero surviving entities and zero surviving facts
    (Requirements 7.1, 7.2). The relative order of surviving topics is
    preserved (Requirement 7.3).

    Nodes whose metadata does not carry ``TOPICS_KEY`` pass through
    unchanged (Requirement 7.5); no other key on ``node.metadata`` is
    read or written (Requirement 7.4).

    Fields:
        ontology: The in-memory ontology used to answer resolver and
            subclass-closure questions during filtering.
        validate_datatypes: When ``True`` (default), SPC facts are
            additionally required to satisfy
            :func:`_validate_literal_against_xsd` against the declared
            XSD datatype. When ``False``, the literal check is
            bypassed while subject-class and predicate-in-ontology
            checks still apply (Requirements 9.1, 9.2, 9.3).
    """

    ontology: OntologySchema = Field(
        description="OntologySchema used to filter entities and facts.",
    )
    validate_datatypes: bool = Field(
        default=True,
        description=(
            "When True (default), SPC facts are dropped if their "
            "complement literal fails XSD validation. When False, the "
            "literal check is skipped; subject-class and "
            "predicate-in-ontology checks still apply."
        ),
    )

    # OntologySchema is a plain ``@dataclass`` (not a pydantic model),
    # so pydantic's strict type enforcement needs to be relaxed for
    # this field. The same pattern is used by ``SchemaFilter`` for
    # ``ExtractionSchema``.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def __call__(
        self, nodes: Sequence[BaseNode], **kwargs
    ) -> Sequence[BaseNode]:
        """Iterate ``nodes``, filter each one's ``TopicCollection`` in place.

        Node processing order does not affect output — each node is
        independent. Nodes missing ``TOPICS_KEY`` pass through without
        any metadata mutation (Requirement 7.5). Nodes with
        ``TOPICS_KEY`` have only that single key rewritten; every
        other metadata key is preserved byte-for-byte (Requirement
        7.4).

        The filtered ``TopicCollection`` is serialized back via
        ``model_dump()`` so downstream stages see the same shape that
        ``LLMTopicExtractionStage`` produced.
        """
        for node in nodes:
            topics_data = node.metadata.get(TOPICS_KEY)
            if topics_data is None:
                # Requirement 7.5 — pass through untouched.
                continue

            tc = TopicCollection(**topics_data)
            surviving_topics: List[Topic] = []

            # Preserve relative order of surviving topics
            # (Requirement 7.3). Iteration order of ``tc.topics`` is
            # insertion order per pydantic list semantics.
            for topic in tc.topics:
                filtered = self._filter_topic(topic)
                if filtered is not None:
                    surviving_topics.append(filtered)

            tc.topics = surviving_topics
            node.metadata[TOPICS_KEY] = tc.model_dump()

        return nodes

    # ------------------------------------------------------------------
    # Per-topic filter (Requirements 7.1, 7.2, 7.3)
    # ------------------------------------------------------------------

    def _filter_topic(self, topic: Topic) -> Optional[Topic]:
        """Filter entities and facts on a single topic; drop the topic if empty.

        Entities are filtered first so statement-level fact filtering
        sees the pruned entity list on the same topic. Returns the
        possibly-mutated topic when at least one entity or one fact
        survives; returns ``None`` when the topic is empty and should
        be removed from the ``TopicCollection`` (Requirements 7.1,
        7.2).
        """
        # Filter entities (Requirement 6.1). Using a list comprehension
        # preserves the relative order of surviving entities.
        topic.entities = [
            e for e in topic.entities if self._filter_entity(e) is not None
        ]

        # Filter facts within each statement. Statements themselves are
        # not removed by this feature — only their ``facts`` list is
        # rewritten — so downstream code that keys off
        # ``statementId`` / ``value`` keeps working.
        for stmt in topic.statements:
            kept_facts: List[Fact] = []
            for fact in stmt.facts:
                kept = self._route_fact(fact)
                if kept is not None:
                    kept_facts.append(kept)
            stmt.facts = kept_facts

        # Keep the topic iff something survived (Requirements 7.1, 7.2).
        any_fact_survived = any(stmt.facts for stmt in topic.statements)
        if not topic.entities and not any_fact_survived:
            return None
        return topic

    # ------------------------------------------------------------------
    # Per-entity and per-fact helpers (design §"Algorithm: OntologyFilterStage main loop")
    # ------------------------------------------------------------------

    def _filter_entity(self, entity: Entity) -> Optional[Entity]:
        """Keep ``entity`` iff its classification resolves to an ontology class.

        Implements Requirement 6.1. ``resolve_class`` already handles
        the ``None`` / empty-string cases by returning ``None``, so
        this is a one-line guard.
        """
        if self.ontology.resolve_class(entity.classification or "") is None:
            return None
        return entity

    def _route_fact(self, fact: Fact) -> Optional[Fact]:
        """Dispatch a fact to the SPO or SPC filter, or drop it if malformed.

        * ``fact.object is not None`` — SPO; check
          :meth:`_filter_spo_fact` (Requirement 6.2).
        * ``fact.object is None`` and ``fact.complement is not None`` —
          SPC; check :meth:`_filter_spc_fact` (Requirement 6.3).
        * Both ``None`` — malformed, drop (Requirement 6.6). This
          guards against the rare LLM-parser edge case where a
          ``Fact`` is produced with no object and no complement; the
          toolkit's data model allows both fields to be ``None``
          simultaneously, so we handle it explicitly rather than rely
          on the dispatch falling through.
        """
        if fact.object is not None:
            return self._filter_spo_fact(fact)
        if fact.complement is not None:
            return self._filter_spc_fact(fact)
        return None

    def _filter_spo_fact(self, fact: Fact) -> Optional[Fact]:
        """Keep ``fact`` iff it matches some ``ObjectProperty`` with domain/range coverage.

        Implements design §"Algorithm: domain/range check with subclass
        closure". The fact is kept iff there exists at least one
        :class:`ObjectProperty` ``op`` such that:

        * ``op`` matches ``fact.predicate.value`` via
          :meth:`OntologySchema.resolve_object_predicate`;
        * ``op.domain`` is empty (``owl:Thing`` — matches any class)
          **or** some ``d ∈ op.domain`` satisfies
          ``is_subclass_of(subj_iri, d)``;
        * ``op.range`` is empty (``owl:Thing``) **or** some
          ``r ∈ op.range`` satisfies
          ``is_subclass_of(obj_iri, r)``.

        A miss at any step drops the fact. Covers Requirements 6.2,
        6.4, 6.5. Subclass closure is the only reasoning performed —
        see design §D4.
        """
        subj_iri = self.ontology.resolve_class(
            fact.subject.classification or ""
        )
        if subj_iri is None:
            return None
        # ``fact.object`` is guaranteed non-None by the caller
        # (``_route_fact``), so accessing ``.classification`` is safe.
        obj_iri = self.ontology.resolve_class(
            fact.object.classification or ""
        )
        if obj_iri is None:
            return None

        candidates = self.ontology.resolve_object_predicate(
            fact.predicate.value
        )
        if not candidates:
            return None

        for op in candidates:
            # Empty domain = owl:Thing — matches any class.
            domain_ok = not op.domain or any(
                self.ontology.is_subclass_of(subj_iri, d) for d in op.domain
            )
            if not domain_ok:
                continue
            range_ok = not op.range or any(
                self.ontology.is_subclass_of(obj_iri, r) for r in op.range
            )
            if range_ok:
                return fact
        return None

    def _filter_spc_fact(self, fact: Fact) -> Optional[Fact]:
        """Keep ``fact`` iff it matches some ``DatatypeProperty`` with domain coverage
        and (when ``validate_datatypes`` is True) a conforming literal.

        Implements the SPC branch of design §"Algorithm: OntologyFilterStage
        main loop" and the opt-out contract from Requirement 9:

        * Resolve the subject class; miss → drop (Requirement 6.3a).
        * Look up candidate datatype properties by predicate string;
          empty → drop (Requirement 6.3a, 9.3).
        * For each candidate, require domain coverage via subclass
          closure (Requirement 6.3b, 9.3). Note that ``DatatypeProperty``
          has no range of class IRIs — the "range" is the XSD datatype
          and is enforced by the literal check below, not by
          ``is_subclass_of``.
        * If ``validate_datatypes`` is ``True``, additionally require
          :func:`_validate_literal_against_xsd` to accept the
          complement literal (Requirement 6.3c, 9.1).
        * If ``validate_datatypes`` is ``False``, accept on the first
          domain-matching candidate (Requirement 9.2).

        The complement may be a plain string or an :class:`Entity`
        (``EntityType = Union[Entity, str]`` per the toolkit's data
        model); ``_complement_literal`` below extracts a string form in
        both cases.
        """
        subj_iri = self.ontology.resolve_class(
            fact.subject.classification or ""
        )
        if subj_iri is None:
            return None

        candidates = self.ontology.resolve_datatype_predicate(
            fact.predicate.value
        )
        if not candidates:
            return None

        literal = _complement_literal(fact.complement)

        for dp in candidates:
            domain_ok = not dp.domain or any(
                self.ontology.is_subclass_of(subj_iri, d) for d in dp.domain
            )
            if not domain_ok:
                continue
            if self.validate_datatypes:
                if _validate_literal_against_xsd(literal, dp.datatype):
                    return fact
                # Literal did not validate against this candidate's
                # datatype — try the next candidate. Some ontologies
                # declare multiple DatatypeProperties that share a
                # local_name but differ in XSD range; any one of them
                # validating is enough to keep the fact.
                continue
            # ``validate_datatypes=False`` — domain match alone is
            # sufficient (Requirement 9.2).
            return fact
        return None


def _complement_literal(complement) -> str:
    """Return a string form of an SPC fact's complement.

    The toolkit's ``Fact.complement`` field is typed as
    ``Optional[EntityType]`` where ``EntityType = Union[Entity, str]``
    (see ``indexing/model.py``). Both forms appear in practice —
    ``LLMTopicExtractionStage`` usually emits plain strings, but some
    upstream transformations wrap the literal in an :class:`Entity`.
    This helper collapses both shapes to a single string suitable for
    :func:`_validate_literal_against_xsd`.

    ``None`` is mapped to the empty string; the caller
    (``_filter_spc_fact``) only invokes this for facts whose
    complement is non-``None``, but the guard keeps the helper total.
    """
    if complement is None:
        return ""
    if isinstance(complement, Entity):
        return complement.value
    return str(complement)


class OntologyFilterStage(ExtractionStage):
    """:class:`ExtractionStage` that activates strict-mode ontology filtering.

    Presence of this stage in the ``ExtractionConfig.from_stages``
    stage list is what activates strict mode (Requirements 8.1, 8.4).
    The stage implements the :class:`ExtractionStage` ABC so it
    composes naturally with ``PipelineBuilder`` alongside the existing
    ``LLMTopicExtractionStage`` and ``SchemaFilterStage``; no pipeline
    builder, configuration, or prompt change is required (design §D2,
    NFR-3, NFR-4).

    The constructor honours the runtime kill-switch
    :data:`ENABLE_ONTOLOGY_FILTER_STAGE` (Requirement 14.6): when the
    flag is ``False``, ``__init__`` raises :class:`RuntimeError` with
    a message naming the constant so operators can disable strict
    mode without removing the module.

    Args:
        ontology: The :class:`OntologySchema` to enforce.
        validate_datatypes: Keyword-only flag, default ``True``.
            Forwarded to :class:`OntologyFilterTransform`. Disables the
            XSD literal check on SPC facts when ``False`` while
            preserving every other check (Requirements 9.1–9.3).
    """

    def __init__(
        self,
        ontology: OntologySchema,
        *,
        validate_datatypes: bool = True,
    ):
        # Kill-switch check (Requirement 14.6). Runs before any field
        # assignment so a disabled stage never holds a reference to the
        # ontology — eliminates any risk of a partially-constructed
        # stage leaking into the pipeline if a caller catches the
        # ``RuntimeError`` and proceeds.
        if not ENABLE_ONTOLOGY_FILTER_STAGE:
            raise RuntimeError(
                "OntologyFilterStage is disabled: "
                "ENABLE_ONTOLOGY_FILTER_STAGE is False. Strict-mode "
                "ontology filtering is turned off at the module level. "
                "To re-enable, set ENABLE_ONTOLOGY_FILTER_STAGE = True "
                "in ontology_filter_stage.py."
            )
        self._ontology = ontology
        self._validate_datatypes = validate_datatypes

    def input_keys(self) -> List[str]:
        """Strict-mode filter reads from ``TOPICS_KEY`` (Requirement 8.2)."""
        return [TOPICS_KEY]

    def output_keys(self) -> List[str]:
        """Strict-mode filter writes back to ``TOPICS_KEY`` (Requirement 8.2)."""
        return [TOPICS_KEY]

    def as_transform(self) -> TransformComponent:
        """Return a fresh :class:`OntologyFilterTransform` for the pipeline.

        A new instance is returned on each call so upstream ingestion
        code (which may cache transforms or wrap them in checkpoints)
        gets an independent object — matches the
        ``SchemaFilterStage.as_transform`` pattern.
        """
        return OntologyFilterTransform(
            ontology=self._ontology,
            validate_datatypes=self._validate_datatypes,
        )

    @property
    def stage_type(self) -> str:
        """Stage type identifier for :class:`PipelineBuilder`
        (Requirement 8.2)."""
        return "filter"
