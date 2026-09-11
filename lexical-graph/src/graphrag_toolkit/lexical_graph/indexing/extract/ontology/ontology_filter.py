# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The one component in this feature that changes extracted data.

Everything else the ontology produces is static: a constraint string rendered
into the prompt, and `OntologyIndex`. Neither touches a fact. `OntologyFilter`
runs after the topic extractor and rewrites what the model emitted - renaming
resolved terms to the ontology's own spelling, dropping facts that violate a
declared shape, and recording the resolution on the surviving facts so the build
stage needs no ontology knowledge of its own.

Per fact, in this order:

1. **Resolve** every name once - subject class, object class, predicate.
2. **Normalize** the resolved names to their authored spelling.
3. **Enforce**, each dimension gated on its own flag.
4. **Annotate** the survivors.

Resolution happens before normalization rather than after, which is stronger
than enforcing on the rewritten name would be: enforcement decides
on the resolved *term*, so it can never see a pre-rewrite spelling at all. This
is only sound because resolution is idempotent - a term's authored name is
indexed under its own resolution key, so resolving the rewritten name returns
the same term. `TestIdempotence` asserts that rather than trusting it.

Normalization writes the authored name **verbatim**: `rdfs:label` if the term
declares one, otherwise the IRI local name, with no case, separator or
whitespace transformation applied. `:worksFor` stores `worksFor` and
`:WORKS_FOR` stores `WORKS_FOR`; this module imposes no house convention on
either. That is deliberately *not* what the prompt renders - a class authored
`:SportsTeam` with no label renders as `Sports Team` so it survives the response
parser's `.title()`, and stores as `SportsTeam`. The two must be allowed to
disagree, which is why the `naming.py` rendering helpers are not reused here.

**What this cannot do.** The filter's entire reach is exact match modulo case
and separator style, plus declared `rdfs:label` and `skos:altLabel` values.
Word boundaries are *not* folded away: `resolution_key` maps `worksFor`,
`WORKS_FOR` and `WORKS FOR` onto one key, but `Sportsteam` and `SportsTeam` are
one word and two, so they do not meet. Recognising that `HIRED BY` means the
ontology's employment concept is
the *model's* job, done at extraction time in response to the rendered prompt.
`HIRED BY` against an ontology declaring `worksFor` resolves to nothing and is
left exactly as the parser produced it. Enforcement checks shape, never meaning:
a semantically wrong but resolvable mapping is renamed, annotated, passed by
domain and range if the classes happen to fit, and is then indistinguishable in
the graph from a correct one.

**Process boundary.** Extraction runs `ProcessPoolExecutor(mp_context='spawn')`,
so this component is pickled per node batch per worker. It therefore holds only
`OntologyIndex` - plain `str`, `List[str]`, `Dict[str, ...]` and `FrozenSet[str]`
- never an `rdflib.Graph`, declares **no custom `__init__`** because
`BaseComponent.__setstate__` calls `self.__init__(**state['__dict__'])`, and
imports no rdflib so workers do not pay for the import. Every import below is
from a submodule rather than from the `ontology` package, whose `__init__`
imports `ontology.py` and therefore rdflib.
"""

import logging
import re

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Union

from llama_index.core.schema import BaseNode, TransformComponent

from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.datatype_utils import (
    coerce_literal,
    validates_datatype,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.naming import (
    resolution_key,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_index import (
    DatatypeProperty,
    ObjectProperty,
    OntologyClass,
    OntologyIndex,
)
from graphrag_toolkit.lexical_graph.indexing.model import (
    Entity,
    Fact,
    Topic,
    TopicCollection,
)

logger = logging.getLogger(__name__)

OntologyProperty = Union[ObjectProperty, DatatypeProperty]

# The drop counters, in the order `_enforce` applies its gates, each paired with
# the setting that produced it. The report names the *setting* rather than the
# dimension, because the action a count implies is turning that setting off - and
# a gate the user left off has a zero count and so cannot appear at all, which
# means the report can never attribute a drop to a gate that did not run.
_DROP_DIMENSIONS = (
    ('facts_dropped_type_restatement', 'drop_type_restatements'),
    ('facts_dropped_entity_type', 'enforce_entity_types'),
    ('facts_dropped_relationship_type', 'enforce_relationship_types'),
    ('facts_dropped_domain_range', 'enforce_domain_range'),
    ('facts_dropped_datatype', 'enforce_datatypes'),
)

_NON_ALPHANUMERIC = re.compile(r'[^a-z0-9]')

def _compact(name:str) -> str:
    """Fold a name to lowercase alphanumerics, dropping every separator.

    Deliberately coarser than `resolution_key`, which preserves word boundaries
    because it has to round-trip against the prompt rendering. Here the opposite
    is wanted: the names being matched are ones no ontology declared and no
    renderer produced, so there is no round trip to preserve, and every spelling
    a model might reach for should land on one key.

        'rdf:type' 'RDF_TYPE' 'rdf type'   -> 'rdftype'
        'subClassOf' 'SUBCLASS_OF'         -> 'subclassof'
        'isA' 'IS_A' 'is a'                -> 'isa'

    Note what this gives up: it cannot tell `rdfs:label` from a predicate
    genuinely named `RDFSLABEL`. That is why the prefixed family below is
    matched on the *whole* compacted string, prefix included - stripping the
    prefix would fold `rdfs:domain` onto `DOMAIN`, and a web domain is a real
    attribute.
    """
    return _NON_ALPHANUMERIC.sub('', (name or '').lower())

# Predicates that are ontology *language* rather than domain vocabulary. A fact
# whose predicate is one of these is never a statement about the world, whatever
# its value, so `drop_type_restatements` drops it unconditionally.
#
# Matched with the prefix attached, which is what makes the set safe: `rdfs:label`
# is meta, `LABEL` is a perfectly good attribute of a parcel, and only the prefix
# distinguishes them. A model that emits the bare local name is caught by the
# restatement rule below instead, if it is caught at all.
#
# Empirically empty on the recorded corpus - no predicate any model produced
# contains a colon. It is here because the failure it guards is the one the
# `turtle` vocabulary format invites, and a model that does write `rdf:type`
# verbatim should not need a code change to be handled.
_ONTOLOGY_LANGUAGE_PREDICATES = frozenset(
    _compact(name) for name in (
        'rdf:type', 'rdf:Property', 'rdf:value',
        'rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdfs:domain', 'rdfs:range',
        'rdfs:label', 'rdfs:comment', 'rdfs:isDefinedBy', 'rdfs:seeAlso',
        'owl:Class', 'owl:ObjectProperty', 'owl:DatatypeProperty',
        'owl:sameAs', 'owl:equivalentClass', 'owl:equivalentProperty',
        'owl:Thing', 'owl:NamedIndividual',
        'skos:prefLabel', 'skos:altLabel', 'skos:broader', 'skos:narrower',
    )
)

# Predicates whose meaning is "this entity is of type X". Membership here is not
# enough to drop a fact: the value must also restate the subject's own
# classification, because every one of these words is a legitimate domain
# attribute against some ontology. `Ship|CLASS|Destroyer` stays unless the ship
# is already classified `Destroyer`, at which point the fact is a duplicate of
# the entity's own label and carries nothing.
#
# `a` is Turtle's own type keyword, and `describedBy` is on the list for a reason
# that is empirical rather than semantic: it is the worked example the base
# extraction prompt gives for attribute naming, so models reach for it when they
# have a class name and nowhere to put it. Both are safe here only because of the
# restatement requirement.
#
# The list will drift - these are the forms three models produced on one corpus,
# plus the obvious neighbours. A form that is missing is a cleanup not performed,
# never a wrong drop, which is the asymmetry that makes an incomplete list
# acceptable.
_TYPE_ASSERTING_PREDICATES = frozenset(
    _compact(name) for name in (
        'a', 'type', 'typeOf', 'entityType', 'instanceOf', 'isA', 'isAn',
        'class', 'classification', 'classifiedAs', 'category', 'categoryOf',
        'kind', 'kindOf', 'subClassOf', 'subTypeOf', 'describedBy',
    )
)

# Datatype IRIs already warned about, per process. Module level rather than
# instance level so a fresh `OntologyFilter` per node batch does not re-warn -
# the component is rebuilt on every unpickle. See
# `OntologyFilter._warn_unvalidated_datatype` for why per-process is the honest
# contract. Exposed under this name so tests can clear it.
_warned_unvalidated_datatypes = set()

def _complement_literal(complement:Optional[Any]) -> Optional[str]:
    """The string form of a fact's complement.

    `Fact.complement` is `Optional[EntityType]` where
    `EntityType = Union[Entity, str]`, and both forms occur: the parser builds an
    `Entity`, while a hand-constructed or older payload may carry a bare `str`.
    """
    if complement is None:
        return None
    if isinstance(complement, Entity):
        return complement.value
    return str(complement)

def authored_name(term:Union[OntologyClass, OntologyProperty]) -> str:
    """The term's name as the ontology author wrote it.

    `rdfs:label` when declared, otherwise the IRI local name, returned verbatim.
    This is the canonical stored spelling and is
    deliberately unrelated to the prompt rendering convention - see the module
    docstring, and `naming.py`, which documents collapsing the two into one
    helper as the bug it exists to prevent.

    Args:
        term: Any declared class or property from the index.

    Returns:
        The authored name. Never empty: `local_name` is always populated.
    """
    return term.label or term.local_name

@dataclass
class FilterCounters:
    """What the filter changed, over one `__call__`.

    Rewrites are counted per dimension because they are not equivalent:
    a classification rewrite changes entity *identity*, since
    `include_classification_in_entity_id` defaults to `True` and
    `create_entity_id` hashes the classification into the node id, whereas a
    predicate rewrite only changes an edge label.

    Accumulated per `__call__` as a local, never as component state - the
    component is pickled per node batch per worker, so an instance attribute
    would count one worker's share of one batch and read as a total.

    `OntologyFilter._report` turns these into the log line `report_violations`
    asks for. Counting happens either way; only the logging is gated.
    """
    classifications_rewritten:int = 0
    predicates_rewritten:int = 0
    facts_dropped_type_restatement:int = 0
    facts_dropped_entity_type:int = 0
    facts_dropped_relationship_type:int = 0
    facts_dropped_domain_range:int = 0
    facts_dropped_datatype:int = 0

    def facts_dropped(self) -> int:
        """Total facts dropped, across all five dropping dimensions.

        Summed from `_DROP_DIMENSIONS` rather than by naming the fields, so a
        sixth gate cannot be added with a counter that the total silently omits.
        """
        return sum(getattr(self, field) for (field, _) in _DROP_DIMENSIONS)

    def any_change(self) -> bool:
        """Whether the filter altered anything at all.

        Annotation is excluded deliberately: it happens to every surviving fact
        at every level, so counting it would make every call a change and the
        report would carry no signal.
        """
        return bool(
            self.classifications_rewritten
            or self.predicates_rewritten
            or self.facts_dropped()
        )

    def summary(self) -> str:
        """The counts as a log fragment.

        The three totals are always present, so a reader can tell zero from
        absent. The per-dimension breakdown lists only the dimensions that
        dropped something, since a gate that is off contributes an unbreakable
        zero and naming it would suggest it ran.
        """
        parts = [
            f'classifications rewritten: {self.classifications_rewritten}',
            f'predicates rewritten: {self.predicates_rewritten}',
            f'facts dropped: {self.facts_dropped()}',
        ]

        breakdown = ', '.join(
            f'{setting}: {getattr(self, field)}'
            for (field, setting) in _DROP_DIMENSIONS
            if getattr(self, field)
        )

        if breakdown:
            parts.append(f'dropped by: {breakdown}')

        return ', '.join(parts)

@dataclass
class FactResolution:
    """What the index says about one fact, resolved once and reused.

    Attributes:
        subject_class (Optional[OntologyClass]): The subject's declared class,
            or `None` if its classification resolved to nothing.
        object_class (Optional[OntologyClass]): As above for the object. Always
            `None` on a fact with no object.
        predicate (Optional[OntologyProperty]): The declared property the
            predicate resolved to, or `None`.
        predicate_is_datatype (bool): True when `predicate` is a
            `DatatypeProperty`. Read rather than re-tested with `isinstance`
            at each use.
    """
    subject_class:Optional[OntologyClass] = None
    object_class:Optional[OntologyClass] = None
    predicate:Optional[OntologyProperty] = None
    predicate_is_datatype:bool = False

class OntologyFilter(TransformComponent):
    """Rewrites, filters and annotates extracted topics against an ontology.

    Reads and writes exactly one metadata key, `TOPICS_KEY`. A node without it
    passes through untouched.

    Attributes:
        index (OntologyIndex): The plain-data read index. The only state, and
            the only thing pickled into a worker.
        normalize_names (bool): Rewrite resolved names to their authored
            spelling.
        drop_type_restatements (bool): Drop facts that carry ontology language
            rather than a statement about the world. See
            `_carries_no_domain_fact`. The one dropping gate that is not an
            ontology-conformance check, and the only one on at `align`.
        enforce_entity_types (bool): Drop facts whose subject or object
            classification does not resolve to a declared class.
        enforce_relationship_types (bool): Drop facts whose predicate does not
            resolve to a declared property.
        enforce_domain_range (bool): Drop facts violating a declared
            `rdfs:domain` or `rdfs:range`, honouring subclass closure.
        enforce_datatypes (bool): Drop facts whose literal does not coerce to
            the declared XSD datatype.
        report_violations (bool): Log what this call rewrote and dropped, per
            dimension, at INFO. Counting happens regardless; only the logging is
            gated on this. See `_report` for what "once per extraction" can
            honestly mean on the far side of a spawned boundary, and for why
            these counts are not a divergence measure.

    Note:
        Annotation is not flag-gated. A fact that survives is annotated even
        with every `enforce_*` off, because that is how typed storage works at
        `ontology_authority='off'`.
    """

    index:OntologyIndex

    normalize_names:bool = False
    drop_type_restatements:bool = False
    enforce_entity_types:bool = False
    enforce_relationship_types:bool = False
    enforce_domain_range:bool = False
    enforce_datatypes:bool = False
    report_violations:bool = False

    @classmethod
    def class_name(cls) -> str:
        """Stable name for llama-index component serialization."""
        return 'OntologyFilter'

    def __call__(self, nodes:Sequence[BaseNode], **kwargs:Any) -> Sequence[BaseNode]:
        """Filter each node's `TopicCollection` in place.

        Nodes are independent, so processing order does not affect the result.

        Args:
            nodes: The nodes leaving the topic extractor.
            **kwargs: Ignored; present because `TransformComponent` passes them.

        Returns:
            The same sequence, with `TOPICS_KEY` rewritten on the nodes that
            carried one.
        """
        counters = FilterCounters()
        filtered = 0

        for node in nodes:
            topics_data = node.metadata.get(TOPICS_KEY)
            if topics_data is None:
                continue

            topics = TopicCollection.model_validate(topics_data)
            for topic in topics.topics:
                self._filter_topic(topic, counters)

            node.metadata[TOPICS_KEY] = topics.model_dump()
            filtered += 1

        self._report(counters, filtered)

        return nodes

    def _report(self, counters:FilterCounters, node_count:int) -> None:
        """Log what this call changed, gated on `report_violations`.

        Per-dimension counts are wanted once per extraction rather
        than per fact. One `__call__` is the largest unit actually available: it
        is one node batch in one worker, and the component is pickled per batch
        per worker, so a run-wide total would need state that crosses the spawn
        boundary. The line therefore says how many nodes it covers, so a reader
        adds the lines up rather than mistaking one for the total - the same
        honesty `_warn_unvalidated_datatype` settles for, and for the same
        reason.

        A call that changed nothing logs at DEBUG instead. Every gate is off by
        default, so on a normalize-only run most batches drop nothing at all, and
        a line of zeros per batch would bury the batches that did something.

        What the line is not: a measure of how far the corpus diverges from the
        ontology. The drop counters see only facts the model actually emitted,
        and the prompt is not neutral about that. At `align` no gate runs, so the
        drops are zero by construction. At `strict` the closing instruction tells
        the model that unlisted concepts are discarded anyway and it should leave
        those facts out, so what reaches a gate is the residue that ignored the
        instruction - non-compliance, not divergence, and much the smaller of the
        two. The combination that does count divergence is `align`'s prompt with
        the `enforce_*` gates overridden on: the model is not asked to suppress,
        so the gates see its full output.
        """
        if not self.report_violations:
            return

        logger.log(
            logging.INFO if counters.any_change() else logging.DEBUG,
            'Ontology filter [nodes: %d, %s]',
            node_count,
            counters.summary(),
        )

    def _filter_topic(self, topic:Topic, counters:FilterCounters) -> None:
        """Rewrite one topic's entities and facts in place.

        Both `topic.entities` and each fact's subject and object are visited.
        When the filter runs directly on parser output these are the *same*
        objects - `parse_extracted_topics` looks the subject up in the topic's
        entity dict - so the second visit is a no-op, which is sound only
        because normalization is idempotent. After a `model_dump()` round trip
        through `TOPICS_KEY`, which is how the pipeline actually delivers them,
        the sharing is gone and both visits do real work. Relying on the sharing
        to propagate a rewrite would therefore work in a unit test and silently
        fail in the pipeline.

        Topics are never dropped, and `topic.entities` is never pruned
: that list is not written to the graph, and pruning it
        would change entity extraction rather than fact conformance.
        """
        for entity in topic.entities:
            self._apply_entity_class(entity, counters)

        for statement in topic.statements:
            statement.facts = [
                fact for fact in statement.facts
                if self._filter_fact(fact, counters)
            ]

    def _filter_fact(self, fact:Fact, counters:FilterCounters) -> bool:
        """Normalize, enforce and annotate one fact.

        Returns:
            True to keep the fact, False to drop it.
        """
        resolution = self._resolve(fact)

        if self.normalize_names:
            self._apply_names(fact, resolution, counters)

        if not self._enforce(fact, resolution, counters):
            return False

        self._annotate(fact, resolution)
        return True

    # ------------------------------------------------------------------
    # Resolve
    # ------------------------------------------------------------------

    def _resolve(self, fact:Fact) -> FactResolution:
        """Look up every name on `fact`, once.

        Predicate resolution follows the fact's shape, which is the same
        convention used throughout: a fact with an object is a
        relation and resolves against object properties; a fact with only a
        complement is an attribute and resolves against datatype properties.

        The attribute case falls back to object properties on a miss, because
        the shape is not always what it looks like. `parse_extracted_topics`
        produces a complement whenever it could not match the object text to an
        entity it had already seen, so a genuine `worksFor` relation arrives
        complement-shaped whenever the employer was not listed in the entity
        block. Normalization rewrites a predicate that resolves to a declared
        *property*, not specifically to an object property, so refusing the
        fallback would leave those unnormalized.
        """
        resolution = FactResolution()

        resolution.subject_class = self.index.resolve_class(fact.subject.classification or '')

        if fact.object is not None:
            resolution.object_class = self.index.resolve_class(fact.object.classification or '')
            resolution.predicate = self.index.resolve_object_predicate(fact.predicate.value or '')
            return resolution

        datatype_property = self.index.resolve_datatype_predicate(fact.predicate.value or '')
        if datatype_property is not None:
            resolution.predicate = datatype_property
            resolution.predicate_is_datatype = True
            return resolution

        resolution.predicate = self.index.resolve_object_predicate(fact.predicate.value or '')
        return resolution

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------

    def _apply_names(self, fact:Fact, resolution:FactResolution, counters:FilterCounters) -> None:
        """Rewrite the fact's resolved names to their authored spelling.

        Unresolved names are left exactly as the parser produced them
.
        """
        self._rewrite_classification(fact.subject, resolution.subject_class, counters)
        if fact.object is not None:
            self._rewrite_classification(fact.object, resolution.object_class, counters)

        if resolution.predicate is not None:
            name = authored_name(resolution.predicate)
            if fact.predicate.value != name:
                fact.predicate.value = name
                counters.predicates_rewritten += 1

    def _apply_entity_class(self, entity:Entity, counters:FilterCounters) -> None:
        """Normalize and annotate a `Topic.entities` member.

        Topic entities carry no predicate, so this is the classification half of
        the fact path. Annotation is unconditional, matching facts: an entity
        whose class resolved records the IRI even with every `enforce_*` off.
        """
        ontology_class = self.index.resolve_class(entity.classification or '')
        if self.normalize_names:
            self._rewrite_classification(entity, ontology_class, counters)
        if ontology_class is not None:
            entity.classIri = ontology_class.iri

    def _rewrite_classification(
        self,
        entity:Optional[Entity],
        ontology_class:Optional[OntologyClass],
        counters:FilterCounters,
    ) -> None:
        """Rewrite one entity's classification, in place.

        In place rather than by replacement: within a topic
        straight off the parser, this object is also a member of
        `topic.entities`, and rebinding would desynchronize the two views.
        """
        if entity is None or ontology_class is None:
            return
        name = authored_name(ontology_class)
        if entity.classification != name:
            entity.classification = name
            counters.classifications_rewritten += 1

    # ------------------------------------------------------------------
    # Enforce
    # ------------------------------------------------------------------

    def _enforce(self, fact:Fact, resolution:FactResolution, counters:FilterCounters) -> bool:
        """Apply the enforcement gates, each on its own flag.

        The gates are **independent**: each one drops only what its own name
        says, and no gate implies another. That is the whole point of the
        per-dimension escape hatches - `enforce_domain_range`
        alone must not start rejecting unresolvable classifications, or a user
        who turned `enforce_entity_types` off would find it still on.

        The consequence is that "unknown" is not "violating". A fact whose
        subject classification resolves to nothing cannot be *shown* to breach a
        declared `rdfs:domain`, so the domain gate passes it and only
        `enforce_entity_types` rejects it. Likewise an unresolved predicate has
        no declared domain, range or datatype to violate, so only
        `enforce_relationship_types` rejects it. This is a deliberate divergence
        from the prior implementation, which bundled all three behind one
        `strict` flag and could not tell them apart.

        Order is type restatements, then entity types, then relationship types,
        then domain and range, then datatypes - broadest first. A fact breaching
        two dimensions is dropped once and counted once, under the first gate to
        reject it, because the counters break down *drops* rather than violations
        and must sum to the number of facts lost.

        `drop_type_restatements` goes first deliberately. At `strict` most of
        what it drops would fail `enforce_relationship_types` anyway - the
        predicates it matches are by construction ones the ontology does not
        declare - so the ordering does not change what survives, only which
        counter reports it. "This was a restatement of the entity's own class"
        is the more actionable of the two readings, and the less alarming: it
        says the model was redundant, not that the ontology was too narrow.

        Returns:
            True to keep the fact, False to drop it.
        """
        if self.drop_type_restatements and self._carries_no_domain_fact(fact, resolution):
            counters.facts_dropped_type_restatement += 1
            return False

        if self.enforce_entity_types:
            if resolution.subject_class is None:
                counters.facts_dropped_entity_type += 1
                return False
            if fact.object is not None and resolution.object_class is None:
                counters.facts_dropped_entity_type += 1
                return False

        if self.enforce_relationship_types and resolution.predicate is None:
            counters.facts_dropped_relationship_type += 1
            return False

        if self.enforce_domain_range and not self._domain_range_holds(fact, resolution):
            counters.facts_dropped_domain_range += 1
            return False

        if self.enforce_datatypes and not self._datatype_holds(fact, resolution):
            counters.facts_dropped_datatype += 1
            return False

        return True

    def _carries_no_domain_fact(self, fact:Fact, resolution:FactResolution) -> bool:
        """Whether the fact states ontology language instead of something about the world.

        Two independent sufficient conditions, each with its own justification:

        1. **The predicate is ontology language.** `rdf:type`, `rdfs:subClassOf`,
           `owl:sameAs` and their neighbours describe a vocabulary, never a
           company. Dropped whatever the value is. Safe unconditionally only
           because the prefix is required - see `_compact`.

        2. **The value restates the subject's own classification**, under a
           predicate whose meaning is type membership. `Meridian Freight
           [Company] |TYPE| Company` duplicates the entity's own label line and
           adds nothing; dropping it cannot lose information. This is the
           condition that fires in practice, and the two halves are both
           necessary: the value test alone would drop
           `Rovers|COMPETES_WITH|Sports Team`, which is at least arguable, and
           the predicate test alone would drop
           `Rovers|CLASSIFICATION|football club`, which is a real fact the
           classification does not carry.

        A predicate the ontology **declares** is never touched by either
        condition. An author who declares a datatype property called
        `classification` has said what it means, and this gate does not get to
        disagree - the same escape the rest of the module gives declared terms.

        Why this is a gate rather than a prompt fix: both failure modes are
        base-prompt behaviour, worst with no ontology at all (`|TYPE|` appears 43
        times in the `off` recordings against 0 at prose `strict`). The `turtle`
        vocabulary format re-exposes them because it shows type triples in the
        same shape as the requested output. Whether prompt wording could suppress
        them is an open question; this gate is what makes the format usable
        without waiting on that answer.

        Args:
            fact: The fact under consideration, after resolution.
            resolution: Its resolved terms. Only `predicate` is read, to spare a
                declared property.

        Returns:
            True to drop the fact.
        """
        if resolution.predicate is not None:
            return False

        predicate = _compact(fact.predicate.value or '')

        if predicate in _ONTOLOGY_LANGUAGE_PREDICATES:
            return True

        if predicate not in _TYPE_ASSERTING_PREDICATES:
            return False

        # Attribute shape only. A type restatement whose value matched a named
        # entity arrives relation-shaped, and then `object.value` is an entity in
        # its own right with its own classification rather than a bare class
        # word - a different thing, and one no model produced in seven recorded
        # arms, so it is left alone rather than guessed at.
        if fact.object is not None:
            return False

        value = _complement_literal(fact.complement)
        if not value:
            return False

        return resolution_key(value) == resolution_key(fact.subject.classification or '')

    def _domain_range_holds(self, fact:Fact, resolution:FactResolution) -> bool:
        """Whether the fact's classes satisfy the predicate's declared domain and range.

        Subclass closure is honoured through `OntologyClass.ancestors`, which is
        the reflexive transitive closure of `rdfs:subClassOf` - so a declared
        domain of `Agent` is satisfied by a `Company`, and by an `Agent`.

        An undeclared domain or range is `owl:Thing` and constrains nothing. An
        unresolved class or predicate is unknown rather than wrong; see
        `_enforce`. A datatype property has no class range at all - its range is
        the XSD type, which `_datatype_holds` checks - so only its domain is
        tested here.

        **This checks types, never meaning**. The corpus's one
        reproducible mislabelling, a board member recorded as
        `Priya Raman | WORKS FOR | Halcyon Motors`, satisfies
        `:worksFor rdfs:domain :Person; rdfs:range :Company` exactly and is kept
        by this gate at every authority level. Domain and range bound which
        entities a property may relate; they cannot see a wrong predicate whose
        endpoints happen to fit.
        """
        if resolution.predicate is None:
            return True

        if not self._class_satisfies(resolution.subject_class, resolution.predicate.domain):
            return False

        if resolution.predicate_is_datatype:
            return True

        # Reached only for an object property, since the datatype case returned
        # above, so `range` is always present.
        return self._class_satisfies(resolution.object_class, resolution.predicate.range)

    def _class_satisfies(self, ontology_class:Optional[OntologyClass], constraint:Optional[str]) -> bool:
        """Whether `ontology_class` is `constraint` or one of its subclasses."""
        if constraint is None:
            return True
        if ontology_class is None:
            return True
        return self.index.is_subclass_of(ontology_class.iri, constraint)

    def _datatype_holds(self, fact:Fact, resolution:FactResolution) -> bool:
        """Whether the complement literal is a value of the declared XSD type.

        Only subject-predicate-complement facts resolving to a datatype property
        have a declared datatype, so everything else passes.

        When the declared type is one coercion does not implement, the literal is
        kept as text and the fact survives - but one WARN is emitted naming the
        type, because a value stored without validation must not look like a
        validated one.
        """
        if not resolution.predicate_is_datatype:
            return True

        datatype = resolution.predicate.datatype

        if not validates_datatype(datatype):
            self._warn_unvalidated_datatype(datatype, resolution.predicate.local_name)
            return True

        return coerce_literal(_complement_literal(fact.complement), datatype) is not None

    @staticmethod
    def _warn_unvalidated_datatype(datatype:str, property_name:str) -> None:
        """Warn once per process that a declared datatype was not enforced.

        Unconditional on `report_violations`: this reports a
        declaration the system did not honour, not a violation count, and silence
        would make an unvalidated value indistinguishable from a validated one.

        Deduped in a module-level set, so the honest contract is once per
        *process*. Extraction runs `mp_context='spawn'` and each worker imports
        this module afresh, so a run with `num_workers=4` can log the same type
        four times. Stated rather than papered over: the alternative is shared
        state across the process boundary, which is not worth it for a log line,
        and the prior implementation's "once per run" was only ever this too.

        The message does not overstate its reach. `Ontology` rejects a datatype
        property whose range is outside the XSD namespace at load time, so only
        an unimplemented *XSD* type reaches here - a non-XSD IRI is refused
        outright by `coerce_literal`.
        """
        if datatype in _warned_unvalidated_datatypes:
            return
        _warned_unvalidated_datatypes.add(datatype)
        logger.warning(
            'enforce_datatypes cannot validate %s, declared as the range of %s. '
            'Values for this and any other property declaring %s are stored as '
            'text without being checked, and facts carrying them are not '
            'dropped. Declare a range that coercion implements, or treat these '
            'values as unvalidated. Logged once per datatype per process.',
            datatype, property_name, datatype,
        )

    # ------------------------------------------------------------------
    # Annotate
    # ------------------------------------------------------------------

    def _annotate(self, fact:Fact, resolution:FactResolution) -> None:
        """Record the resolution on a surviving fact.

        Not gated on any flag. Written once here and never
        recomputed downstream, so the build stage reads an
        answer rather than an ontology, and a checkpoint cannot lose it.

        Every value is a plain `str` straight off the index, which holds no
        `rdflib` terms - the metadata is written through `json.dump` and
        revalidated under `ConfigDict(strict=True)`.

        `canonicalName` is the property's `local_name`, not its authored name:
        it is the key typed-property storage writes under, and `rdfs:label` may
        contain spaces.
        """
        if resolution.subject_class is not None:
            fact.subject.classIri = resolution.subject_class.iri
        if fact.object is not None and resolution.object_class is not None:
            fact.object.classIri = resolution.object_class.iri

        if resolution.predicate is None:
            return

        fact.predicate.propertyIri = resolution.predicate.iri
        fact.predicate.canonicalName = resolution.predicate.local_name

        if resolution.predicate_is_datatype and isinstance(fact.complement, Entity):
            fact.complement.datatype = resolution.predicate.datatype
