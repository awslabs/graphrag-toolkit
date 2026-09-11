# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.indexing.model import Fact, Entity
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import search_string_from, label_from, new_query_var, escape_cypher_label
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.indexing.constants import (
    COMPLEMENT_ENTITY_PROPERTIES,
    COMPLEMENT_PLACEMENTS,
    DEFAULT_CLASSIFICATION,
    LOCAL_ENTITY_CLASSIFICATION,
    RESERVED_ENTITY_PROPERTIES,
    SUBJECT_PLACEMENTS,
    TYPED_PROPERTIES_OFF,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.datatype_utils import coerce_literal
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity

from llama_index.core.schema import BaseNode

logger = logging.getLogger(__name__)

# The no-annotations warning state, and the reason it is module-level.
#
# A build that asks for typed properties and writes none is the worst outcome
# available, because the user's next move is to doubt the graph store rather than
# the pipeline order - the cause is almost always that extraction ran before the
# ontology existed, or from a checkpoint that predates it, so the facts carry no
# annotations for the builder to key a property from. Rebuilding does not fix
# that; re-extracting does.
#
# Detecting it needs state that outlives one fact: a builder sees facts one at a
# time and is never told a build has ended, so there is nowhere else to notice
# "none of them were annotated". Module-level means once per worker process,
# which is the honest scope - the same compromise `OntologyFilter` makes for its
# unvalidated-datatype warning, and for the same reason.
#
# The threshold exists so a single legitimately-unresolved fact cannot cry wolf.
# One fact whose predicate is not in the ontology carries no annotations and is
# entirely normal at `align`; twenty-five in a row with not one annotated is not
# something a working filter produces.
_NO_ANNOTATIONS_WARNING_AFTER = 25

_annotation_seen = False
_unannotated_facts = 0
_no_annotations_warned = False

def _has_ontology_annotations(fact:Fact) -> bool:
    """Return True if any part of this fact was annotated by the filter.

    Read across the whole fact rather than off the one field a placement needs,
    because the question being asked is "did the filter run at all", not "can
    this particular fact be written".
    """
    parts = [fact.subject, fact.object, fact.complement]
    if any(part is not None and part.classIri for part in parts):
        return True
    if fact.complement is not None and fact.complement.datatype:
        return True
    return bool(fact.predicate.propertyIri or fact.predicate.canonicalName)

def _warn_if_no_annotations(fact:Fact, typed_properties:str) -> None:
    """Warn once per process that typed properties were asked for and cannot be written.

    Silence is not an option here: nothing downstream fails, so
    the only signal the user would otherwise get is an absence.
    """
    global _annotation_seen, _unannotated_facts, _no_annotations_warned

    if typed_properties == TYPED_PROPERTIES_OFF or _annotation_seen or _no_annotations_warned:
        return

    if _has_ontology_annotations(fact):
        _annotation_seen = True
        return

    _unannotated_facts += 1

    if _unannotated_facts < _NO_ANNOTATIONS_WARNING_AFTER:
        return

    _no_annotations_warned = True
    logger.warning(
        'typed_properties=%r was requested, but none of the first %d facts in this '
        'build carry ontology annotations, so no typed properties can be written. '
        'This means extraction ran without the ontology filter - most often because '
        'the extracted facts predate the ontology, or come from a checkpoint that '
        'does. Re-extract; rebuilding the same facts will not add the annotations. '
        'Logged once per process.',
        typed_properties, _unannotated_facts
    )

def _reset_no_annotations_warning() -> None:
    """Clear the no-annotations warning state. For tests only."""
    global _annotation_seen, _unannotated_facts, _no_annotations_warned
    _annotation_seen = False
    _unannotated_facts = 0
    _no_annotations_warned = False

def _reserved_property_names(typed_properties:str) -> frozenset:
    """The `__Entity__` property names a typed property must not key.

    The same set `OntologyConfig._validate_no_reserved_property_names` computes,
    widened the same way: `typed_value` and `datatype` are only owned once
    complement placement is writing them. Recomputed here rather than shared as a
    function because the two callers reach it from opposite ends of the package -
    validation from the ontology config, this from a builder in a spawn worker -
    and neither module should import the other.
    """
    reserved = set(RESERVED_ENTITY_PROPERTIES)
    if typed_properties in COMPLEMENT_PLACEMENTS:
        reserved.update(COMPLEMENT_ENTITY_PROPERTIES)
    return frozenset(reserved)

def _typed_subject_property(fact:Fact, typed_properties:str):
    """Resolve the one typed property a fact contributes to its subject, if any.

    Reserved names, unannotated facts and uncoercible literals are all a question
    about *this* fact, so they are answered in one place and returned as data, rather than spread across
    guard clauses in `build`.

    `complement.datatype` is the discriminator, not `predicate.canonicalName`.
    `OntologyFilter._annotate` sets `canonicalName` for every resolved predicate
    including object properties, and sets `datatype` only when the predicate
    resolved to a declared *datatype* property with a literal complement. Keying
    off `canonicalName` alone would write the object entity's display string into
    an attribute slot on the subject.

    Args:
        fact: The validated fact, after `string_complement_to_entity`.
        typed_properties: The resolved placement.

    Returns:
        A `(key, value)` pair, or `None` when nothing should be written. `value`
        may be `False`, `0` or `0.0`, so callers must test the pair, not the value.
    """
    if typed_properties not in SUBJECT_PLACEMENTS:
        return None

    key = fact.predicate.canonicalName
    complement = fact.complement

    # An unresolved predicate, or one that resolved to an object
    # property, has no declared datatype and contributes no typed property.
    if not key or not isinstance(complement, Entity) or not complement.datatype:
        return None

    # A hostile property name can be rejected or escaped, and a line break is the
    # one case where rejecting is clearly better.
    # `escape_cypher_label` handles every other character by doubling backticks, and
    # a quoted identifier may legally contain a newline - but it would split the
    # query across lines the rest of the builder assumes are one statement each, and
    # a raw line break cannot occur in a Turtle IRI or prefixed name, so a name
    # carrying one did not come from a well-formed ontology.
    if '\n' in key or '\r' in key:
        logger.warning(
            'Skipping typed property %r on entity %r: the property name contains a '
            'line break, which no well-formed ontology term does.',
            key, fact.subject.entityId
        )
        return None

    # Defence in depth. `OntologyConfig` has already refused an
    # ontology that could get here, but a fact can also arrive from a checkpoint
    # written under a different config, and this builder is the last place before
    # the write.
    if key in _reserved_property_names(typed_properties):
        logger.warning(
            'Skipping typed property %r on entity %r: the graph model already owns '
            'that property on __Entity__. Rename the property in the ontology.',
            key, fact.subject.entityId
        )
        return None

    # Refuse rather than store the raw string under a key whose
    # name promises a number.
    value = coerce_literal(complement.value, complement.datatype)
    if value is None:
        logger.debug(
            f'Skipping typed property [key: {key}, value: {complement.value!r}, '
            f'datatype: {complement.datatype}] - the literal does not coerce'
        )
        return None

    return (key, value)

def _typed_complement_values(entity:Entity, typed_properties:str):
    """Resolve the `typed_value` / `datatype` pair for a complement node, if any.

    The mirror of `_typed_subject_property`, and deliberately not
    folded into it: the two placements write different property names, to different
    nodes, under different conditions, and the only thing they share is
    `coerce_literal`. `'both'` runs both, each to its own node - it is not a
    fallback chain.

    No reserved-name check, because both names are fixed by the graph model rather
    than taken from the ontology. The collision runs the other way, and
    `_reserved_property_names` is where it is handled: an ontology declaring
    `:typed_value` cannot key a subject write once complement placement is on.

    Args:
        entity: The complement entity, after `string_complement_to_entity`.
        typed_properties: The resolved placement.

    Returns:
        A `(typed_value, datatype)` pair, or `None` when nothing should be written.
        `typed_value` may be `False`, `0` or `0.0`, so callers must test the pair.
    """
    if typed_properties not in COMPLEMENT_PLACEMENTS:
        return None

    if not isinstance(entity, Entity) or not entity.datatype:
        return None

    # The reason `datatype` is never written on its own: a
    # `datatype` without a `typed_value` would assert a type for a value that is
    # not there, which is worse than the absence of both. The string is still on
    # the node as `value`, where every existing consumer reads it.
    value = coerce_literal(entity.value, entity.datatype)
    if value is None:
        logger.debug(
            f'Skipping typed complement value [value: {entity.value!r}, '
            f'datatype: {entity.datatype}] - the literal does not coerce'
        )
        return None

    return (value, entity.datatype)

class EntityGraphBuilder(GraphBuilder):
    """
    Handles the process of building and interacting with a graph database for entity and fact data
    representation. Supports operations to insert and manage entities and their relationships in the
    graph structure. Provides mechanisms for integrating metadata into the graph storage system.

    This class is designed to work with a specific graph storage client and encapsulates the logic
    necessary for mapping entities and facts from node data to a graph database, considering the domain
    classification when required. It assumes an ontology structure with subject and object entities
    linked by facts.

    Attributes:
        DEFAULT_CLASSIFICATION (str): The default classification for entities when not explicitly provided.
    """
    @classmethod
    def index_key(cls) -> str:
        """
        Provides a method to retrieve the index key associated with the class.

        This method is a class-level function that returns the index
        key string associated with the class. It can be used to
        uniquely identify or categorize instances of the class in
        various contexts.

        Returns:
            str: The index key associated with the class.
        """
        return 'fact'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        Processes a given node and builds the corresponding entities in the graph database.

        This method extracts fact metadata from the provided node to construct nodes and
        relationships in a graph database using Cypher queries. It validates the fact
        metadata and leverages the graph_client to execute the queries. Properties for
        the subject and object are set or updated based on whether they already exist in
        the graph. Optionally, domain-specific labels can be included.

        The function handles missing fact metadata by logging a warning message. It also
        ensures reliability through query retries with controlled attempts and wait times.

        Args:
            node (BaseNode): The node from which fact metadata is to be extracted.
            graph_client (GraphStore): The graph database client to execute queries.
            **kwargs (Any): Additional options, such as `include_domain_labels`, which
                determines whether domain-specific labels are added to the entities,
                and `typed_properties`, which says where a coerced attribute value is
                stored and defaults to `'off'` when absent. At `'subject'` or
                `'both'`, a fact whose predicate resolved to a declared datatype
                property additionally gets that value written onto the subject
                `__Entity__` under the property's canonical name. At `'complement'`
                or `'both'`, the complement `__Entity__` - where one is created at
                all, which needs `include_local_entities` - additionally gets
                `typed_value` and `datatype`, keeping its string `value`. `'both'`
                does each of those to its own node; it is not a fallback chain.
        """
        fact_metadata = node.metadata.get('fact', {})
        include_domain_labels = kwargs['include_domain_labels']
        include_local_entities = kwargs['include_local_entities']
        # `.get` rather than a subscript, unlike the two above.
        # Those are always supplied by `BuildPipeline`, but a caller who
        # constructed a pipeline before this setting existed - or who calls a
        # builder directly, as several tests do - would otherwise get a `KeyError`
        # from a feature they never asked for.
        typed_properties = kwargs.get('typed_properties', TYPED_PROPERTIES_OFF)

        if fact_metadata:

            fact = Fact.model_validate(fact_metadata)
            fact = string_complement_to_entity(fact)

            _warn_if_no_annotations(fact, typed_properties)

            if fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION:
                if not include_local_entities:
                    logger.debug(f'Ignoring local entities for fact [fact_id: {fact.factId}]')
                    return
        
            logger.debug(f'Inserting entities for fact [fact_id: {fact.factId}]')

            def insert_for_entity(entity:Entity):

                statements = [
                    '// insert entities',
                    'UNWIND $params AS params',
                    f'MERGE (entity:`__Entity__`{{{graph_client.node_id("entityId")}: params.e_id}})',
                    'ON CREATE SET entity.value = params.v, entity.search_str = params.e_search_str, entity.class = params.ec',
                    'ON MATCH SET entity.value = params.v, entity.search_str = params.e_search_str, entity.class = params.ec'
                ]

                properties = {
                    'e_id': entity.entityId,
                    'v': entity.value,
                    'e_search_str': search_string_from(entity.value),
                    'ec': entity.classification or DEFAULT_CLASSIFICATION
                }

                query = '\n'.join(statements)
                
                graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

            def insert_typed_complement_values(entity:Entity):
                """Set `typed_value` and `datatype` on a complement `__Entity__`.

                A second query for the same reason the
                subject write is one: `insert_for_entity` stays byte-identical at
                every placement, so the complement's string `value` cannot be
                displaced by an edit here, and the recorded baseline keeps
                proving something.

                Called only from the branch that inserts the complement node, so
                the write cannot conjure a node that `include_local_entities`
                would not have created. Duplicating the
                condition instead would be one refactor away from a `MERGE` that
                creates local entities behind the setting's back.

                Both keys are fixed by the graph model, so neither needs escaping,
                but the assignments still go through `property_assigment_fn`
. Both are the identity on every store today -
                neither name matches `metadata_datetime_suffixes` - and routing
                through it anyway keeps the store's decision the store's to make.
                """
                typed_values = _typed_complement_values(entity, typed_properties)

                if not typed_values:
                    return

                (typed_value, datatype) = typed_values

                (value_key, datatype_key) = COMPLEMENT_ENTITY_PROPERTIES
                c_var = new_query_var()
                value_assigment = graph_client.property_assigment_fn(value_key, typed_value)('params.typedValue')
                datatype_assigment = graph_client.property_assigment_fn(datatype_key, datatype)('params.datatype')

                statements = [
                    '// insert typed complement values',
                    'UNWIND $params AS params',
                    f'MERGE ({c_var}:`__Entity__`{{{graph_client.node_id("entityId")}: params.entityId}})',
                    f'SET {c_var}.`{value_key}` = {value_assigment}, '
                    f'{c_var}.`{datatype_key}` = {datatype_assigment}'
                ]

                query = '\n'.join(statements)

                graph_client.execute_query_with_retry(
                    query,
                    self._to_params({
                        'entityId': entity.entityId,
                        'typedValue': typed_value,
                        'datatype': datatype
                    }),
                    max_attempts=5, max_wait=7
                )

            insert_for_entity(fact.subject)

            if fact.object and fact.object.entityId != fact.subject.entityId:
                insert_for_entity(fact.object)
            elif include_local_entities and fact.complement and fact.complement.entityId != fact.subject.entityId:
                insert_for_entity(fact.complement)
                insert_typed_complement_values(fact.complement)

            typed_subject_property = _typed_subject_property(fact, typed_properties)

            if typed_subject_property:

                def insert_typed_subject_property(entity:Entity, key:str, value:Any):
                    """Set one coerced attribute value on the subject `__Entity__`.

                    A second query rather than an extra `SET` on the insert above,
                    which costs a round trip and buys one thing
                    outright: the entity insert is byte-identical at every
                    placement, so `value`, `search_str` and `class` cannot be
                    disturbed by a change to this query, and there is no branch in
                    the insert that a future edit could get wrong. Same trade
                    `insert_domain_entity` below already makes; batching both into
                    the insert is a future optimization, not a correctness fix.

                    `key` is a Cypher identifier, so it
                    is backtick-quoted and escaped; `value` is bound, and the
                    assignment goes through the store's `property_assigment_fn` so
                    Neptune's `datetime(...)` wrapper still applies.
                    """
                    e_var = new_query_var()
                    e_key = escape_cypher_label(key)

                    # Bound under a fixed name rather than under the property's own
                    # name, which is where this deliberately departs from
                    # `source_graph_builder`. That builder's keys come from document
                    # metadata and it orders `sourceId` last to win a collision;
                    # here the key comes from an ontology, so a property canonically
                    # named `entityId` would otherwise collide with the merge key in
                    # the params dict. `property_assigment_fn` still receives the
                    # real key, so the store's name-based decisions are unchanged.
                    assigment = graph_client.property_assigment_fn(key, value)('params.typedValue')

                    statements = [
                        '// insert typed property',
                        'UNWIND $params AS params',
                        f'MERGE ({e_var}:`__Entity__`{{{graph_client.node_id("entityId")}: params.entityId}})',
                        f'SET {e_var}.`{e_key}` = {assigment}'
                    ]

                    query = '\n'.join(statements)

                    graph_client.execute_query_with_retry(
                        query,
                        self._to_params({'entityId': entity.entityId, 'typedValue': value}),
                        max_attempts=5, max_wait=7
                    )

                (typed_key, typed_value) = typed_subject_property
                insert_typed_subject_property(fact.subject, typed_key, typed_value)

            if include_domain_labels:

                def insert_domain_entity(entity:Entity):
                    """Add the entity's domain label to the `__Entity__` node.

                    `label_from` passes `__...__` values through unescaped, so the
                    label is escaped, the entity id is bound as a parameter (not
                    inlined), and newlines are stripped from the `//` comment so a
                    crafted label cannot terminate it and append further Cypher.
                    """
                    if entity.classification and entity.classification == LOCAL_ENTITY_CLASSIFICATION:
                        return

                    # new_query_var() makes each query string unique, so this
                    # UNWIND runs one row per call. Correct, but batching these
                    # into a single multi-row UNWIND is a future optimization.
                    e_var = new_query_var()
                    e_id = entity.entityId
                    e_label = escape_cypher_label(label_from(entity.classification or DEFAULT_CLASSIFICATION))
                    e_comment = f'// awsqid:{e_id}-{e_label}'.replace('\r', ' ').replace('\n', ' ')
                    query_e = f"UNWIND $params AS params MERGE ({e_var}:`__Entity__`{{{graph_client.node_id('entityId')}: params.entityId}}) SET {e_var} :`{e_label}` {e_comment}"
                    graph_client.execute_query_with_retry(query_e, self._to_params({'entityId': e_id}), max_attempts=5, max_wait=7)

                insert_domain_entity(fact.subject)

                if fact.object and fact.object.entityId != fact.subject.entityId:
                    insert_domain_entity(fact.object)

                if include_local_entities and fact.complement and fact.complement.entityId != fact.subject.entityId:
                    insert_domain_entity(fact.complement)
                    
        else:
            logger.warning(f'fact_id missing from fact node [node_id: {node.node_id}]')