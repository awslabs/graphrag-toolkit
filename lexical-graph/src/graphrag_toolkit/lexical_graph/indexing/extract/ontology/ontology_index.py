# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Plain-data read index over a parsed ontology.

`OntologyIndex` is the artefact that crosses the extraction process boundary:
extraction runs `ProcessPoolExecutor(mp_context='spawn')`, so the transform
components in the pipeline are pickled per node batch per worker. Every value
here is therefore a `str`, `List[str]`, `Dict[str, ...]` or `FrozenSet[str]` -
never an `rdflib` term, and this module does not import `rdflib` at all. The
`rdflib.Graph` stays behind in the parent process on `Ontology`.

`is_subclass_of` is a containment check against a precomputed reflexive
ancestor set rather than a graph traversal, because the ontology filter calls
it per fact per node. The `*_by_key` maps are precomputed for the same reason -
`resolve_*` is called per emitted name per fact.
"""

import logging

from typing import Dict, FrozenSet, List, Optional, Set, Union

from pydantic import BaseModel, ConfigDict, model_validator

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.naming import (
    compact_key,
    resolution_key,
)

logger = logging.getLogger(__name__)

XSD_NAMESPACE = 'http://www.w3.org/2001/XMLSchema#'

OWL_THING = 'http://www.w3.org/2002/07/owl#Thing'

class OntologyClass(BaseModel):
    """An `owl:Class` declared in the ontology.

    Attributes:
        iri (str): The full class IRI.
        local_name (str): The IRI's last segment, split on `#` or `/`.
        label (Optional[str]): The `rdfs:label` value, if declared. This is the
            canonical stored spelling when `normalize_names` is on, in
            preference to `local_name`.
        aliases (List[str]): `skos:altLabel` values, sorted.
        parents (List[str]): Direct `rdfs:subClassOf` class IRIs, sorted.
        ancestors (FrozenSet[str]): Reflexive transitive closure of `parents` -
            includes `iri` itself. What makes `is_subclass_of` O(1).
        description (Optional[str]): The `rdfs:comment` value, if declared.
    """
    model_config = ConfigDict(frozen=True, strict=True)

    iri:str
    local_name:str
    label:Optional[str]=None
    aliases:List[str]=[]
    parents:List[str]=[]
    ancestors:FrozenSet[str]=frozenset()
    description:Optional[str]=None

class ObjectProperty(BaseModel):
    """An `owl:ObjectProperty` - an entity-to-entity predicate.

    Attributes:
        iri (str): The full property IRI.
        local_name (str): The IRI's last segment.
        label (Optional[str]): The `rdfs:label` value, if declared.
        aliases (List[str]): `skos:altLabel` values, sorted.
        domain (Optional[str]): The `rdfs:domain` class IRI. `None` means
            `owl:Thing` - the property matches any subject class.
        range (Optional[str]): The `rdfs:range` class IRI. `None` means
            `owl:Thing`.
        description (Optional[str]): The `rdfs:comment` value, if declared.
    """
    model_config = ConfigDict(frozen=True, strict=True)

    iri:str
    local_name:str
    label:Optional[str]=None
    aliases:List[str]=[]
    domain:Optional[str]=None
    range:Optional[str]=None
    description:Optional[str]=None

class DatatypeProperty(BaseModel):
    """An `owl:DatatypeProperty` - an entity-to-literal predicate.

    Attributes:
        iri (str): The full property IRI.
        local_name (str): The IRI's last segment.
        label (Optional[str]): The `rdfs:label` value, if declared.
        aliases (List[str]): `skos:altLabel` values, sorted.
        domain (Optional[str]): The `rdfs:domain` class IRI. `None` means
            `owl:Thing`.
        datatype (str): The `rdfs:range` XSD IRI, as a plain string. Always
            populated - a datatype property without an XSD range is rejected
            at load time.
        description (Optional[str]): The `rdfs:comment` value, if declared.
    """
    model_config = ConfigDict(frozen=True, strict=True)

    iri:str
    local_name:str
    label:Optional[str]=None
    aliases:List[str]=[]
    domain:Optional[str]=None
    datatype:str
    description:Optional[str]=None

OntologyTerm = Union[OntologyClass, ObjectProperty, DatatypeProperty]

# Resolution precedence. A term's own local name outranks its rdfs:label, which
# outranks any skos:altLabel, so that a name folding to the same key as another
# term's alias still resolves to the term that owns it outright. Ties within a
# rank break on the IRI, which makes resolution a function of the ontology's
# content and not of dict iteration order.
_LOCAL_NAME_RANK = 0
_LABEL_RANK = 1
_ALIAS_RANK = 2

def _ranked_keys(term:OntologyTerm) -> List[tuple]:
    """Return `(resolution_key, rank)` for a term's local name, label, aliases.

    Empty keys are dropped rather than indexed - a term whose alias is a blank
    string should not answer a lookup for the empty name.
    """
    ranked = [(resolution_key(term.local_name), _LOCAL_NAME_RANK)]
    if term.label:
        ranked.append((resolution_key(term.label), _LABEL_RANK))
    for alias in term.aliases:
        ranked.append((resolution_key(alias), _ALIAS_RANK))
    return [(key, rank) for (key, rank) in ranked if key]

def _rank_by_key(terms:Dict[str, OntologyTerm]) -> Dict[str, Dict[str, int]]:
    """Group terms by resolution key, keeping each term's best rank."""
    ranked:Dict[str, Dict[str, int]] = {}
    for term in terms.values():
        for (key, rank) in _ranked_keys(term):
            by_iri = ranked.setdefault(key, {})
            if rank < by_iri.get(term.iri, _ALIAS_RANK + 1):
                by_iri[term.iri] = rank
    return ranked

def _single_valued_key_index(terms:Dict[str, OntologyTerm]) -> Dict[str, str]:
    """Map each resolution key to the one term that wins it."""
    ranked = _rank_by_key(terms)
    _warn_on_key_collisions(ranked)
    return {
        key: min(by_iri, key=lambda iri: (by_iri[iri], iri))
        for (key, by_iri) in ranked.items()
    }

def _warn_on_key_collisions(ranked:Dict[str, Dict[str, int]]) -> None:
    """Warn when two terms of the same kind claim one resolution key.

    The tie is still broken deterministically - by rank, then by IRI - so nothing
    here changes what resolves. What it changes is the silence. A collision has
    no correct resolution: whichever term loses becomes unreachable by *every*
    name it has, while the renderers still put that name in the prompt, so at
    `strict` the model complies and its facts are dropped and attributed to
    `enforce_domain_range` - pointing at a declaration that is correct.

    Two shapes reach here, and the second is the one that bites. Two vocabularies
    merged into one file (`schema:Person` beside `foaf:Person`) is obvious once
    seen. A class whose `rdfs:label` is another class's local name
    (`:Company rdfs:label "Organisation"` beside `:Organisation`) is not: the
    label loses to the other class's own local name, so the compliant emission
    resolves to the *other* class, and since `create_entity_id` hashes the
    classification the two collapse onto one node id carrying a conflicting
    `classIri`.

    Logged rather than raised: two terms sharing a key is legal RDF, and an
    ontology may be authored this way knowingly. Nothing downstream is unsafe -
    it is only unlikely to be what the author meant.
    """
    for (key, by_iri) in sorted(ranked.items()):
        if len(by_iri) < 2:
            continue
        logger.warning(
            'Ontology terms %s all resolve under the name %r; only %s will be '
            'found by it. Rename or relabel the others - a term that loses a '
            'name collision is unreachable by every name it has, while the '
            'prompt still offers that name to the model.',
            ', '.join(sorted(by_iri)), key,
            min(by_iri, key=lambda iri: (by_iri[iri], iri)),
        )

def _unambiguous_compact_index(terms:Dict[str, OntologyTerm]) -> Dict[str, str]:
    """Map each *unambiguous* compact key to the one class that owns it.

    The fallback `resolve_class` consults when an exact lookup misses. It exists
    because the response parser destroys a word boundary that `resolution_key`
    depends on: `format_classification` applies `.title()`, so a model that writes
    a class's own name - `SportsTeam` - hands back `Sportsteam`, whose resolution
    key is `'sportsteam'` against an index holding `'sports team'`.

    That matters because the ontology's spelling is the authority: a user who
    declares `:SportsTeam` should be able to have the model write `SportsTeam`.
    Properties need no such fallback - `format_value` only maps `_` to a space, so
    `worksFor` survives it and `resolution_key` splits the camel boundary at both
    ends.

    A compact key claimed by more than one class is **omitted** rather than
    resolved to a winner. `ABCorp` and `AB Corp` are distinct classes with
    distinct resolution keys, and guessing between them on a coarse match would
    lose the exactness the rest of the index guarantees. Omitting means such a
    name simply does not resolve, which is the behaviour before this fallback
    existed.
    """
    owners:Dict[str, Set[str]] = {}
    for term in terms.values():
        for (key, _) in _ranked_keys(term):
            compact = compact_key(key)
            if compact:
                owners.setdefault(compact, set()).add(term.iri)

    return {
        compact: next(iter(iris))
        for (compact, iris) in owners.items()
        if len(iris) == 1
    }

def _multi_valued_key_index(terms:Dict[str, OntologyTerm]) -> Dict[str, List[str]]:
    """Map each resolution key to every term that claims it, best first."""
    ranked = _rank_by_key(terms)
    _warn_on_key_collisions(ranked)
    return {
        key: sorted(by_iri, key=lambda iri: (by_iri[iri], iri))
        for (key, by_iri) in ranked.items()
    }

class OntologyIndex(BaseModel):
    """The read side of an ontology, keyed by IRI.

    A pure function of the `rdflib.Graph` it was built from. Constructed once
    at pipeline-configuration time by `Ontology.index()` and read-only
    thereafter.

    Attributes:
        classes (Dict[str, OntologyClass]): Declared classes by IRI.
        object_properties (Dict[str, ObjectProperty]): Declared object
            properties by IRI.
        datatype_properties (Dict[str, DatatypeProperty]): Declared datatype
            properties by IRI.
        class_by_key (Dict[str, str]): Class IRI by `resolution_key`. Derived -
            recomputed on construction, so a value passed in is discarded.
        class_by_compact_key (Dict[str, str]): Class IRI by `compact_key`, for
            the unambiguous keys only. Derived. The fallback `resolve_class`
            consults when an exact lookup misses, so that a class's own spelling
            resolves after the response parser has title-cased it.
        obj_property_by_key (Dict[str, List[str]]): Object property IRIs by
            `resolution_key`, in resolution precedence order. Derived.
            Multi-valued because a key collision between two declared
            properties is an authoring choice to report on, not a reason to
            lose one of them.
        dt_property_by_key (Dict[str, List[str]]): Datatype property IRIs by
            `resolution_key`, in resolution precedence order. Derived.
    """
    model_config = ConfigDict(frozen=True, strict=True)

    classes:Dict[str, OntologyClass]={}
    object_properties:Dict[str, ObjectProperty]={}
    datatype_properties:Dict[str, DatatypeProperty]={}
    class_by_key:Dict[str, str]={}
    class_by_compact_key:Dict[str, str]={}
    obj_property_by_key:Dict[str, List[str]]={}
    dt_property_by_key:Dict[str, List[str]]={}

    @model_validator(mode='after')
    def _build_key_indexes(self) -> 'OntologyIndex':
        """Derive the three key maps from the terms.

        Done here rather than in `Ontology._build_index` so that an index built
        by any route - constructed directly in a test, unpickled in a worker,
        round-tripped through `model_dump` - cannot carry keys that disagree
        with its terms. `object.__setattr__` because the model is frozen.
        """
        object.__setattr__(self, 'class_by_key', _single_valued_key_index(self.classes))
        object.__setattr__(self, 'class_by_compact_key', _unambiguous_compact_index(self.classes))
        object.__setattr__(self, 'obj_property_by_key', _multi_valued_key_index(self.object_properties))
        object.__setattr__(self, 'dt_property_by_key', _multi_valued_key_index(self.datatype_properties))
        return self

    def resolve_class(self, name:str) -> Optional[OntologyClass]:
        """Resolve an emitted classification to a declared class.

        Exact match on `resolution_key` first, then - only on a miss - the
        separator-free `class_by_compact_key`. The fallback is what lets a class's
        own spelling work: the response parser title-cases a classification, so
        `SportsTeam` comes back as `Sportsteam` and its resolution key no longer
        matches the indexed `'sports team'`. See `_unambiguous_compact_index` for
        why an ambiguous coarse match resolves to nothing rather than to a winner.

        Args:
            name: A name in any convention - `'Sports Team'` as the parser
                hands it over, `'SPORTS_TEAM'`, `'SportsTeam'`, `'Sportsteam'`, or
                a declared label or alias.

        Returns:
            The declared class, or `None` if nothing in the ontology carries
            that name.
        """
        iri = self.class_by_key.get(resolution_key(name))
        if iri is None:
            iri = self.class_by_compact_key.get(compact_key(name))
        return self.classes.get(iri) if iri else None

    def resolve_object_predicate(self, name:str) -> Optional[ObjectProperty]:
        """Resolve an emitted predicate to a declared object property.

        Args:
            name: A name in any convention - `'WORKS FOR'` as the parser hands
                it over, `'WORKS_FOR'`, `'works for'`, `'worksFor'`, or a
                declared label or alias.

        Returns:
            The highest-precedence declared object property carrying that name,
            or `None`.
        """
        return self._first(self.obj_property_by_key, self.object_properties, name)

    def resolve_datatype_predicate(self, name:str) -> Optional[DatatypeProperty]:
        """Resolve an emitted attribute name to a declared datatype property.

        Args:
            name: A name in any convention, as for `resolve_object_predicate`.

        Returns:
            The highest-precedence declared datatype property carrying that
            name, or `None`.
        """
        return self._first(self.dt_property_by_key, self.datatype_properties, name)

    @staticmethod
    def _first(by_key:Dict[str, List[str]], terms:Dict[str, OntologyTerm], name:str) -> Optional[OntologyTerm]:
        """Return the first term claiming `name`'s resolution key."""
        iris = by_key.get(resolution_key(name))
        return terms.get(iris[0]) if iris else None

    def is_subclass_of(self, child_iri:str, parent_iri:str) -> bool:
        """Return True if `parent_iri` is an ancestor of `child_iri`.

        Reflexive: `is_subclass_of(c, c)` is True for any declared class `c`.
        An IRI that is not a declared class returns False rather than raising,
        so callers can pass an unresolved classification straight through.
        """
        ontology_class = self.classes.get(child_iri)
        if ontology_class is None:
            return False
        return parent_iri in ontology_class.ancestors
