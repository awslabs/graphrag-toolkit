# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""How much authority the ontology has, expressed as one level.

`ontology_authority` is the single knob. It reads as inclusion authority - how much say
the ontology has over what ends up in the graph - and it resolves to the six
dimensions the deterministic layer acts on:

| | `off` | `align` | `strict` |
|---|---|---|---|
| `normalize_names` | False | True | True |
| `drop_type_restatements` | False | True | True |
| `enforce_entity_types` | False | False | True |
| `enforce_relationship_types` | False | False | True |
| `enforce_domain_range` | False | False | True |
| `enforce_datatypes` | False | False | True |

Three levels, and each one owns a distinct mechanism: `off` says nothing, `align`
names what the ontology declares, `strict` also decides what survives.

`drop_type_restatements` is the one row that does not fit that summary, and the
exception is deliberate. It is a dropping gate that is on at `align`, which reads
as a contradiction of "keep what the ontology does not declare" - so it is worth
being precise about what it removes: facts whose predicate is ontology language
(`rdf:type`, `rdfs:subClassOf`), and facts that restate the subject's own
classification under a type-asserting predicate (`Meridian Freight [Company]
|TYPE| Company`). Neither is content the level promised to keep. The second is a
duplicate of the entity's own label line, and dropping it is information-
preserving in the strict sense that nothing in the graph changes except the
absence of a redundant edge. `align` still keeps every fact that says something
the ontology did not anticipate; see `OntologyFilter._carries_no_domain_fact`.

There was a fourth level between `align` and `strict`, `guide`, whose only
mechanism was extra prompt wording - flag-identical to `align`, so nothing in
this table could distinguish it. It was measured against a same-configuration
control and removed.

`strict` was deliberately absent from `OntologyAuthority` until the enforcement
existed, because accepting it before then would have let a user ask for
exclusion and quietly get `align`; it lands here together with `OntologyFilter`'s
four conformance gates, which is what makes its prompt claim true.

Each dimension is also settable on its own, as `True`, `False`, or `None`
meaning "follow the level". That is what makes "say nothing in the prompt but
still coerce datatypes" - `ontology_authority='off', enforce_datatypes=True` -
expressible, and it is accepted rather than treated as a contradiction: the
level chooses defaults, it does not constrain overrides.

`typed_properties` is the second knob, and a different kind of one: `ontology_authority`
governs what reaches the builders, `typed_properties` governs what the builders
then *write*. It is deliberately not folded into the level, because a user who
asks for `align` is asking about naming and has not thereby asked for new
properties on their nodes. It defaults to `'off'` at every layer.

`OntologyConfig` lives in the parent process. It holds an `Ontology`, which owns
the `rdflib.Graph`, so it is configuration rather than something that crosses
the spawn boundary; what crosses is the rendered constraint string and
`OntologyIndex`.
"""

import logging
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Literal, Optional, Tuple, Union

from rdflib import Graph

from graphrag_toolkit.lexical_graph.indexing.constants import (
    COMPLEMENT_ENTITY_PROPERTIES,
    COMPLEMENT_PLACEMENTS,
    RESERVED_ENTITY_PROPERTIES,
    SUBJECT_PLACEMENTS,
    TYPED_PROPERTIES_OFF,
    TYPED_PROPERTY_PLACEMENTS,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology import Ontology
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.prompt_constraint import (
    PROMPT_CONSTRAINT_LEVELS,
    PROSE_VOCABULARY,
    VOCABULARY_FORMATS,
)

logger = logging.getLogger(__name__)

OntologyAuthority = Literal['off', 'align', 'strict']

# Where a coerced attribute value is stored, if anywhere.
#
# `'subject'` keys the value from the property's own name on the subject entity,
# which is what makes `WHERE c.foundedYear < 2000` possible. `'complement'`
# writes `typed_value` / `datatype` on the complement node instead, which keeps
# the value where the string already is and needs `include_local_entities`.
# `'both'` does each of those things to its own node; it is not a fallback chain.
#
# The accepted values are `TYPED_PROPERTY_PLACEMENTS`, in `indexing.constants`
# alongside the placement membership tuples, so that the build pipeline can read
# them without importing rdflib. A test asserts the two agree.
TypedProperties = Literal['off', 'subject', 'complement', 'both']

# The same set the renderer knows, deliberately not a second list. A level this
# module accepted but `prompt_constraint.py` did not would pass configuration
# and then raise from inside pipeline setup.
ONTOLOGY_AUTHORITY_LEVELS:Tuple[str, ...] = PROMPT_CONSTRAINT_LEVELS

# How the vocabulary is written down for the model, orthogonal to how much
# authority it has. Re-exported from the renderer for the same reason
# `ONTOLOGY_AUTHORITY_LEVELS` is.
VocabularyFormat = Literal['prose', 'turtle']

# Anything `Ontology.load` accepts.
OntologySource = Union[Ontology, Graph, str, Path]

# Anything `ExtractionConfig(ontology=...)` accepts.
OntologyType = Union['OntologyConfig', OntologySource]

@dataclass(frozen=True)
class ResolvedDimensions:
    """The six dimensions after the level defaults and any overrides.

    Attributes:
        normalize_names (bool): Rewrite a resolved name to the ontology's own
            spelling.
        drop_type_restatements (bool): Drop a fact that carries ontology
            language rather than a domain statement - an `rdf:type`-family
            predicate, or a type-asserting predicate whose value repeats the
            subject's own classification.
        enforce_entity_types (bool): Drop a fact whose entity classification
            does not resolve.
        enforce_relationship_types (bool): Drop a fact whose predicate does not
            resolve.
        enforce_domain_range (bool): Drop a fact whose subject or object class
            does not satisfy the declared domain or range.
        enforce_datatypes (bool): Drop a fact whose literal does not parse as
            the declared XSD datatype.
    """
    normalize_names:bool = False
    drop_type_restatements:bool = False
    enforce_entity_types:bool = False
    enforce_relationship_types:bool = False
    enforce_domain_range:bool = False
    enforce_datatypes:bool = False

    def any_enabled(self) -> bool:
        """Return True if any dimension is on."""
        return any(getattr(self, dimension) for dimension in DIMENSIONS)

# Derived from the dataclass rather than restated, so the defaults table below
# and the override loop cannot fall out of step with the fields themselves.
DIMENSIONS:Tuple[str, ...] = tuple(field.name for field in fields(ResolvedDimensions))

# `off` and the all-False default are the same thing, which is
# why `off` maps to a bare `ResolvedDimensions()`.
#
# `strict` turns everything on, and it is the only level that does: the four
# `enforce_*` dimensions are what distinguishes authority over *naming* from
# authority over *membership*, and nothing between `align` and `strict` is a
# level. That last clause is now a measured position rather than a preference -
# `guide` was exactly such an in-between level, distinguishable only by prompt
# wording, and the wording moved extraction in opposite directions on different
# models. A user who wants one gate without the rest asks for it by name, which
# is a request the code can honour precisely.
_LEVEL_DEFAULTS = {
    'off': ResolvedDimensions(),
    'align': ResolvedDimensions(normalize_names=True, drop_type_restatements=True),
    'strict': ResolvedDimensions(
        normalize_names=True,
        drop_type_restatements=True,
        enforce_entity_types=True,
        enforce_relationship_types=True,
        enforce_domain_range=True,
        enforce_datatypes=True,
    ),
}

@dataclass
class OntologyConfig:
    """An ontology and the authority it has over extraction.

    Attributes:
        ontology (Ontology): The loaded ontology. Any source `Ontology.load`
            accepts may be passed in and is normalized to an `Ontology` here,
            so `OntologyConfig('company.ttl')` is valid.
        ontology_authority (OntologyAuthority): `'off'`, `'align'` or `'strict'`.
            Defaults to `'align'` - name what the ontology declares, keep what it
            does not. `'strict'` additionally discards what does not conform, and
            on a corpus wider than the ontology that is a large share of it.
        normalize_names (Optional[bool]): Override; `None` follows the level.
        drop_type_restatements (Optional[bool]): Override; `None` follows the
            level. On at `align` and `strict`. Set False to keep facts like
            `Meridian Freight|TYPE|Company` that restate an entity's own
            classification, which the `turtle` vocabulary format provokes and
            the prose rendering does not.
        enforce_entity_types (Optional[bool]): Override; `None` follows the
            level.
        enforce_relationship_types (Optional[bool]): As above.
        enforce_domain_range (Optional[bool]): As above.
        enforce_datatypes (Optional[bool]): As above.
        report_violations (bool): Log per-dimension counts of what the filter
            rewrote and dropped, at INFO, per node batch rather than per fact.
            A record of the filter's actions, not a measure of how far the
            corpus diverges from the ontology: at `align` the drop counts are
            zero because no gate runs, and at `strict` they are low because the
            prompt already asked the model to omit non-conforming facts, so what
            reaches the gates is the residue that ignored the instruction. To
            count what `strict` would exclude, ask for `align`'s prompt with the
            `enforce_*` gates overridden on.
        vocabulary_format (VocabularyFormat): How the vocabulary is written down
            for the model. `'prose'` (the default) renders the three generated
            sections; `'turtle'` shows the ontology's own source instead. Affects
            only the topics vocabulary block - the propositions block stays a
            class-name list either way, because that stage extracts nothing a
            property declaration could steer. Orthogonal to `ontology_authority`, which
            still chooses how much authority the vocabulary has. `'turtle'` is
            experimental: it gives up the object/datatype channel separation of
            the prose sections and their plain-language ranges, in
            exchange for stating the RDFS entailments the prose sections
            paraphrase away. On the corpus it was measured against, it was a small
            net loss; see the Ontology-Guided Extraction documentation.
        typed_properties (TypedProperties): Where a coerced attribute value is
            stored: `'off'` (the default) stores nothing and leaves the graph
            byte-identical to a build without an ontology, `'subject'` keys it
            from the property's own name on the subject entity, `'complement'`
            writes `typed_value` / `datatype` on the complement node, `'both'`
            does each. Anything other than `'off'` needs the annotations the
            filter adds, which is why it makes `filter_required()` True on its
            own.
    """
    ontology:OntologySource
    ontology_authority:OntologyAuthority = 'align'
    normalize_names:Optional[bool] = None
    drop_type_restatements:Optional[bool] = None
    enforce_entity_types:Optional[bool] = None
    enforce_relationship_types:Optional[bool] = None
    enforce_domain_range:Optional[bool] = None
    enforce_datatypes:Optional[bool] = None
    report_violations:bool = False
    typed_properties:TypedProperties = 'off'
    vocabulary_format:VocabularyFormat = PROSE_VOCABULARY

    def __post_init__(self) -> None:
        """Validate the settings and normalize `ontology` to an `Ontology`.

        Both validations run before the ontology is loaded, so a typo in a
        setting is reported without first paying to parse a Turtle file - except
        the reserved-name check, which by definition needs the loaded ontology.

        Raises:
            ValueError: If `ontology_authority` is not a supported level, if
                `typed_properties` is not a supported placement, if
                `vocabulary_format` is not a supported format, or if a
                declared datatype property would key a reserved `__Entity__`
                property under the requested placement.
            OntologyLoadError: If `ontology` cannot be loaded.
        """
        if self.ontology_authority not in ONTOLOGY_AUTHORITY_LEVELS:
            raise ValueError(
                f'Unknown ontology authority level: {self.ontology_authority!r}. '
                f'Expected one of {", ".join(ONTOLOGY_AUTHORITY_LEVELS)}.'
            )

        if self.typed_properties not in TYPED_PROPERTY_PLACEMENTS:
            raise ValueError(
                f'Unknown typed_properties placement: {self.typed_properties!r}. '
                f'Expected one of {", ".join(TYPED_PROPERTY_PLACEMENTS)}.'
            )

        if self.vocabulary_format not in VOCABULARY_FORMATS:
            raise ValueError(
                f'Unknown ontology vocabulary_format: {self.vocabulary_format!r}. '
                f'Expected one of {", ".join(VOCABULARY_FORMATS)}.'
            )

        self.ontology = Ontology.load(self.ontology)

        self._validate_no_reserved_property_names()

    def _validate_no_reserved_property_names(self) -> None:
        """Refuse an ontology whose vocabulary would overwrite the graph model.

        `__Entity__` already owns `value`, `search_str` and
        `class`, and complement placement additionally owns `typed_value` and
        `datatype`. A declared datatype property named `value` would, under
        subject placement, be written with the same key as the entity's own
        identity string.

        Raised here rather than left to the builder because this is a
        configuration mistake and the builder runs in a spawn worker, one fact at
        a time, where the only available response is a warning nobody reads. The
        builder skips the write as well - defence in depth, not the primary
        check.

        The check is scoped to the placement actually requested, so an ontology
        that is fine for `'off'` is not rejected for a write it will never
        perform. Only subject placement keys a property from the ontology's own
        vocabulary, so `'complement'` alone has nothing to collide: it widens the
        reserved set only when subject placement is active too, which is `'both'`.
        """
        if self.typed_properties not in SUBJECT_PLACEMENTS:
            return

        reserved = set(RESERVED_ENTITY_PROPERTIES)
        if self.typed_properties in COMPLEMENT_PLACEMENTS:
            reserved.update(COMPLEMENT_ENTITY_PROPERTIES)

        collisions = sorted(
            declared.local_name
            for declared in self.ontology.index().datatype_properties.values()
            if declared.local_name in reserved
        )

        if collisions:
            raise ValueError(
                f'Ontology declares datatype propert{"ies" if len(collisions) > 1 else "y"} '
                f'{", ".join(repr(name) for name in collisions)}, which '
                f'{"collide" if len(collisions) > 1 else "collides"} with a property the '
                f'graph model already owns on __Entity__ '
                f'({", ".join(sorted(reserved))}). Writing '
                f'{"them" if len(collisions) > 1 else "it"} under '
                f'typed_properties={self.typed_properties!r} would overwrite the entity '
                'itself. Rename the property in the ontology, or set '
                "typed_properties='off'."
            )

    def resolved(self) -> ResolvedDimensions:
        """Resolve the six dimensions: level defaults, then explicit overrides.

        Recomputed on each call rather than cached in `__post_init__`, because
        this is a plain mutable dataclass and a caller who reassigns
        `ontology_authority` should not be reading a stale answer.
        """
        defaults = _LEVEL_DEFAULTS[self.ontology_authority]
        overrides = {}
        for dimension in DIMENSIONS:
            override = getattr(self, dimension)
            overrides[dimension] = (
                getattr(defaults, dimension) if override is None else bool(override)
            )
        return ResolvedDimensions(**overrides)

    def writes_subject_properties(self) -> bool:
        """Return True if a coerced value is keyed onto the subject entity."""
        return self.typed_properties in SUBJECT_PLACEMENTS

    def writes_complement_properties(self) -> bool:
        """Return True if `typed_value` / `datatype` go on the complement node."""
        return self.typed_properties in COMPLEMENT_PLACEMENTS

    def filter_required(self) -> bool:
        """Return True if the ontology filter transform has work to do.

        When every dimension resolves to False there is nothing
        for the filter to normalize, enforce or annotate, and it is left out of
        the pipeline rather than added as a no-op.

        `typed_properties` is part of this test because the annotations the
        builders read - `Relation.canonicalName` and `Entity.datatype` - are
        written by the filter and by nothing else. So
        `ontology_authority='off', typed_properties='subject'` is a coherent request that
        needs the filter present purely to annotate, and dropping it there would
        produce a build that silently writes no typed properties at all.
        """
        return self.resolved().any_enabled() or self.typed_properties != TYPED_PROPERTIES_OFF

def to_ontology_config(source:OntologyType) -> OntologyConfig:
    """Normalize whatever the user configured into an `OntologyConfig`.

    The single entry point for `ExtractionConfig.ontology`, so that every
    accepted form converges on one object before anything reads it.

    Args:
        source: An `OntologyConfig`, an `Ontology`, an `rdflib.Graph`, or a path
            to a `.ttl` file.

    Returns:
        `source` itself when it is already an `OntologyConfig` - settings a user
        made are never rebuilt at defaults - otherwise a new `OntologyConfig`
        wrapping the loaded ontology at the default authority level.

    Raises:
        OntologyLoadError: If `source` is not a form that can be loaded.
    """
    if isinstance(source, OntologyConfig):
        return source
    return OntologyConfig(source)
