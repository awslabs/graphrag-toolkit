# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Render an ontology as the constraint block that goes into the prompt.

Three labelled sections, mapped onto the two output channels
`EXTRACT_TOPICS_PROMPT` already defines plus its entity classification:

| Section        | Prompt channel                  | Fed by                |
|----------------|---------------------------------|-----------------------|
| Entity types   | `entity\\|label`                 | `owl:Class`           |
| Relationships  | `entity\\|RELATIONSHIP\\|entity`  | `owl:ObjectProperty`  |
| Attributes     | `entity\\|ATTRIBUTE_NAME\\|value` | `owl:DatatypeProperty`|

The two property sections are separate and separately labelled on purpose.
Merged into one list they invite the model to emit an attribute where a
relationship belongs, and the attribute path depends on the distinction
surviving all the way to the response: an emitted attribute name that resolves
to a declared `owl:DatatypeProperty` is what yields a canonical name and a
declared datatype, and nothing downstream of that fires without it.

Datatype ranges are named in terms a model can act on - `integer`, `decimal
number`, `date`, `true/false`, `text` - never as XSD IRIs. The range is rendered
to help the model pick the right *attribute* and report the value in a sensible
form. It is not an instruction to convert or validate anything: value handling
is deterministic and happens after extraction.

Rendering reads only `OntologyIndex`, so it is plain-data in and a string out,
with no `rdflib` involved. `Ontology.format_as_prompt_constraint` is the public
entry point.

Determinism: every iteration is over a `sorted(...)` by
`local_name`, so the output is a pure function of the ontology's content and not
of triple insertion order, and two calls at the same level are byte-identical.
"""

from typing import Dict, List, Optional

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.naming import (
    camel_to_upper_snake,
    resolution_key,
    title_case_with_spaces,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_index import (
    XSD_NAMESPACE,
    DatatypeProperty,
    OntologyClass,
    OntologyIndex,
)

# The levels this renderer knows, weakest first. The order is the ladder of
# prompt pressure, and each closing below adds to the one before it rather than
# replacing its reasoning.
#
# There was a fourth, `guide`, between `align` and `strict`, whose entire
# mechanism was one extra sentence of preference wording. It was measured against
# a same-configuration control on three models and removed: the sentence had a
# real effect with no consistent direction, raising resolution on one model and
# lowering it 11-16 points on another, so no user could pick it in advance. The
# recordings that showed it are in git history.
PROMPT_CONSTRAINT_LEVELS = ('off', 'align', 'strict')

# How the vocabulary itself is presented, independent of the level. `'prose'` is
# the three generated sections above; `'turtle'` shows the ontology's own source
# instead and keeps only the protocol section.
#
# The point of `'turtle'` is that the prose sections are a *paraphrase*:
# `rdfs:subClassOf` becomes indentation, which carries no formal meaning, so the
# entailment that a subclass may carry its parent's properties is nowhere stated.
# Turtle carries it inherently. Whether that is worth the two things it costs -
# the object/datatype channel separation, and the plain-language ranges - is a
# measurement, not a preference, and it measured slightly worse overall.
VOCABULARY_FORMATS = ('prose', 'turtle')

PROSE_VOCABULARY, TURTLE_VOCABULARY = VOCABULARY_FORMATS

# What a domain or range of owl:Thing - or none at all - is called in the
# prompt. 'owl:Thing' is vocabulary for an ontologist, not for a model being
# asked to read text.
ANY_ENTITY = 'anything'

ANY_SUBJECT_GROUP = 'any entity'

# XSD range -> the words the model sees. Deliberately plain: the model is being
# helped to pick the right attribute and to report the value in a sensible form,
# not asked to perform a conversion.
_TYPE_NAMES = {
    'integer': 'integer',
    'int': 'integer',
    'long': 'integer',
    'short': 'integer',
    'byte': 'integer',
    'nonNegativeInteger': 'integer',
    'nonPositiveInteger': 'integer',
    'positiveInteger': 'integer',
    'negativeInteger': 'integer',
    'unsignedInt': 'integer',
    'unsignedLong': 'integer',
    'unsignedShort': 'integer',
    'unsignedByte': 'integer',
    'gYear': 'integer',
    'decimal': 'decimal number',
    'double': 'decimal number',
    'float': 'decimal number',
    'boolean': 'true/false',
    'date': 'date',
    'dateTime': 'date and time',
    'time': 'time of day',
}

DEFAULT_TYPE_NAME = 'text'

_HEADER = """\
# Vocabulary for this extraction

The entity types, relationships and attributes below are the vocabulary for this
extraction. Read the guidance at the end of this section before using them."""

# The `'turtle'` counterpart. It has to do two jobs the prose header does not.
#
# It has to say which OWL construct feeds which output channel, because Turtle
# interleaves `owl:ObjectProperty` and `owl:DatatypeProperty` in whatever order
# the author wrote them - the prose rendering separates them into two labelled
# sections precisely so the model cannot emit an attribute where a relationship
# belongs, and that separation is what this format gives up.
#
# And it has to say that the declared ranges are not conversion instructions,
# because `xsd:integer` reads far more like one than the word `integer` does, and
# value handling here is deterministic and post-hoc.
#
# Both sentences are restatements of what the prose *layout* conveys silently. If
# this arm wins anyway, that is the interesting result; if it loses, these are the
# first two things to suspect.
_TURTLE_HEADER = """\
# Vocabulary for this extraction

The vocabulary for this extraction is the OWL/RDFS ontology below, given in
Turtle. Read the guidance at the end of this section before using it.

How to read it for this task:

  - An `owl:Class` is an entity type. `rdfs:subClassOf` means the subject type is
    a kind of the object type, with everything that follows from that: a type may
    be used wherever any of its ancestors may be used, and it carries every
    property declared on any of its ancestors. Use the most specific type the
    text actually supports.
  - An `owl:ObjectProperty` is a relationship between two entities. Emit these on
    the relationship channel, as `entity|RELATIONSHIP|entity`.
  - An `owl:DatatypeProperty` is an attribute of one entity holding a literal
    value. Emit these on the attribute channel, as `entity|ATTRIBUTE_NAME|value`.
    Do not confuse the two: an attribute emitted where a relationship belongs is
    discarded.
  - `rdfs:domain` and `rdfs:range` say which entity types a property may relate,
    honouring `rdfs:subClassOf`.
  - An `rdfs:range` naming an XSD datatype describes the kind of value the
    attribute holds. It is there to help you pick the right attribute and report
    the value in a sensible form. **It is not an instruction to convert, reformat
    or validate anything** - report the value as the text states it.
  - `rdfs:label` and `skos:altLabel` are the names this vocabulary is known by;
    `rdfs:comment` describes what a term means.

When you write a name from this ontology, write the local name - the part after
the `:` - and not the full IRI or the prefix."""

def format_turtle_vocabulary(turtle:str, level:str) -> str:
    """Wrap an ontology's Turtle source as a prompt constraint block.

    The Turtle stands in for the three generated sections; the protocol section
    is the same text the prose format uses, because how to *use* the vocabulary
    is not a function of how the vocabulary was written down, and holding it
    constant is what makes the two formats comparable.

    Serialization lives with the caller, in `ontology.py`: this module renders
    from `OntologyIndex` with no `rdflib` involved, and an import-graph test
    holds it to that.

    Args:
        turtle: The ontology serialized as Turtle.
        level: `'align'` or `'strict'`. `'off'` never reaches here.

    Returns:
        The block, with no trailing newline, or `''` for empty Turtle.
    """
    if not turtle.strip():
        return ''

    return '\n\n'.join([
        _TURTLE_HEADER,
        f'```turtle\n{turtle.strip()}\n```',
        f'{_PROTOCOL_HEADING}\n\n{_MAP_BY_MEANING}\n\n{_THEN_SPELLING}\n\n{_CLOSINGS[level]}',
    ])

_CLASSES_HEADING = """\
## Entity types (entity|label)

Indentation shows specialization: an indented type is a kind of the type above
it. Use the most specific type that the text actually supports."""

_OBJECT_PROPERTIES_HEADING = """\
## Relationships (entity|RELATIONSHIP|entity)

Each line is a relationship name followed by the entity types it relates, as
subject -> object."""

_DATATYPE_PROPERTIES_HEADING = """\
## Attributes (entity|ATTRIBUTE_NAME|value)

Grouped by the entity type that carries the attribute. Each line is an
attribute name followed by the kind of value it holds. The kind of value is
there to help you pick the right attribute and report the value in a sensible
form - report what the text says, and do not convert or invent values.

Write a listed attribute name exactly as it appears here. Do not add a prefix
such as HAS_ to it, and do not reword it."""

_PROTOCOL_HEADING = '## Using this vocabulary'

_MAP_BY_MEANING = """\
Map by meaning first. When a relationship or attribute in the text means what
one of the entries above means, use that entry - however differently the text
words it. The text will usually not use the listed wording, and that does not
matter; what the entry means is what decides it."""

_THEN_SPELLING = """\
Then use the listed spelling. Once you have chosen an entry, write its name
exactly as it appears above, in the same case and with the same underscores, and
classify its subject and object as the entity types listed for it."""

_ALIGN_CLOSING = """\
When nothing listed carries the meaning, name the concept yourself as you
normally would. Do not stretch a listed entry to cover something it does not
mean - a wrong listed name is worse than an unlisted one."""

# Two claims, and the second is the one that changes the
# model's incentive: not just that unlisted names are discarded, but that
# omitting therefore costs nothing while a stretched listed name is *kept*. At
# align a wrong listed name is merely wrong; here it is the only way to get bad
# data past the filter, so it is worth naming as the specific hazard.
#
# The opening preference sentence is the one `guide` was removed over. It is kept
# here for a reason that did not apply there: strict discards what does not match,
# so a closing that says "leave the fact out" without first asking the model to
# look for a genuine match would discard more while helping less. What the guide
# measurement establishes is that the sentence is not free, and its effect here -
# alongside four gates rather than alone - has not been measured on its own.
_STRICT_CLOSING = """\
Prefer a listed entry whenever the meaning is close, even when the text words it
quite differently. When nothing listed carries the meaning, leave the fact out:
anything not listed above is discarded, so omitting it loses nothing and naming
it gains nothing. Do not stretch a listed entry to cover something it does not
mean - that is worse than omitting, because a wrong listed name is kept."""

_CLOSINGS = {
    'align': _ALIGN_CLOSING,
    'strict': _STRICT_CLOSING,
}

# The propositions prompt gets the entity types and nothing else. Short on
# purpose: that stage decomposes text into atomic statements and classifies the
# entities it names, and a relationship or attribute vocabulary would be
# instructions for work it does not do.
_PROPOSITION_HEADER = '# Entity types for this extraction'

_PROPOSITION_LEADS = {
    'align': """\
When a proposition classifies a named entity, use one of these types where the
entity is one of them:""",
    'strict': """\
When a proposition classifies a named entity, prefer one of these types whenever
the entity is close to one of them, however differently the text words it:""",
}

# Keyed by level because the align wording is not merely weaker at strict, it is
# wrong there: "classify the entity as you normally would" invites a type that
# `enforce_entity_types` will then drop facts over. The strict variant says what
# actually happens instead.
_PROPOSITION_CLOSINGS = {
    'align': """\
When none of them fits, classify the entity as you normally would. Do not
stretch a listed type to cover something it does not mean.""",
    'strict': """\
When none of them fits, classify the entity as you normally would - but note that
facts about an entity classified as anything not listed above are discarded. Do
not stretch a listed type to cover something it does not mean; a wrong listed
type is kept, and is worse than an unlisted one.""",
}

def format_as_prompt_constraint(index:OntologyIndex, level:str) -> str:
    """Render `index` as a prompt constraint block at `level`.

    Args:
        index: The read index to render.
        level: `'off'`, `'align'` or `'strict'`.

    Returns:
        The block, with no trailing newline, or the empty string when `level` is
        `'off'` or the ontology declares no terms at all.

    Raises:
        ValueError: If `level` is not a level this renderer knows.
    """
    if level not in PROMPT_CONSTRAINT_LEVELS:
        raise ValueError(
            f'Unknown ontology authority level: {level!r}. '
            f'Expected one of {", ".join(PROMPT_CONSTRAINT_LEVELS)}.'
        )

    if level == 'off':
        return ''

    sections = [
        _render_classes(index),
        _render_object_properties(index),
        _render_datatype_properties(index),
    ]
    sections = [section for section in sections if section]

    if not sections:
        return ''

    return '\n\n'.join([
        _HEADER,
        *sections,
        f'{_PROTOCOL_HEADING}\n\n{_MAP_BY_MEANING}\n\n{_THEN_SPELLING}\n\n{_CLOSINGS[level]}',
    ])

def format_as_proposition_constraint(index:OntologyIndex, level:str) -> str:
    """Render the entity types alone, for the propositions prompt.

    The propositions stage decomposes text before topics see it, and the one
    thing it does that an ontology can steer is rule 4 - "add a proposition per
    named entity that classifies that entity". So it gets the class names and
    nothing else: relationships and attributes are extracted downstream, and
    naming them here would spend a large part of the propositions prompt on
    vocabulary that stage cannot use.

    Args:
        index: The read index to render.
        level: `'off'`, `'align'` or `'strict'`.

    Returns:
        The hint, with no trailing newline, or the empty string when `level` is
        `'off'` or the ontology declares no classes.

    Raises:
        ValueError: If `level` is not a level this renderer knows.
    """
    if level not in PROMPT_CONSTRAINT_LEVELS:
        raise ValueError(
            f'Unknown ontology authority level: {level!r}. '
            f'Expected one of {", ".join(PROMPT_CONSTRAINT_LEVELS)}.'
        )

    if level == 'off' or not index.classes:
        return ''

    return '\n\n'.join([
        _PROPOSITION_HEADER,
        f'{_PROPOSITION_LEADS[level]}\n\n{", ".join(rendered_class_names(index))}.',
        _PROPOSITION_CLOSINGS[level],
    ])

def rendered_class_names(index:OntologyIndex) -> List[str]:
    """The class names exactly as the prompt shows them, sorted.

    `preferred_entity_classifications` is seeded from these, so the
    vocabulary block and the `{preferred_entity_classifications}` slot cannot
    name the same class two different ways. That only holds while both go through
    `_render_class_name`, which is why this is the one place either of them gets
    a list of names from.

    Flat and alphabetical rather than in the tree order `_render_classes` uses:
    that ordering carries the hierarchy, and a preference list has nowhere to put
    it.

    Args:
        index: The read index to take the class names from.

    Returns:
        The rendered names, sorted; empty when the ontology declares no classes.
    """
    return sorted(
        _render_class_name(ontology_class) for ontology_class in index.classes.values()
    )

def _render_classes(index:OntologyIndex) -> str:
    """Render the class hierarchy as an indented tree.

    Depth is shown by indentation rather than stated, and the tree is walked
    parent-first so that a specialization sits directly under the type it
    specializes. Alphabetical ordering within each level keeps it deterministic;
    a flat alphabetical list would have put `Athlete` above `Company` and lost
    the structure the model is being shown.
    """
    if not index.classes:
        return ''

    children:Dict[str, List[OntologyClass]] = {}
    roots:List[OntologyClass] = []

    for ontology_class in index.classes.values():
        # A class with more than one declared parent is rendered once, under the
        # first of them; the others are named on its own line, so nothing is
        # lost and no subtree is duplicated.
        parents = [parent for parent in ontology_class.parents if parent in index.classes]
        if parents:
            children.setdefault(parents[0], []).append(ontology_class)
        else:
            roots.append(ontology_class)

    lines:List[str] = []

    def render(ontology_class:OntologyClass, depth:int) -> None:
        extra_parents = [
            _render_class_name(index.classes[parent])
            for parent in ontology_class.parents[1:]
            if parent in index.classes
        ]
        also = ([f'also a kind of {", ".join(extra_parents)}'] if extra_parents else [])
        lines.append('  ' * (depth + 1) + _render_term_line(
            _render_class_name(ontology_class),
            _render_aliases(ontology_class, title_case_with_spaces),
            ontology_class.description,
            also
        ))
        for child in sorted(children.get(ontology_class.iri, []), key=lambda c: c.local_name):
            render(child, depth + 1)

    for root in sorted(roots, key=lambda c: c.local_name):
        render(root, 0)

    return f'{_CLASSES_HEADING}\n\n' + '\n'.join(lines)

def _render_object_properties(index:OntologyIndex) -> str:
    """Render the object properties as `NAME  subject -> object`."""
    if not index.object_properties:
        return ''

    properties = sorted(index.object_properties.values(), key=lambda p: p.local_name)
    width = max(len(_render_property_name(prop)) for prop in properties)

    lines = [
        '  ' + _render_term_line(
            _render_property_name(prop).ljust(width),
            _render_aliases(prop, camel_to_upper_snake),
            prop.description,
            [f'{_class_name(index, prop.domain)} -> {_class_name(index, prop.range)}']
        )
        for prop in properties
    ]

    return f'{_OBJECT_PROPERTIES_HEADING}\n\n' + '\n'.join(lines)

def _render_datatype_properties(index:OntologyIndex) -> str:
    """Render the datatype properties grouped by the class that carries them.

    Properties with no declared domain are grouped last, under a heading that
    says any entity may carry them, rather than being silently attached to a
    class the ontology did not name.
    """
    if not index.datatype_properties:
        return ''

    properties = sorted(index.datatype_properties.values(), key=lambda p: p.local_name)
    width = max(len(_render_property_name(prop)) for prop in properties)

    grouped:Dict[str, List[DatatypeProperty]] = {}
    for prop in properties:
        grouped.setdefault(prop.domain or '', []).append(prop)

    lines:List[str] = []
    for domain in sorted(grouped, key=lambda iri: _domain_sort_key(index, iri)):
        subject = _class_name(index, domain) if domain else ANY_SUBJECT_GROUP
        lines.append(f'  {subject}:')
        for prop in grouped[domain]:
            lines.append('    ' + _render_term_line(
                _render_property_name(prop).ljust(width),
                _render_aliases(prop, camel_to_upper_snake),
                prop.description,
                [_type_name_of(prop.datatype)]
            ))

    return f'{_DATATYPE_PROPERTIES_HEADING}\n\n' + '\n'.join(lines)

def _render_term_line(name:str, aliases:List[str], description:Optional[str], middle:List[str]) -> str:
    """Assemble one vocabulary line: name, then facts, then description.

    `middle` carries whatever the section puts between the name and the
    description - a `subject -> object` pair, a value type, an extra parent.
    """
    parts = [name, *middle]
    if aliases:
        parts.append(f'(also known as {", ".join(aliases)})')
    if description:
        parts.append(f'"{description}"')
    return '  '.join(parts)

def _render_class_name(ontology_class:OntologyClass) -> str:
    """Render a class name for the prompt, preferring the declared label."""
    return title_case_with_spaces(ontology_class.label or ontology_class.local_name)

def _render_property_name(prop) -> str:
    """Render a property name for the prompt, preferring the declared label."""
    return camel_to_upper_snake(prop.label or prop.local_name)

def _render_aliases(term, render) -> List[str]:
    """Render a term's `skos:altLabel` values as "also known as" hints.

    Rendered through the same function as the primary name, because an alias is
    a name the model may emit and therefore has to survive the response parser
    too. An alias that renders to the same name as the term itself is dropped -
    it tells the model nothing.
    """
    primary = resolution_key(term.label or term.local_name)
    rendered:List[str] = []
    for alias in term.aliases:
        if resolution_key(alias) == primary:
            continue
        name = render(alias)
        if name and name not in rendered:
            rendered.append(name)
    return rendered

def _class_name(index:OntologyIndex, iri:Optional[str]) -> str:
    """Render a domain or range class name, or `anything` when unconstrained."""
    if not iri:
        return ANY_ENTITY
    ontology_class = index.classes.get(iri)
    return _render_class_name(ontology_class) if ontology_class else ANY_ENTITY

def _domain_sort_key(index:OntologyIndex, iri:str) -> tuple:
    """Sort domain groups by class local name, with the no-domain group last."""
    if not iri:
        return (1, '')
    ontology_class = index.classes.get(iri)
    return (0, ontology_class.local_name if ontology_class else iri)

def _type_name_of(datatype:str) -> str:
    """Name an XSD range in terms the model can act on."""
    local_name = datatype[len(XSD_NAMESPACE):] if datatype.startswith(XSD_NAMESPACE) else ''
    return _TYPE_NAMES.get(local_name, DEFAULT_TYPE_NAME)
