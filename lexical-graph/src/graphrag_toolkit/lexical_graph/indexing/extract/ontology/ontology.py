# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Load an OWL/Turtle ontology and validate it at load time.

Turtle is the only accepted file format. Other RDF serializations are supported
by parsing them into an `rdflib.Graph` with rdflib and passing that to
`Ontology.from_graph`.

`Ontology` lives in the parent process only - it owns the `rdflib.Graph` and is
never pickled. It yields two artefacts, both computed once before any text is
seen: the prompt constraint string, and `OntologyIndex`, the plain-data read
index that does cross into extraction workers.

Structural problems are raised as `OntologyLoadError` when the ontology is
loaded, not when a document is extracted, so that an authoring mistake does not
cost an extraction run.
"""

import logging
import re
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Set, Tuple, Union

from rdflib import RDF, RDFS, BNode, Graph, Literal, Namespace

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_index import (
    OWL_THING,
    XSD_NAMESPACE,
    DatatypeProperty,
    ObjectProperty,
    OntologyClass,
    OntologyIndex,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.prompt_constraint import (
    PROMPT_CONSTRAINT_LEVELS,
    PROSE_VOCABULARY,
    VOCABULARY_FORMATS,
    format_as_prompt_constraint,
    format_as_proposition_constraint,
    format_turtle_vocabulary,
    rendered_class_names,
)

logger = logging.getLogger(__name__)

OWL = Namespace('http://www.w3.org/2002/07/owl#')
SKOS = Namespace('http://www.w3.org/2004/02/skos/core#')

TURTLE_SUFFIX = '.ttl'

# Collapses an `rdfs:comment` onto one line before it is serialized into the
# turtle vocabulary block. See `_flattened_comments`.
_WHITESPACE_RUN = re.compile(r'\s+')

OntologyTerms = Tuple[
    Dict[str, OntologyClass],
    Dict[str, ObjectProperty],
    Dict[str, DatatypeProperty],
]

class OntologyLoadError(Exception):
    """Raised when an ontology cannot be loaded.

    Covers malformed Turtle, an unsupported file format, and the structural
    rules checked at load time: `rdfs:subClassOf` cycles, dangling class
    references in `rdfs:subClassOf` / `rdfs:domain` / `rdfs:range`, a property
    declared as both `owl:ObjectProperty` and `owl:DatatypeProperty`, and a
    datatype property whose `rdfs:range` is missing or is not an XSD datatype.

    No partial `Ontology` is returned when this is raised.
    """

class Ontology:
    """A parsed, validated OWL/Turtle ontology.

    Attributes:
        graph (Graph): The canonical representation. Public and immutable by
            convention - `graph.serialize()` is the supported way to write an
            ontology back out, which is why there is no `serialize()` method
            here.
        namespace (str): The base IRI, detected from `@prefix :`, rdflib's
            default namespace, the first `owl:Ontology` subject, or the
            caller-supplied `base_iri`. Empty when none of those resolve; this
            is not an error, because nothing in the feature requires it.
    """

    def __init__(self, graph:Graph, base_iri:Optional[str]=None):
        """Adopt a parsed graph, index it, and validate it.

        Raises:
            OntologyLoadError: If `graph` is not an `rdflib.Graph`, or if the
                ontology violates one of the load-time structural rules.
        """
        if not isinstance(graph, Graph):
            raise OntologyLoadError(
                f'Expected an rdflib.Graph, got {type(graph).__name__}.'
            )

        self.graph = graph
        self.namespace = _extract_base_iri(graph, base_iri) or ''

        # Index first so that validation has plain data to read, then validate
        # before __init__ returns - a caller never sees an unvalidated
        # Ontology. Cycle detection raises from within the index build, since
        # the ancestor closure is where a cycle shows up.
        self._index = self._build_index()
        self._validate()

    @classmethod
    def from_turtle(cls, path:Union[str, Path]) -> 'Ontology':
        """Load an ontology from a Turtle file.

        Args:
            path: Path to a `.ttl` file.

        Raises:
            OntologyLoadError: If the suffix is not `.ttl`, the file does not
                exist, the Turtle does not parse, or the ontology is
                structurally invalid.
        """
        path = Path(path)

        if path.suffix.lower() != TURTLE_SUFFIX:
            raise OntologyLoadError(
                f'Unsupported ontology file format: {path.name}. Turtle '
                f'({TURTLE_SUFFIX}) is the only supported file format. Parse '
                f'other RDF serializations with rdflib and pass the resulting '
                f'graph to Ontology.from_graph().'
            )

        if not path.is_file():
            raise OntologyLoadError(f'Ontology file not found: {path}')

        graph = Graph()
        try:
            graph.parse(source=str(path), format='turtle')
        except Exception as e:
            raise OntologyLoadError(f'Failed to parse Turtle file {path}: {e}') from e

        return cls(graph)

    @classmethod
    def from_turtle_string(cls, turtle:str, base_iri:Optional[str]=None) -> 'Ontology':
        """Load an ontology from a Turtle document held in memory.

        Args:
            turtle: The Turtle document.
            base_iri: Optional base IRI, passed to rdflib as `publicID` to
                resolve relative IRIs, and used as the lowest-priority fallback
                when detecting `namespace`.

        Raises:
            OntologyLoadError: If the Turtle does not parse, or the ontology is
                structurally invalid.
        """
        graph = Graph()
        try:
            graph.parse(data=turtle, format='turtle', publicID=base_iri)
        except Exception as e:
            raise OntologyLoadError(f'Failed to parse Turtle string: {e}') from e

        return cls(graph, base_iri=base_iri)

    @classmethod
    def from_graph(cls, graph:Graph) -> 'Ontology':
        """Adopt an already-parsed `rdflib.Graph` unchanged.

        The supported route for any RDF serialization other than Turtle: parse
        it with rdflib, then pass the graph here.

        Raises:
            OntologyLoadError: If `graph` is not an `rdflib.Graph`, or the
                ontology is structurally invalid.
        """
        return cls(graph)

    @classmethod
    def load(cls, source:Union['Ontology', Graph, str, Path]) -> 'Ontology':
        """Load an ontology from whatever form the caller has one in.

        The dispatching constructor, and the one configuration goes through:
        `ExtractionConfig(ontology=...)` accepts a path because most users have
        a file, and an `Ontology` because a user who built one already should not
        pay to parse it twice.

        Args:
            source: An `Ontology`, returned unchanged; an `rdflib.Graph`, adopted
                by `from_graph`; or a `str`/`Path` to a `.ttl` file, parsed by
                `from_turtle`.

        Raises:
            OntologyLoadError: If `source` is a path with a suffix other than
                `.ttl`, or is not one of the forms above. A Turtle document held
                in a string is loaded by `from_turtle_string`, not here - a bare
                `str` is read as a path, because guessing between the two from
                its content would be the kind of ambiguity that surfaces as a
                confusing parse error.
        """
        if isinstance(source, Ontology):
            return source

        if isinstance(source, Graph):
            return cls.from_graph(source)

        if isinstance(source, (str, Path)):
            return cls.from_turtle(source)

        raise OntologyLoadError(
            f'Cannot load an ontology from {type(source).__name__}. Pass an '
            f'Ontology, an rdflib.Graph, or a path to a Turtle '
            f'({TURTLE_SUFFIX}) file. A Turtle document held in a string is '
            f'loaded with Ontology.from_turtle_string().'
        )

    def index(self) -> OntologyIndex:
        """Return the read index. Built once at construction."""
        return self._index

    def format_as_prompt_constraint(self, level:str, vocabulary_format:str=PROSE_VOCABULARY) -> str:
        """Render the ontology as the constraint block for the prompt.

        The block names the vocabulary the model should reach for and how much
        authority it has; it is composed into the extraction prompt rather than
        replacing any part of it. Rendered once at pipeline-configuration time,
        in this process.

        Two formats. `'prose'` renders the three generated sections and is the
        default and the measured configuration. `'turtle'` shows this ontology's
        own source instead, on the argument that the prose sections paraphrase
        away the RDFS entailments - `rdfs:subClassOf` survives only as
        indentation - and that a model has read a great deal of real RDFS.

        Serializing is done here rather than in the renderer because the renderer
        works from `OntologyIndex` and imports no `rdflib`. `format='turtle'`
        round-trips the canonical graph, so the text the model sees is the
        ontology as loaded, not as authored - prefix bindings and triple order may
        differ from the input file, and a blank node rdflib cannot inline is
        labelled freshly per process, so the block is not reproducible across
        runs.

        Args:
            level: `'off'`, `'align'` or `'strict'`.
            vocabulary_format: `'prose'` (default) or `'turtle'`.

        Returns:
            The block, with no trailing newline, or `''` for `'off'` and for an
            ontology that declares no terms.

        Raises:
            ValueError: If `level` is not a known authority level, or
                `vocabulary_format` is not a known format.
        """
        if vocabulary_format not in VOCABULARY_FORMATS:
            raise ValueError(
                f'Unknown ontology vocabulary format: {vocabulary_format!r}. '
                f'Expected one of {", ".join(VOCABULARY_FORMATS)}.'
            )

        if vocabulary_format == PROSE_VOCABULARY:
            return format_as_prompt_constraint(self._index, level)

        # Validate the level on the same terms the prose path does, and before
        # paying to serialize, so a typo is reported identically either way.
        if level not in PROMPT_CONSTRAINT_LEVELS:
            raise ValueError(
                f'Unknown ontology authority level: {level!r}. '
                f'Expected one of {", ".join(PROMPT_CONSTRAINT_LEVELS)}.'
            )

        if level == 'off':
            return ''

        # An ontology declaring no terms renders nothing in either format. Tested
        # against the index rather than the graph because a graph can hold
        # prefix bindings and an `owl:Ontology` header and still declare no
        # vocabulary, and a block with a header and no terms is worse than none.
        if not (self._index.classes or self._index.object_properties
                or self._index.datatype_properties):
            return ''

        return format_turtle_vocabulary(
            _stable_blank_node_labels(
                _flattened_comments(self.graph).serialize(format='turtle')
            ),
            level,
        )

    def format_as_proposition_constraint(self, level:str) -> str:
        """Render the entity types alone, for the propositions prompt.

        The propositions stage classifies the entities it names and extracts
        nothing else, so it is steered with the class names and not with the full
        vocabulary.

        Args:
            level: `'off'`, `'align'` or `'strict'`.

        Returns:
            The hint, with no trailing newline, or `''` for `'off'` and for an
            ontology that declares no classes.

        Raises:
            ValueError: If `level` is not a known authority level.
        """
        return format_as_proposition_constraint(self._index, level)

    def class_names(self) -> List[str]:
        """The rendered class names, for seeding `preferred_entity_classifications`.

        Independent of `ontology_authority`, unlike the two `format_as_*` methods: the
        level decides how much the prompt says *about* the vocabulary, not what
        the vocabulary is. A caller that wants nothing seeded reads the level
        itself and does not call this.

        Returns:
            The names as the prompt renders them, sorted; empty when the ontology
            declares no classes.
        """
        return rendered_class_names(self._index)

    def _build_index(self) -> OntologyIndex:
        """Read the graph into plain data.

        Raises:
            OntologyLoadError: If `rdfs:subClassOf` contains a cycle.
        """
        classes, object_properties, datatype_properties = _collect_terms(self.graph)

        _warn_if_no_terms(self.graph, classes, object_properties, datatype_properties)

        return OntologyIndex(
            classes=classes,
            object_properties=object_properties,
            datatype_properties=datatype_properties,
        )

    def _validate(self) -> None:
        """Run the load-time structural checks over the built index.

        Raises:
            OntologyLoadError: On the first violation found, naming the
                offending term.
        """
        _validate_terms(
            self._index.classes,
            self._index.object_properties,
            self._index.datatype_properties,
        )

def _flattened_comments(graph:Graph) -> Graph:
    """Return a copy of `graph` with every `rdfs:comment` collapsed to one line.

    The prose renderer flattens comments as it writes each vocabulary line, but
    the `'turtle'` format shows the ontology's own serialization, so a comment
    reaches the prompt with its newlines intact - and a comment carrying a newline
    and then `##` forges a section heading in a block whose structure is headings.
    Measured before this: the turtle block had two `## Using this vocabulary`
    headings where the prose block had one.

    A copy, not an edit. `Ontology.graph` is public and documented as immutable by
    convention - `graph.serialize()` is the supported way to write an ontology back
    out - so flattening in place would change what a caller reads back. Only
    `rdfs:comment` objects are rewritten, and a literal's language tag and datatype
    are carried over so the copy differs from the original in whitespace alone.

    Note this does not make the block injection-proof, and is not trying to: the
    fence is sized to the content by `format_turtle_vocabulary`, and what remains
    is prose inside a fenced block that the header names as the ontology.
    """
    flattened = Graph()
    for prefix, namespace in graph.namespaces():
        flattened.bind(prefix, namespace, override=True)

    for (s, p, o) in graph:
        if p == RDFS.comment and isinstance(o, Literal):
            o = Literal(
                _WHITESPACE_RUN.sub(' ', str(o)).strip(),
                lang=o.language,
                datatype=o.datatype if o.language is None else None,
            )
        flattened.add((s, p, o))

    return flattened

def _stable_blank_node_labels(turtle:str) -> str:
    """Renumber blank-node labels to `_:b1`, `_:b2`, … in first-appearance order.

    rdflib mints a fresh `_:nXXXXb1` per process for any blank node it cannot
    inline - one referenced by more than one subject, such as a reused
    `owl:Restriction`. That label reaches the prompt verbatim, and `LLMCache` keys
    on the formatted prompt, so without this the same ontology produces a
    different cache key on every run: a guaranteed miss per chunk per run, and no
    byte-reproducible build. Measured before this: three processes, three blocks,
    three different sha256s.

    Renumbering rather than skolemising, because `Graph.skolemize()` mints a fresh
    UUID too and so is no more stable. Ordering by first appearance in the
    serialized text makes the result a function of the serialization, which is
    already deterministic for everything except these labels.

    The labels carry no meaning - a blank node is anonymous by definition, and the
    only thing a label has to do is match its other occurrences.
    """
    labels:Dict[str, str] = {}

    def replace(match):
        label = match.group(1)
        if label not in labels:
            labels[label] = f'b{len(labels) + 1}'
        return f'_:{labels[label]}'

    # The lookbehind is load-bearing. Without it the `_:` matches inside a legal
    # prefixed name whose prefix ends in an underscore - `@prefix ex_:` makes
    # `ex_:Company` contain `_:Company` - and the class gets rewritten to
    # `ex_:b1`, corrupting the vocabulary this function exists to stabilise. A
    # blank node label is only a blank node label at the start of a token.
    return re.sub(r'(?<![A-Za-z0-9_\-])_:([A-Za-z][A-Za-z0-9_-]*)', replace, turtle)

def _local_name_of(iri:str) -> str:
    """Return an IRI's last segment, split on `#` then `/`."""
    if '#' in iri:
        return iri.rsplit('#', 1)[-1]
    if '/' in iri:
        return iri.rsplit('/', 1)[-1]
    return iri

def _values(graph:Graph, subject, predicate) -> List[str]:
    """Return the sorted, deduplicated non-blank objects of a predicate.

    Sorted because rdflib does not guarantee an order over a triple pattern,
    and every artefact derived from the ontology has to be deterministic.
    """
    return sorted(
        {str(o) for o in graph.objects(subject, predicate) if not isinstance(o, BNode)}
    )

def _first_value(graph:Graph, subject, predicate) -> Optional[str]:
    """Return the first of `_values`, or None when the predicate is absent.

    Warns when there is more than one, for the same reason `_class_reference` and
    the datatype-range check do: the winner is decided by string ordering, and for
    `rdfs:label` that string becomes the term's **canonical stored spelling** -
    `authored_name` prefers the label over the local name, and `create_entity_id`
    hashes the classification into the node id. So a term carrying
    `"Aktiengesellschaft"@de` alongside `"Company"@en` puts the German label on
    every entity node, decided by nothing but the alphabet.

    No language preference is applied. Picking `@en` would put an opinion about
    language in exactly one place in the toolkit with no setting to change it;
    reporting the ambiguity lets the author resolve it in the ontology, where the
    answer belongs. Multilingual vocabularies are the route this arrives by -
    `owl:imports` is not followed, so reusing one means inlining its terms and
    their language tags.
    """
    values = _values(graph, subject, predicate)

    if len(values) > 1:
        logger.warning(
            'Term %s declares %d %s values (%s); using %r. One per term is '
            'supported, and for rdfs:label the winner becomes the canonical '
            'stored spelling.',
            subject, len(values), _local_name_of(str(predicate)),
            ', '.join(repr(value) for value in values), values[0],
        )

    return values[0] if values else None

def _class_reference(
    graph:Graph, subject, predicate, term_iri:str
) -> Optional[str]:
    """Return a single class IRI for an `rdfs:domain` / `rdfs:range` slot.

    `None` means the slot is unconstrained - either undeclared or declared as
    `owl:Thing`, which are the same thing as far as the filter is concerned.
    Multiple declarations are narrowed to one, deterministically and with a
    warning, because a single domain and range per property is what the
    prompt rendering and the domain/range check are defined over.
    """
    values = [value for value in _values(graph, subject, predicate) if value != OWL_THING]

    if not values:
        return None

    if len(values) > 1:
        logger.warning(
            'Property %s declares %d %s values (%s); using %s. One domain and '
            'one range per property is supported.',
            term_iri, len(values), _local_name_of(str(predicate)),
            ', '.join(values), values[0],
        )

    return values[0]

def _collect_terms(graph:Graph) -> OntologyTerms:
    """Read classes, object properties and datatype properties out of a graph.

    Blank-node subjects are skipped: anonymous class axioms such as an
    `owl:Restriction` body are not terms a user can name in a domain or range,
    and are not vocabulary the model can be asked to emit.

    Raises:
        OntologyLoadError: If `rdfs:subClassOf` contains a cycle.
    """
    classes:Dict[str, OntologyClass] = {}
    parents_by_iri:Dict[str, List[str]] = {}

    for subject in graph.subjects(RDF.type, OWL.Class):
        if isinstance(subject, BNode):
            continue
        iri = str(subject)
        # `owl:Thing` is dropped rather than carried, for the same reason
        # `_class_reference` drops it from a domain or range: it constrains
        # nothing, and every class is already a subclass of it. Carried through,
        # it is a `subClassOf` reference to a class the ontology does not
        # declare, so `_validate_terms` rejects the file - and
        # `:C rdfs:subClassOf owl:Thing` is a legal axiom that Protege emits by
        # default. The only workaround was to declare `owl:Thing a owl:Class`,
        # which then puts a meaningless `Thing` entity type in the prompt.
        parents_by_iri[iri] = [
            parent for parent in _values(graph, subject, RDFS.subClassOf)
            if parent != OWL_THING
        ]

    ancestors_by_iri = _compute_subclass_closure(parents_by_iri)

    for subject in graph.subjects(RDF.type, OWL.Class):
        if isinstance(subject, BNode):
            continue
        iri = str(subject)
        classes[iri] = OntologyClass(
            iri=iri,
            local_name=_local_name_of(iri),
            label=_first_value(graph, subject, RDFS.label),
            aliases=_values(graph, subject, SKOS.altLabel),
            parents=parents_by_iri[iri],
            ancestors=ancestors_by_iri[iri],
            description=_first_value(graph, subject, RDFS.comment),
        )

    object_properties:Dict[str, ObjectProperty] = {}

    for subject in graph.subjects(RDF.type, OWL.ObjectProperty):
        if isinstance(subject, BNode):
            continue
        iri = str(subject)
        object_properties[iri] = ObjectProperty(
            iri=iri,
            local_name=_local_name_of(iri),
            label=_first_value(graph, subject, RDFS.label),
            aliases=_values(graph, subject, SKOS.altLabel),
            domain=_class_reference(graph, subject, RDFS.domain, iri),
            range=_class_reference(graph, subject, RDFS.range, iri),
            description=_first_value(graph, subject, RDFS.comment),
        )

    datatype_properties:Dict[str, DatatypeProperty] = {}

    for subject in graph.subjects(RDF.type, OWL.DatatypeProperty):
        if isinstance(subject, BNode):
            continue
        iri = str(subject)
        # An absent or non-XSD range is rejected by _validate_terms; store the
        # empty string here so the model invariant (one datatype per property)
        # holds even before validation runs.
        ranges = _values(graph, subject, RDFS.range)
        # Warned about for the same reason `_class_reference` warns about a
        # multiply-declared domain, and deliberately not silent: which of two
        # declared XSD ranges wins decides whether a literal coerces at all, and
        # `_values` sorts, so the winner is chosen by spelling.
        if len(ranges) > 1:
            logger.warning(
                'DatatypeProperty %s declares %d rdfs:range values (%s); using '
                '%s. One XSD range per datatype property is supported.',
                iri, len(ranges), ', '.join(ranges), ranges[0],
            )
        datatype_properties[iri] = DatatypeProperty(
            iri=iri,
            local_name=_local_name_of(iri),
            label=_first_value(graph, subject, RDFS.label),
            aliases=_values(graph, subject, SKOS.altLabel),
            domain=_class_reference(graph, subject, RDFS.domain, iri),
            datatype=ranges[0] if ranges else '',
            description=_first_value(graph, subject, RDFS.comment),
        )

    return classes, object_properties, datatype_properties

def _warn_if_no_terms(
    graph:Graph,
    classes:Dict[str, OntologyClass],
    object_properties:Dict[str, ObjectProperty],
    datatype_properties:Dict[str, DatatypeProperty],
) -> None:
    """Warn when a non-empty graph yielded no vocabulary at all.

    A warning and not an `OntologyLoadError`, because an ontology that declares
    only an `owl:Ontology` header is legal and the rest of the feature already
    treats it as "no vocabulary" rather than as an error. But it must not be
    *silent*: an ontology with no terms still makes `filter_required()` True and
    still replaces `DEFAULT_ENTITY_CLASSIFICATIONS` with an empty list, so at
    `ontology_authority='strict'` nothing resolves, every fact is dropped, and the
    user finds out after paying for the whole extraction run - with
    `report_violations` defaulting to False, from nothing at all.

    The RDFS/SKOS dialects are called out by name because they are the mistake
    this actually catches: `_collect_terms` reads `owl:Class`,
    `owl:ObjectProperty` and `owl:DatatypeProperty`, so an ontology written with
    `rdfs:Class` and `rdf:Property` parses cleanly and yields nothing, and the
    documentation invites exactly that input by saying "OWL/RDFS".
    """
    if classes or object_properties or datatype_properties:
        return

    if not len(graph):
        return

    rdfs_classes = sum(1 for _ in graph.subjects(RDF.type, RDFS.Class))
    rdf_properties = sum(1 for _ in graph.subjects(RDF.type, RDF.Property))

    hint = ''
    if rdfs_classes or rdf_properties:
        hint = (
            f' The graph does declare {rdfs_classes} rdfs:Class and '
            f'{rdf_properties} rdf:Property term(s), which are not read: '
            f'retype them as owl:Class, owl:ObjectProperty and '
            f'owl:DatatypeProperty.'
        )

    logger.warning(
        'Ontology parsed %d triples but declares no owl:Class, '
        'owl:ObjectProperty or owl:DatatypeProperty terms, so it can steer '
        'nothing: the prompt gets no vocabulary block, the preferred entity '
        'classifications are seeded empty, and at ontology_authority=%r every '
        'fact is dropped.%s',
        len(graph), 'strict', hint,
    )

def _compute_subclass_closure(
    parents_by_iri:Dict[str, List[str]]
) -> Dict[str, FrozenSet[str]]:
    """Compute the reflexive transitive `rdfs:subClassOf` closure.

    A memoized depth-first walk over the parent edges. Parents that are not
    themselves declared classes are skipped here so that `ancestors` only ever
    holds declared class IRIs - `_validate_terms` is the single place that
    raises on a dangling reference.

    Raises:
        OntologyLoadError: If a cycle is reached, naming the class at which it
            closed.
    """
    memo:Dict[str, FrozenSet[str]] = {}
    path:List[str] = []

    def compute(iri:str) -> FrozenSet[str]:
        if iri in memo:
            return memo[iri]
        if iri in path:
            cycle = path[path.index(iri):] + [iri]
            raise OntologyLoadError(
                f'rdfs:subClassOf cycle detected at {iri}: '
                f'{" -> ".join(cycle)}.'
            )
        path.append(iri)

        ancestors:Set[str] = {iri}
        for parent_iri in parents_by_iri.get(iri, []):
            if parent_iri in parents_by_iri:
                ancestors |= compute(parent_iri)

        path.pop()
        memo[iri] = frozenset(ancestors)
        return memo[iri]

    return {iri: compute(iri) for iri in sorted(parents_by_iri)}

def _validate_terms(
    classes:Dict[str, OntologyClass],
    object_properties:Dict[str, ObjectProperty],
    datatype_properties:Dict[str, DatatypeProperty],
) -> None:
    """Check the structural rules that the closure walk does not cover.

    Cheapest check first, and each raises on the first offending term so that a
    broken ontology fails fast with one actionable message. Iteration is sorted
    so the term named in the message is deterministic.

    Raises:
        OntologyLoadError: On the first violation found.
    """
    both = sorted(set(object_properties) & set(datatype_properties))
    if both:
        raise OntologyLoadError(
            f'Property {both[0]} is declared as both an owl:ObjectProperty and '
            f'an owl:DatatypeProperty. A property must be one or the other.'
        )

    for iri, datatype_property in sorted(datatype_properties.items()):
        if not datatype_property.datatype:
            raise OntologyLoadError(
                f'DatatypeProperty {iri} has no rdfs:range. Every '
                f'owl:DatatypeProperty must declare one XSD datatype as its '
                f'range.'
            )
        if not datatype_property.datatype.startswith(XSD_NAMESPACE):
            raise OntologyLoadError(
                f'DatatypeProperty {iri} has rdfs:range '
                f'{datatype_property.datatype}, which is not an XSD datatype. '
                f'Only ranges in {XSD_NAMESPACE} are supported.'
            )

    for iri, ontology_class in sorted(classes.items()):
        for parent_iri in ontology_class.parents:
            if parent_iri not in classes:
                raise OntologyLoadError(
                    f'Class {iri} has a dangling rdfs:subClassOf reference to '
                    f'{parent_iri}, which is not declared as an owl:Class in '
                    f'this ontology.'
                )

    for iri, object_property in sorted(object_properties.items()):
        for slot, class_iri in (
            ('rdfs:domain', object_property.domain),
            ('rdfs:range', object_property.range),
        ):
            if class_iri is not None and class_iri not in classes:
                raise OntologyLoadError(
                    f'ObjectProperty {iri} has a dangling {slot} reference to '
                    f'{class_iri}, which is not declared as an owl:Class in '
                    f'this ontology.'
                )

    for iri, datatype_property in sorted(datatype_properties.items()):
        class_iri = datatype_property.domain
        if class_iri is not None and class_iri not in classes:
            raise OntologyLoadError(
                f'DatatypeProperty {iri} has a dangling rdfs:domain reference '
                f'to {class_iri}, which is not declared as an owl:Class in '
                f'this ontology.'
            )

def _extract_base_iri(graph:Graph, base_iri:Optional[str]=None) -> Optional[str]:
    """Detect the ontology's base IRI.

    In priority order: the Turtle `@prefix : <...>` declaration, which rdflib
    surfaces as the empty prefix; rdflib's parser-level default namespace; the
    first `owl:Ontology` subject; and finally the caller-supplied `base_iri`,
    last so that an in-file declaration always wins.

    Returns None when none of those resolve.
    """
    for prefix, namespace in graph.namespaces():
        if prefix == '' and str(namespace):
            return str(namespace)

    default_namespace = getattr(graph, 'default_namespace', None)
    if default_namespace and str(default_namespace):
        return str(default_namespace)

    for subject in sorted(str(s) for s in graph.subjects(RDF.type, OWL.Ontology)):
        if subject:
            return subject

    return base_iri or None
