# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The naming-convention contract between the ontology and the extraction path.

Three separate jobs, which must agree with each other and with
`indexing/utils/topic_utils.py`:

* **Render** - `camel_to_upper_snake` for properties, `title_case_with_spaces`
  for classes. Chosen so the name survives the response parser: `format_value`
  turns `_` into a space and `format_classification` then applies `.title()`.
* **Resolve** - `resolution_key` folds a name to a convention-free form, so a
  name the LLM emitted in any of the conventions in play still finds its
  declared term.
* **Canonicalize** - not here. The canonical stored spelling is the authored
  name, taken verbatim from `rdfs:label` or the IRI local name with no
  transformation at all, so it needs no helper and must not be folded into one
  of the above.

Rendering and canonicalization are allowed to disagree, and for a class
authored `:SportsTeam` they do: it renders as `Sports Team` and stores as
`SportsTeam`. Collapsing them into a single "normalize" helper is what produced
the bug this module exists to prevent - rendering `SportsTeam` directly comes
back from the parser as `Sportsteam`, which resolves to nothing.

The guarding invariant, asserted over every term in every test ontology:

    resolution_key(parse_transform(render(term))) == resolution_key(local_name)

`render` and `resolution_key` therefore share one camel-case boundary rule:
split only where an uppercase letter follows a **lowercase** letter. The
narrower rule matters. Splitting after any non-uppercase character would split
`Company2X`, which is what `.title()` makes of `Company2x`, and the invariant
would fail on a name that only differs from a working one by a digit.

This module is plain string handling with no `rdflib` import, so it is safe on
both sides of the extraction process boundary.
"""

import re

# Split where an uppercase letter follows a lowercase letter, and nowhere else.
# 'worksFor' -> 'works For'; 'HTTPServer' -> 'HTTPServer' (unsplit, but folded
# to 'httpserver' consistently at both ends of the round trip).
_CAMEL_BOUNDARY = re.compile(r'(?<=[a-z])(?=[A-Z])')

_WHITESPACE_RUN = re.compile(r'\s+')

def _split_words(name:str) -> str:
    """Insert a space at every camel-case boundary and every underscore."""
    return _CAMEL_BOUNDARY.sub(' ', name).replace('_', ' ')

def resolution_key(name:str) -> str:
    """Fold a name to its convention-free form, for index lookup.

    Splits camelCase, replaces underscores with spaces, lowercases, and
    collapses whitespace:

        'worksFor'    -> 'works for'
        'WORKS FOR'   -> 'works for'      <- what the parser actually hands us
        'WORKS_FOR'   -> 'works for'
        'Sports Team' -> 'sports team'
        'SportsTeam'  -> 'sports team'

    `casefold` and not `lower`, because this has to be the inverse of the
    rendering helpers below, and `lower` is not: `camel_to_upper_snake` renders
    with `upper`, `'Größe'.upper()` is `'GRÖSSE'`, and `'GRÖSSE'.lower()` is
    `'grösse'` while `'Größe'.lower()` is `'größe'` - so the name the prompt asked
    the model for resolved to nothing. `casefold` maps both onto `'grösse'`.
    Identical to `lower` on ASCII, so every key in the corpus is unchanged.

    Note what this does not fix: `Straße` and `Strasse` now share a key, so an
    ontology declaring both has a collision rather than one unresolvable term.
    A collision is the better failure - it is at least detectable - but nothing
    detects it yet.

    Args:
        name: A name from anywhere - an IRI local name, an `rdfs:label`, a
            `skos:altLabel`, or a predicate the LLM emitted. `None` and the
            empty string fold to the empty string, which is never indexed.

    Returns:
        The folded key.
    """
    if not name:
        return ''
    return _WHITESPACE_RUN.sub(' ', _split_words(name)).strip().casefold()

_NON_ALPHANUMERIC = re.compile(r'[^a-z0-9]')

def compact_key(name:str) -> str:
    """Fold a name to lowercase alphanumerics, dropping every separator.

    Deliberately coarser than `resolution_key`, which preserves word boundaries
    because it has to round-trip against the prompt rendering:

        'rdf:type' 'RDF_TYPE' 'rdf type'   -> 'rdftype'
        'SportsTeam' 'Sportsteam'          -> 'sportsteam'

    Two callers, for opposite reasons. `OntologyFilter` matches predicates that no
    ontology declared, where there is no round trip to preserve and every spelling
    a model might reach for should land on one key. `OntologyIndex` uses it as a
    *fallback* class key, for the case where the response parser has destroyed a
    word boundary that `resolution_key` relies on - see `resolve_class`.

    Note what this gives up: it cannot tell `rdfs:label` from a predicate
    genuinely named `RDFSLABEL`, and it cannot tell `ABCorp` from `AB Corp`. Both
    callers account for that themselves.
    """
    return _NON_ALPHANUMERIC.sub('', (name or '').lower())

def camel_to_upper_snake(name:str) -> str:
    """Render a property name for the prompt, as `UPPER_SNAKE`.

        'worksFor'    -> 'WORKS_FOR'
        'WORKS_FOR'   -> 'WORKS_FOR'
        'founded year'-> 'FOUNDED_YEAR'

    `EXTRACT_TOPICS_PROMPT` already instructs the model that relationship names
    are "all uppercase, with underscores instead of spaces", so rendering
    `:worksFor` verbatim would put two conflicting instructions in one prompt.

    Args:
        name: A property's local name, label, or alias.

    Returns:
        The rendered name, containing no lowercase letter.
    """
    if not name:
        return ''
    words = _WHITESPACE_RUN.sub(' ', _split_words(name)).strip().split(' ')
    return '_'.join(words).upper()

def title_case_with_spaces(name:str) -> str:
    """Render a class name for the prompt, as `Title Case With Spaces`.

        'SportsTeam'  -> 'Sports Team'
        'SPORTS_TEAM' -> 'Sports Team'
        'Sports Team' -> 'Sports Team'

    `.title()` is applied here rather than left to the parser, which makes the
    rendered form a fixed point of `format_classification` - the name comes
    back from the parser exactly as it went out.

    Args:
        name: A class's local name, label, or alias.

    Returns:
        The rendered name, containing no underscore.
    """
    if not name:
        return ''
    return _WHITESPACE_RUN.sub(' ', _split_words(name)).strip().title()
