# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic coercion of an extracted literal to its declared XSD type.

The division of labour on attributes mirrors the rest of the feature: the LLM's
job is to emit a resolvable attribute *name*, and every decision about the
*value* is made here, deterministically. Nothing in this module
asks a model anything.

`coerce_literal` is the single place that answers "is this string a value of that
declared type, and if so what is it natively?". Two callers, both later phases:
`enforce_datatypes` drops a subject-predicate-complement fact when the answer is
`None`, and `typed_properties` writes the native value when it
is not.

Three properties the callers depend on:

* **Every non-`None` return is JSON-serializable.** The value lands in
  `node.metadata` and then in Cypher parameters, so a `Decimal`, a `date`, or a
  float `nan` would fail somewhere far from here. Integers come back as `int`,
  decimals as a *finite* `float`, booleans as `bool`, and dates and times as ISO
  strings.
* **The type families agree with what the prompt promised.** `prompt_constraint`
  renders `xsd:gYear` to the model as `integer` and `xsd:double` as
  `decimal number`; this module coerces them the same way. If the two disagreed,
  a value reported in exactly the form the prompt asked for could still fail
  coercion.
* **No `rdflib` import.** XSD IRIs are plain strings here, so this module is safe
  on both sides of the extraction spawn boundary and can be imported by
  `ontology_filter.py`.

**A wrong value is worse than no value, so parsing does not guess.** This is the
one judgement running through every table below. `enforce_datatypes` treats a
successful coercion as conforming, so a literal parsed to the wrong number is
stored and trusted, whereas `None` is visible and handled. Real extracted output
for `xsd:double` in the replay corpus includes `'41,500,000 dollars'` and
`'2,400,000,000 US dollars'`, and it is tempting to strip the trailing words.
That is refused: nothing distinguishes a harmless unit (`'dollars'`) from a
magnitude (`'nine hundred million'`, `'$1.2bn'`) without interpreting it, and
interpreting it is a conversion, not a parse. Getting `900` for nine hundred
million is a six-order-of-magnitude error that no downstream check would catch.
So a literal must be numeric *in its entirety*, and the unit-bearing values
return `None` and are dropped or skipped by the caller.

The same reasoning excludes all-numeric slash dates: `'01/03/1994'` is the first
of March or the third of January depending on where the text came from, and there
is no way to tell. Non-ISO dates are accepted only where the month is named.
"""

import re

from datetime import date, datetime
from functools import partial
from typing import Any, Optional, Tuple

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_index import (
    XSD_NAMESPACE,
)

# Integer types, with the bounds their declaration promises. The bounds are the
# reason to enumerate rather than to treat every integer type alike: a property
# declared `xsd:nonNegativeInteger` and given `'-5'` has a value outside its
# declared type, and `enforce_datatypes` exists to catch exactly that. `None`
# means unbounded on that side.
#
# `gYear` is here, not with the dates, because that is how the prompt renders it
# (`_TYPE_NAMES` in `prompt_constraint.py` maps it to `integer`) and a value the
# model reported as the prompt asked must coerce.
_INTEGER_BOUNDS = {
    'integer': (None, None),
    'long': (-(2 ** 63), 2 ** 63 - 1),
    'int': (-(2 ** 31), 2 ** 31 - 1),
    'short': (-32768, 32767),
    'byte': (-128, 127),
    'nonNegativeInteger': (0, None),
    'positiveInteger': (1, None),
    'nonPositiveInteger': (None, 0),
    'negativeInteger': (None, -1),
    'unsignedLong': (0, 2 ** 64 - 1),
    'unsignedInt': (0, 2 ** 32 - 1),
    'unsignedShort': (0, 65535),
    'unsignedByte': (0, 255),
    'gYear': (None, None),
}

_DECIMAL_TYPES = frozenset({'decimal', 'double', 'float'})

# XSD's lexical space for boolean is exactly these four, and case folding is the
# only latitude taken. `'yes'` and `'no'` are deliberately absent: the prompt
# tells the model `true/false`, the corpus shows it complying, and inventing
# synonyms here would be guessing at a vocabulary nobody was offered.
_TRUE_LEXICAL = frozenset({'true', '1'})
_FALSE_LEXICAL = frozenset({'false', '0'})

# Shape first, calendar second. A regex alone accepts '2015-02-29'; `strptime`
# is what rejects it. Both are needed - the regex because `strptime('%Y-%m-%d')`
# would otherwise accept '15-2-9' and silently produce year 15.
_ISO_DATE = re.compile(r'^-?\d{4}-\d{2}-\d{2}$')
_ISO_TIME = re.compile(r'^\d{2}:\d{2}(:\d{2}(\.\d+)?)?$')

# Non-ISO dates, accepted only where the month is named and the reading is
# therefore unambiguous. 'March 3rd 2020' is real output from the replay corpus.
_NAMED_MONTH_FORMATS = (
    '%d %B %Y',      # 3 March 2020
    '%d %b %Y',      # 3 Mar 2020
    '%B %d %Y',      # March 3 2020
    '%b %d %Y',      # Mar 3 2020
    '%B %d, %Y',     # March 3, 2020
    '%b %d, %Y',     # Mar 3, 2020
    '%Y %B %d',      # 2020 March 3
)

_ORDINAL_SUFFIX = re.compile(r'(?<=\d)(st|nd|rd|th)\b', re.IGNORECASE)

# A trailing timezone is part of several XSD lexical spaces and carries nothing
# this module reports, so it is removed before parsing rather than rejected.
_TRAILING_TIMEZONE = re.compile(r'(Z|[+-]\d{2}:\d{2})$')

# Digit grouping, and only in valid grouping positions. Matching the whole
# number is what keeps a European decimal comma out: '1994,5' does not match, so
# it is refused rather than silently read as 19945.
_GROUPED_NUMBER = re.compile(r'^([+-]?)(\d{1,3}(?:,\d{3})+)(\.\d+)?$')

# A fraction that adds nothing, so '1994.0' is an integer but '1994.5' is not.
_ZERO_FRACTION = re.compile(r'\.0*$')

_INTEGER_LEXICAL = re.compile(r'^[+-]?\d+$')

# `xsd:anyURI` is almost unconstrained in the standard, so the check here is
# narrow on purpose: it rejects the failure actually seen from a model, which is
# a sentence where a URI was asked for. Internal whitespace is the signal.
_ANY_URI = re.compile(r'^(?:[A-Za-z][A-Za-z0-9+.\-]*:|[/#?])\S*$|^\S+\.\S+\S*$')

def _local_name(xsd_iri:Optional[str]) -> Optional[str]:
    """The XSD local name, or None when the IRI is not an XSD datatype.

    `Ontology` rejects a datatype property whose `rdfs:range` is outside the XSD
    namespace at load time, so a non-XSD IRI here means a caller built an index
    some other way. There is no defensible coercion against a type this module
    knows nothing about, so it declines rather than guessing.
    """
    if not xsd_iri or not xsd_iri.startswith(XSD_NAMESPACE):
        return None
    return xsd_iri[len(XSD_NAMESPACE):]

def _degrouped(text:str) -> Optional[str]:
    """`'2,400,000'` -> `'2400000'`; `'1994,5'` -> None; `'1994'` unchanged."""
    match = _GROUPED_NUMBER.match(text)
    if match:
        return f'{match.group(1)}{match.group(2).replace(",", "")}{match.group(3) or ""}'
    return None if ',' in text else text

def _coerce_integer(text:str, bounds:Tuple[Optional[int], Optional[int]]) -> Optional[int]:
    """An integer, tolerating digit grouping and a zero fraction."""
    degrouped = _degrouped(text)
    if degrouped is None:
        return None

    # '1994.0' is the same integer as '1994'; '1994.5' is not an integer at all.
    if '.' in degrouped:
        if not _ZERO_FRACTION.search(degrouped):
            return None
        degrouped = degrouped[:degrouped.index('.')]

    if not _INTEGER_LEXICAL.match(degrouped):
        return None

    value = int(degrouped)
    (low, high) = bounds
    if (low is not None and value < low) or (high is not None and value > high):
        return None

    return value

def _coerce_decimal(text:str) -> Optional[float]:
    """A finite float. `nan` and `inf` are refused - they are not JSON."""
    degrouped = _degrouped(text)
    if degrouped is None:
        return None

    try:
        value = float(degrouped)
    except ValueError:
        return None

    # `float('nan')` and `float('inf')` both succeed, and `json.dumps` emits
    # `NaN` / `Infinity` for them, which no JSON parser is required to accept.
    # Neither is a value any ontology means by `xsd:double`.
    if value != value or value in (float('inf'), float('-inf')):
        return None

    return value

def _coerce_boolean(text:str) -> Optional[bool]:
    """`True`, `False`, or None. Note the caller must test `is not None`."""
    folded = text.lower()
    if folded in _TRUE_LEXICAL:
        return True
    if folded in _FALSE_LEXICAL:
        return False
    return None

def _coerce_date(text:str) -> Optional[str]:
    """An ISO date string, from an ISO or a named-month input."""
    stripped = _TRAILING_TIMEZONE.sub('', text).strip()

    if _ISO_DATE.match(stripped):
        try:
            return date.fromisoformat(stripped.lstrip('-')).isoformat()
        except ValueError:
            # Shape was right, calendar was not - '2015-02-29'.
            return None

    # 'March 3rd 2020' -> 'March 3 2020'. Done before the format loop because no
    # `strptime` directive matches an ordinal suffix.
    plain = re.sub(r'\s+', ' ', _ORDINAL_SUFFIX.sub('', stripped)).strip()

    for fmt in _NAMED_MONTH_FORMATS:
        try:
            return datetime.strptime(plain, fmt).date().isoformat()
        except ValueError:
            continue

    return None

def _coerce_datetime(text:str) -> Optional[str]:
    """An ISO datetime string. Only the ISO lexical form is accepted."""
    stripped = _TRAILING_TIMEZONE.sub('', text).strip()
    try:
        return datetime.fromisoformat(stripped).isoformat()
    except ValueError:
        return None

def _coerce_time(text:str) -> Optional[str]:
    """An ISO time-of-day string."""
    stripped = _TRAILING_TIMEZONE.sub('', text).strip()
    if not _ISO_TIME.match(stripped):
        return None
    try:
        return datetime.strptime(
            stripped, '%H:%M:%S' if stripped.count(':') == 2 else '%H:%M'
        ).time().isoformat()
    except ValueError:
        return None

def _coerce_any_uri(text:str) -> Optional[str]:
    """The URI, or None when the value is prose rather than a reference."""
    return text if _ANY_URI.match(text) else None

def _coerce_text(text:str) -> str:
    """The trimmed text, for the types whose value space *is* text."""
    return text

# The string family, where returning the text unchanged is the implementation
# rather than a give-up. Kept apart from the fallback below so the two cases are
# distinguishable: `enforce_datatypes` warns about a declaration it could not
# honour, and `xsd:string` is honoured.
_TEXT_TYPES = frozenset({
    'string', 'normalizedString', 'token', 'language',
    'Name', 'NCName', 'NMTOKEN', 'ID', 'IDREF', 'ENTITY',
})

# One dispatch table, so membership and behaviour cannot disagree.
#
# This is deliberately a table rather than a chain of `if local_name == ...`:
# `validates_datatype` below is derived from its keys, and `enforce_datatypes`
# uses that to decide whether a coercion returning text was a check or a silent
# pass. A separately-maintained list of "implemented types"
# would drift the first time a branch was added, and drift silently, since the
# symptom is a missing warning.
_COERCERS = {
    **{name: partial(_coerce_integer, bounds=bounds) for (name, bounds) in _INTEGER_BOUNDS.items()},
    **{name: _coerce_decimal for name in _DECIMAL_TYPES},
    **{name: _coerce_text for name in _TEXT_TYPES},
    'boolean': _coerce_boolean,
    'date': _coerce_date,
    'dateTime': _coerce_datetime,
    'time': _coerce_time,
    'anyURI': _coerce_any_uri,
}

def validates_datatype(xsd_iri:Optional[str]) -> bool:
    """Whether coercion actually checks values of this declared type.

    False means `coerce_literal` will return the trimmed literal for any input,
    so a `True` from `validate_literal_against_xsd` carries no information. The
    caller that enforces datatypes is expected to say so out loud rather than
    report a validation that did not happen.

    Args:
        xsd_iri: A declared `rdfs:range`, as a plain string.

    Returns:
        True for every XSD type with an implementation, including the string
        family. False for an XSD type with no implementation (`xsd:hexBinary`,
        `xsd:duration`, `xsd:gMonthDay`), and False for a non-XSD IRI - which
        `coerce_literal` refuses outright, so nothing is stored unvalidated and
        there is nothing to warn about.
    """
    local_name = _local_name(xsd_iri)
    return local_name is not None and local_name in _COERCERS

def coerce_literal(literal:Optional[str], xsd_iri:Optional[str]) -> Any:
    """Parse an extracted literal into a native value of its declared XSD type.

    Best-effort on the *lexical* form and strict on meaning: digit grouping,
    surrounding whitespace, a zero fraction, an ordinal suffix and a named month
    are all tolerated, while anything needing interpretation is refused. See the
    module docstring for why refusing beats guessing.

    Args:
        literal: The value the model emitted, typically `Fact.complement.value`.
        xsd_iri: The declared `rdfs:range` of the resolved datatype property, as
            a plain string - `DatatypeProperty.datatype`.

    Returns:
        An `int` for the integer types, a finite `float` for the decimal types, a
        `bool` for `xsd:boolean`, an ISO `str` for `xsd:date`, `xsd:dateTime` and
        `xsd:time`, and the trimmed text for `xsd:string` and any other XSD type,
        matching the `text` the prompt renders for those. `None` when the literal
        does not parse, when it is empty, or when `xsd_iri` is not an XSD type.

        **Test the result with `is not None`.** `False`, `0` and `0.0` are all
        successful coercions and all falsy.
    """
    if literal is None or xsd_iri is None:
        return None

    text = literal.strip()
    if not text:
        return None

    local_name = _local_name(xsd_iri)
    if local_name is None:
        return None

    coercer = _COERCERS.get(local_name)
    if coercer is not None:
        return coercer(text)

    # An XSD type with no implementation. Treated as text, which is also what
    # the prompt rendered for it (`DEFAULT_TYPE_NAME`), and returned trimmed
    # because the trimmed form is what would be stored anyway.
    #
    # Note that this is *not* a validation: any input at all is accepted. Callers
    # that need to distinguish this from a real check ask `validates_datatype`
    # first - `enforce_datatypes` does, and warns.
    return text

def validate_literal_against_xsd(literal:Optional[str], xsd_iri:Optional[str]) -> bool:
    """Whether `literal` is a value of the type `xsd_iri` declares.

    One implementation, not two: validity *is* coercibility, so the two can
    never disagree about a value. Kept as a named function because
    `enforce_datatypes` reads as a validity check at its call site even though it
    also wants the coerced value.

    Args:
        literal: The value the model emitted.
        xsd_iri: The declared `rdfs:range`, as a plain string.

    Returns:
        True when the literal parses. `'false'` against `xsd:boolean` is valid
        and returns True, which is the reason this is `is not None` rather than a
        truth test.
    """
    return coerce_literal(literal, xsd_iri) is not None
