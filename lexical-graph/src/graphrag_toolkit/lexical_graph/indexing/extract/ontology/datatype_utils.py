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
#
# No leading `-`. XSD's lexical space for a negative (BCE) year is legal, but
# nothing here can represent one: `date.fromisoformat` refuses it, so accepting
# the shape only led to the sign being stripped and '-0500-01-01' stored as
# 0500-01-01 - a value off by a millennium and reported as conforming. Refusing
# it is both correct and what `_coerce_datetime` already does.
_ISO_DATE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
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

# A trailing timezone is part of several XSD lexical spaces.
#
# For `xsd:date` it is dropped: the value being reported is a calendar date, and
# the offset only says which instant within that day the label was anchored to.
#
# For `xsd:dateTime` and `xsd:time` it is **refused**, because there dropping it
# changes the value. Stripping meant `2020-03-03T23:00:00-05:00` and
# `2020-03-04T04:00:00Z` - the same instant - stored as different strings, while
# `-05:00` and `+09:00`, fourteen hours apart, stored as identical ones. Both were
# reported as conforming, which defeats the range queries the feature exists for.
# Converting to UTC instead was considered and rejected: it would silently rewrite
# the value the text stated, which the prompt promises the model it will not do,
# and would leave a naive local time indistinguishable from a converted one.
# Refusing is visible - `enforce_datatypes` drops the fact, `typed_properties`
# skips the write, and the string is still on the node as `value`.
_TRAILING_TIMEZONE = re.compile(r'(Z|[+-]\d{2}:\d{2})$')

# Digit grouping, and only in valid grouping positions. Matching the whole
# number is what keeps a European decimal comma out: '1994,5' does not match, so
# it is refused rather than silently read as 19945.
_GROUPED_NUMBER = re.compile(r'^([+-]?)(\d{1,3}(?:,\d{3})+)(\.\d+)?$')

# A fraction that adds nothing, so '1994.0' is an integer but '1994.5' is not.
_ZERO_FRACTION = re.compile(r'\.0*$')

_INTEGER_LEXICAL = re.compile(r'^[+-]?\d+$')

# CPython's default limit for string-to-int conversion, above which `int()` raises
# `ValueError` (3.11+). Hard-coded rather than read from
# `sys.get_int_max_str_digits()`: an interpreter configured with a different limit
# would otherwise change which literals coerce, and this module's answers should
# not depend on that.
_MAX_INTEGER_DIGITS = 4300

# `xsd:anyURI` is almost unconstrained in the standard, so the check here is
# narrow on purpose: it rejects the failure actually seen from a model, which is
# a sentence where a URI was asked for. Internal whitespace is the signal.
#
# Only the scheme prefix is a regex. The rest of the test - no whitespace, and
# otherwise an interior `.` - is spelled out in `_coerce_any_uri`, because as one
# alternation (`^(?:scheme|[/#?])\S*$|^\S+\.\S+$`) it was quadratic on exactly the
# input it exists to reject: dotted prose, which fails only on its spaces, made
# the engine try every dot against every tail. A 4 KB value took 28 seconds, and
# the value comes from LLM output.
_ANY_URI_SCHEME = re.compile(r'^(?:[A-Za-z][A-Za-z0-9+.\-]*:|[/#?])')

_WHITESPACE = re.compile(r'\s')

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

    # `int()` on a very long digit string raises rather than returning a number:
    # CPython caps string-to-int conversion at 4300 digits by default (3.11+,
    # CVE-2020-10735). The literal comes from document text, so a run of digits
    # that long is reachable, and an uncaught `ValueError` here would end the run.
    # Refused for the same reason a unit-bearing number is: this module returns
    # None for anything it cannot turn into a value, and never raises.
    if len(degrouped.lstrip('+-')) > _MAX_INTEGER_DIGITS:
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
            return date.fromisoformat(stripped).isoformat()
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
    """An ISO datetime string. Only the ISO lexical form, and only without a zone.

    A zoned literal is refused rather than stripped - see `_TRAILING_TIMEZONE`.

    The zone is detected on the *parsed* value rather than by matching the text.
    A regex has to enumerate the forms, and `_TRAILING_TIMEZONE` only covers `Z`
    and `±HH:MM`, so `'...+0530'` and `'...+05'` slipped past it and then parsed
    successfully on 3.11+ - storing the offset, which is the one outcome refusing
    was meant to prevent, while the spelled-out `'...-05:00'` was refused. Asking
    `tzinfo` cannot miss a form the parser accepts.
    """
    stripped = text.strip()
    try:
        parsed = datetime.fromisoformat(stripped)
    except ValueError:
        return None

    if parsed.tzinfo is not None:
        return None

    return parsed.isoformat()

def _coerce_time(text:str) -> Optional[str]:
    """An ISO time-of-day string.

    The format is picked from the shape `_ISO_TIME` matched, fractional seconds
    included: `%H:%M:%S` with no `%f` would reject every value the `(\\.\\d+)?`
    group admits, and `xsd:dateTime` accepts the same fraction, so rejecting it
    here would make the two types disagree about one lexical form.

    A zoned literal is refused, which a time of day is exactly the case for - an
    offset carries all of the meaning there. No explicit check is needed: unlike
    `_coerce_datetime`, this path is gated on `_ISO_TIME`, which is anchored and
    admits nothing after the optional fraction, so every spelling of an offset -
    `Z`, `±HH:MM`, `±HHMM`, `±HH` - fails the shape test.
    """
    stripped = text.strip()
    if not _ISO_TIME.match(stripped):
        return None

    if stripped.count(':') < 2:
        fmt = '%H:%M'
    elif '.' in stripped:
        fmt = '%H:%M:%S.%f'
    else:
        fmt = '%H:%M:%S'

    try:
        return datetime.strptime(stripped, fmt).time().isoformat()
    except ValueError:
        return None

def _coerce_any_uri(text:str) -> Optional[str]:
    """The URI, or None when the value is prose rather than a reference.

    Accepts a whitespace-free value that either carries a scheme (or begins
    `/`, `#`, `?`) or contains a `.` with something on each side - `example.com`,
    so a bare host is not rejected. Exactly the language the single alternation
    this replaced accepted, in linear time; see `_ANY_URI_SCHEME`.
    """
    if _WHITESPACE.search(text):
        return None
    if _ANY_URI_SCHEME.match(text):
        return text
    # An interior '.': `^\S+\.\S+$` needs at least one character on each side of
    # it, which is what excluding the first and last positions expresses.
    return text if '.' in text[1:-1] else None

def _coerce_text(text:str) -> str:
    """The trimmed text, for the one type whose value space *is* text."""
    return text

# `xsd:string` is the only member of the string family in the table below, and its
# absence from it for the others is the point.
#
# Returning the text unchanged is the *implementation* for `xsd:string`, whose
# value space is any sequence of characters. It is a give-up for every sibling,
# because each of those has a restricted lexical space that nothing here checks:
#
#   normalizedString   no CR, LF or tab
#   token              normalizedString, plus no leading, trailing or doubled space
#   language           a BCP 47 tag - `[A-Za-z]{1,8}(-[A-Za-z0-9]{1,8})*`
#   Name               an XML Name production
#   NCName, ID,        an XML Name with no colon
#     IDREF, ENTITY
#   NMTOKEN            XML NameChars only, so no whitespace
#
# They used to sit here alongside `xsd:string`, which made `validates_datatype`
# return True for all ten - so `enforce_datatypes` neither dropped anything nor
# warned, and `'has spaces and: colons\n'` was reported as a conforming
# `xsd:NCName`. That contradicted this module's own contract, which says a `True`
# from `validate_literal_against_xsd` means the value *was* checked.
#
# Leaving them out of `_COERCERS` is the whole fix: `coerce_literal` falls through
# to the unimplemented-type path, which already returns the trimmed text and
# already makes `enforce_datatypes` warn once per type. Behaviour for stored values
# is unchanged - the string is kept verbatim either way - and the user is now told
# the declaration was not honoured, exactly as they are for `xsd:duration`.
#
# Not implemented instead, deliberately: at `strict` a failing value is dropped,
# and losing a fact because a `token` carried two consecutive spaces is a worse
# outcome than storing it unchecked. These types document intent; they are not
# worth making into a gate.

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
    'string': _coerce_text,
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
        True for every XSD type with an implementation. `xsd:string` counts:
        returning the literal unchanged *is* the check, because its value space is
        any sequence of characters. Its siblings do not - see the note above
        `_COERCERS` for why `xsd:NCName`, `xsd:token` and the rest report False
        despite being handled. False for an XSD type with no implementation
        (`xsd:hexBinary`, `xsd:duration`, `xsd:gMonthDay`), and False for a non-XSD
        IRI - which `coerce_literal` refuses outright, so nothing is stored
        unvalidated and there is nothing to warn about.
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
