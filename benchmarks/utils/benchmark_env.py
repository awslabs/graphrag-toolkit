# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Environment variable readers for the benchmark harness.

Two rules, both learned from runs that went wrong:

Empty means absent. The harness allowlists variables into .env.testing, and an
allowlisted variable that happens to be unset arrives as an empty string rather
than not arriving. int('') then killed every run 27 seconds in, and
os.environ.get(name, 'true') read that empty string as a choice nobody made.

Omitting the default makes the variable required. A benchmark that would
otherwise measure the wrong thing should fail at startup instead of returning a
plausible number.

A supplied default is returned unchanged, so `env_int(name, None)` yields None
for an absent variable rather than coercing it.
"""

from typing import Any, Optional

import os

# Sentinel for "no default", so that None stays available as a real default.
_REQUIRED = object()

# The spellings lexical_graph.config.string_to_bool accepts, so the harness has
# one boolean vocabulary. It cannot be imported here: benchmarks/tests runs
# without graphrag_toolkit.
_TRUE = frozenset({'true'})
_FALSE = frozenset({'false'})


def _raw(name: str, default: Any) -> Optional[str]:
    """Return the stripped value, or None if it is absent and has a default."""
    value = os.environ.get(name)
    if value is not None:
        value = value.strip()

    if value:
        return value

    if default is _REQUIRED:
        raise ValueError(
            f'{name} is not set. The benchmark harness requires it to be stated '
            f'explicitly rather than defaulted.'
        )

    return None


def env_bool(name: str, default: Any = _REQUIRED) -> bool:
    """
    Read a boolean environment variable.

    Args:
        name: Variable to read.
        default: Value to use when unset or empty. Omit to make it required.

    Raises:
        ValueError: The variable is required and absent, or its value is not
            'true' or 'false'. A typo cannot silently invert the setting.
    """
    raw = _raw(name, default)
    if raw is None:
        return default

    lowered = raw.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False

    raise ValueError(
        f'{name}={raw!r} is not a boolean. Use one of: '
        f'{", ".join(sorted(_TRUE | _FALSE))}.'
    )


def env_int(name: str, default: Any = _REQUIRED) -> int:
    """
    Read an integer environment variable.

    Args:
        name: Variable to read.
        default: Value to use when unset or empty. Omit to make it required.

    Raises:
        ValueError: The variable is required and absent, or its value is not an
            integer.
    """
    raw = _raw(name, default)
    if raw is None:
        return default

    try:
        return int(raw)
    except ValueError:
        raise ValueError(f'{name}={raw!r} is not an integer.') from None


def env_string(name: str, default: Any = _REQUIRED) -> str:
    """
    Read a string environment variable.

    Args:
        name: Variable to read.
        default: Value to use when unset or empty. Omit to make it required.

    Raises:
        ValueError: The variable is required and absent.
    """
    raw = _raw(name, default)
    return default if raw is None else raw
