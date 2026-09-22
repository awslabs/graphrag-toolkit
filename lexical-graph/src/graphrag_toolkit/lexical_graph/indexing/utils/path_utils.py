# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

def validate_id(value:str, name:str) -> None:
    """
    Reject an id that would write outside the directory a sink prepared.

    Every file sink joins an id onto a prepared directory, so a separator in the
    id opens a new path segment and a climbing id lands anywhere. IdRewriter
    returns any id already starting ``aws:`` as given, so an id such as
    ``aws:../../etc/x`` reaches a sink untouched; call this before the join.
    Rewritten ids are IdGenerator hashes and carry no separator.
    """
    if not value or not value.strip():
        raise ValueError(f'{name} must be a non-empty string.')
    if '/' in value or '\\' in value:
        raise ValueError(f'{name} must not contain a path separator: {value!r}')
    if any(ord(c) < 0x20 or ord(c) == 0x7f for c in value):
        raise ValueError(f'{name} must not contain control characters: {value!r}')
    if value.strip() in ('.', '..'):
        raise ValueError(f'{name} must not name a directory: {value!r}')
