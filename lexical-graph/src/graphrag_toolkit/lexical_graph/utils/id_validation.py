# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re

# An allowlist (vs blocking specific separators) closes URL-encoding, Unicode,
# and double-encoding bypasses in one check. IdGenerator ids
# (aws::<hex>:<hex>:<hex>, optional tenant segment) fit.
_ID_SEGMENT_PATTERN = re.compile(r'[A-Za-z0-9:._-]+')


def validate_id_segment(value:str, name:str) -> None:
    """
    Reject an id that would not stay inside the path or key segment it names.

    Callers join ids onto a prepared directory or a collection prefix, so an id
    holding a separator opens a new segment and a climbing id lands anywhere.
    """
    if not value or not value.strip():
        raise ValueError(f'{name} must be a non-empty string.')
    if value.strip() in ('.', '..'):
        raise ValueError(f'{name} must not name a directory: {value!r}')
    if not _ID_SEGMENT_PATTERN.fullmatch(value):
        raise ValueError(
            f'{name} contains invalid characters (allowed: alphanumeric, colon, '
            f'period, underscore, hyphen): {value!r}'
        )
