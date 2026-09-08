#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Idempotently write a per-project changelog block into a GitHub release body.

softprops/action-gh-release with ``append_body: true`` appends the changelog on
every run, so re-running a release from the Actions UI - or a ``released`` event
following a ``prereleased`` one for the same release - would append a second
copy. This script instead reads the release's current body, removes any
previously inserted changelog block (delimited by HTML-comment markers), appends
a freshly generated block, and sets the body via ``gh release edit``. Running it
again produces the same body, and author-written notes outside the markers are
preserved.

Usage:
    update_release_notes.py --tag <release-tag> --changelog <path-to-md>
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
from typing import List, Optional

# HTML comments render invisibly in GitHub release notes, so the markers delimit
# the auto-generated block without showing up in the rendered output.
CHANGELOG_START = '<!-- auto-changelog:start -->'
CHANGELOG_END = '<!-- auto-changelog:end -->'

_BLOCK_RE = re.compile(
    re.escape(CHANGELOG_START) + r'.*?' + re.escape(CHANGELOG_END),
    re.DOTALL,
)


def merge_release_body(current_body: str, changelog: str) -> str:
    """Return the release body with exactly one auto-changelog block.

    Any existing marked block is removed first (so a re-run replaces it rather
    than appending a duplicate), then a fresh block is appended after whatever
    notes the author wrote. Idempotent: feeding the result back in with the same
    changelog yields the same string.
    """
    body = _BLOCK_RE.sub('', current_body or '').rstrip()
    block = f'{CHANGELOG_START}\n{changelog.strip()}\n{CHANGELOG_END}'
    return f'{body}\n\n{block}\n' if body else f'{block}\n'


def _gh(*args: str) -> str:
    return subprocess.run(['gh', *args], check=True, capture_output=True, text=True).stdout


def get_release_body(tag: str) -> str:
    # `.body // ""` makes jq emit an empty string (not "null") for a release
    # created without notes.
    return _gh('release', 'view', tag, '--json', 'body', '--jq', '.body // ""')


def set_release_body(tag: str, body: str) -> None:
    with tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8') as fh:
        fh.write(body)
        notes_path = fh.name
    try:
        _gh('release', 'edit', tag, '--notes-file', notes_path)
    finally:
        os.unlink(notes_path)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--tag', required=True, help='release tag to update')
    parser.add_argument('--changelog', required=True,
                        help='path to the generated changelog markdown')
    args = parser.parse_args(argv)

    with open(args.changelog, encoding='utf-8') as fh:
        changelog = fh.read()

    current_body = get_release_body(args.tag)
    set_release_body(args.tag, merge_release_body(current_body, changelog))
    return 0


if __name__ == '__main__':
    sys.exit(main())
