# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the idempotent release-notes merge logic."""

from update_release_notes import (
    CHANGELOG_START,
    CHANGELOG_END,
    merge_release_body,
)

CHANGELOG = "## What's changed in byokg\n\n- Fix a thing (#1)\n"


def _block_count(body: str) -> int:
    return body.count(CHANGELOG_START)


class TestMergeReleaseBody:
    def test_appends_block_after_author_notes(self):
        body = merge_release_body('Author notes here.', CHANGELOG)
        assert body.startswith('Author notes here.')
        assert CHANGELOG_START in body and CHANGELOG_END in body
        assert 'Fix a thing (#1)' in body
        # The author's notes precede the generated block.
        assert body.index('Author notes here.') < body.index(CHANGELOG_START)

    def test_empty_body_yields_only_block(self):
        body = merge_release_body('', CHANGELOG)
        assert body == f'{CHANGELOG_START}\n{CHANGELOG.strip()}\n{CHANGELOG_END}\n'

    def test_none_body_is_treated_as_empty(self):
        assert merge_release_body(None, CHANGELOG) == \
            f'{CHANGELOG_START}\n{CHANGELOG.strip()}\n{CHANGELOG_END}\n'

    def test_idempotent_across_reruns(self):
        """Re-running with the same changelog must not duplicate the block."""
        once = merge_release_body('Author notes.', CHANGELOG)
        twice = merge_release_body(once, CHANGELOG)
        assert once == twice
        assert _block_count(twice) == 1

    def test_replaces_stale_block_with_new_changelog(self):
        """A regenerated changelog replaces the previous block, not appends to it."""
        old = merge_release_body('Author notes.', "## Old\n\n- Old entry (#0)\n")
        new = merge_release_body(old, CHANGELOG)
        assert _block_count(new) == 1
        assert 'Old entry (#0)' not in new
        assert 'Fix a thing (#1)' in new
        assert new.startswith('Author notes.')
