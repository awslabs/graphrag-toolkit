# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for generate_changelog.py.

Most tests exercise the pure functions and need no git. TestPreviousReleaseTag
builds a throwaway git repo in a tmp dir to lock in the baseline-selection logic
(same-project tags only, .dev prereleases skipped, current ref excluded).
"""

import os
import subprocess

import generate_changelog as gc


class TestBucketForPath:
    def test_lexical_graph_folder(self):
        assert gc.bucket_for_path('lexical-graph/src/foo.py') == 'lexical'

    def test_lexical_graph_contrib_folder(self):
        # lexical-graph-contrib/ must not be mistaken for shared: it is not
        # under 'lexical-graph/'.
        assert gc.bucket_for_path('lexical-graph-contrib/bar.py') == 'lexical'

    def test_lexical_examples(self):
        assert gc.bucket_for_path('examples/lexical-graph/notebooks/a.ipynb') == 'lexical'
        assert gc.bucket_for_path('examples/lexical-graph-hybrid-dev/x.ipynb') == 'lexical'

    def test_byokg_folder(self):
        assert gc.bucket_for_path('byokg-rag/src/foo.py') == 'byokg'

    def test_byokg_examples(self):
        assert gc.bucket_for_path('examples/byokg-rag/nb.ipynb') == 'byokg'

    def test_shared_paths(self):
        for path in ('docs-site/x.md', 'benchmarks/y.py', 'integration-tests/z.sh',
                     '.github/workflows/w.yml', 'README.md', 'images/logo.png'):
            assert gc.bucket_for_path(path) == 'shared'


class TestClassifyCommit:
    def test_only_lexical(self):
        assert gc.classify_commit(['lexical-graph/a.py', 'lexical-graph/b.py']) == gc.LEXICAL

    def test_only_byokg(self):
        assert gc.classify_commit(['byokg-rag/a.py']) == gc.BYOKG

    def test_both_when_touching_each(self):
        assert gc.classify_commit(['lexical-graph/a.py', 'byokg-rag/b.py']) == gc.BOTH

    def test_lexical_plus_shared_is_lexical(self):
        assert gc.classify_commit(['lexical-graph/a.py', 'README.md']) == gc.LEXICAL

    def test_byokg_plus_shared_is_byokg(self):
        assert gc.classify_commit(['byokg-rag/a.py', '.github/x.yml']) == gc.BYOKG

    def test_only_shared_is_both(self):
        assert gc.classify_commit(['docs-site/x.md', '.github/x.yml']) == gc.BOTH

    def test_no_files_is_both(self):
        assert gc.classify_commit([]) == gc.BOTH


class TestIsRelevant:
    def test_both_project_includes_everything(self):
        assert all(gc.is_relevant(label, gc.BOTH) for label in (gc.LEXICAL, gc.BYOKG, gc.BOTH))

    def test_lexical_includes_lexical_and_both_only(self):
        assert gc.is_relevant(gc.LEXICAL, gc.LEXICAL)
        assert gc.is_relevant(gc.BOTH, gc.LEXICAL)
        assert not gc.is_relevant(gc.BYOKG, gc.LEXICAL)

    def test_byokg_includes_byokg_and_both_only(self):
        assert gc.is_relevant(gc.BYOKG, gc.BYOKG)
        assert gc.is_relevant(gc.BOTH, gc.BYOKG)
        assert not gc.is_relevant(gc.LEXICAL, gc.BYOKG)


class TestParseGitLog:
    def _record(self, sha, short, subject, files):
        header = gc._FLD.join([sha, short, subject])
        body = ''.join('\n' + f for f in files)
        return gc._REC + header + '\n' + body

    def test_parses_multiple_commits(self):
        text = (
            self._record('aaa', 'aaa1', 'Fix lexical bug (#1)', ['lexical-graph/a.py'])
            + self._record('bbb', 'bbb2', 'Shared CI tweak', ['.github/x.yml'])
        )
        commits = gc.parse_git_log(text)
        assert len(commits) == 2
        assert commits[0].sha == 'aaa'
        assert commits[0].subject == 'Fix lexical bug (#1)'
        assert commits[0].files == ['lexical-graph/a.py']
        assert commits[1].files == ['.github/x.yml']

    def test_commit_with_no_files(self):
        text = gc._REC + gc._FLD.join(['ccc', 'ccc3', 'Empty commit']) + '\n'
        commits = gc.parse_git_log(text)
        assert len(commits) == 1
        assert commits[0].files == []

    def test_empty_input(self):
        assert gc.parse_git_log('') == []


class TestRender:
    def _commits(self):
        return [
            gc.Commit('a', 'a1', 'Lexical feature (#10)', ['lexical-graph/a.py']),
            gc.Commit('b', 'b2', 'BYOKG fix (#11)', ['byokg-rag/b.py']),
            gc.Commit('c', 'c3', 'Bump shared dep', ['.github/x.yml']),
        ]

    def test_lexical_report_excludes_byokg_only(self):
        out = gc.render(self._commits(), gc.LEXICAL, 'graphrag-lexical-graph/v1.0.0')
        assert 'Lexical feature (#10)' in out
        assert 'Bump shared dep' in out          # 'both' commit ships in the release
        assert 'BYOKG fix' not in out
        assert 'since graphrag-lexical-graph/v1.0.0' in out

    def test_byokg_report_excludes_lexical_only(self):
        out = gc.render(self._commits(), gc.BYOKG, None)
        assert 'BYOKG fix (#11)' in out
        assert 'Bump shared dep' in out
        assert 'Lexical feature' not in out

    def test_both_report_has_three_sections(self):
        out = gc.render(self._commits(), gc.BOTH, None)
        assert '### lexical-graph' in out
        assert '### byokg' in out
        assert '### Shared / both projects' in out

    def test_pr_ref_kept_and_sha_added_when_absent(self):
        commits = [
            gc.Commit('a', 'a1', 'Has PR ref (#10)', ['lexical-graph/a.py']),
            gc.Commit('b', 'b2', 'No PR ref', ['lexical-graph/b.py']),
        ]
        out = gc.render(commits, gc.LEXICAL, None)
        assert '- Has PR ref (#10)\n' in out
        assert '- No PR ref (b2)' in out

    def test_empty_relevant_shows_no_changes(self):
        commits = [gc.Commit('b', 'b2', 'BYOKG only', ['byokg-rag/b.py'])]
        out = gc.render(commits, gc.LEXICAL, None)
        assert '_No changes._' in out


def _git(repo, *args, date=None):
    """Run git in `repo` with a fixed identity and (optionally) a fixed date.

    A fixed date makes tag creatordate ordering deterministic, so
    --sort=-creatordate in previous_release_tag returns a stable result.
    """
    env = {
        **os.environ,
        'GIT_AUTHOR_NAME': 'test', 'GIT_AUTHOR_EMAIL': 'test@example.com',
        'GIT_COMMITTER_NAME': 'test', 'GIT_COMMITTER_EMAIL': 'test@example.com',
    }
    if date:
        env['GIT_AUTHOR_DATE'] = date
        env['GIT_COMMITTER_DATE'] = date
    subprocess.run(['git', *args], cwd=repo, env=env, check=True, capture_output=True, text=True)


def _commit(repo, message, date):
    (repo / 'file.txt').write_text(message)
    _git(repo, 'add', '.', date=date)
    _git(repo, 'commit', '-m', message, date=date)


def _tag(repo, name, date):
    # Annotated tags so creatordate is the (fixed) tagger date.
    _git(repo, 'tag', '-a', name, '-m', name, date=date)


class TestPreviousReleaseTag:
    """git-backed tests for previous_release_tag baseline selection."""

    def _init(self, repo):
        _git(repo, 'init', '-q')

    def test_skips_dev_prerelease_and_other_project(self, tmp_path, monkeypatch):
        repo = tmp_path
        self._init(repo)
        _commit(repo, 'c1', '2020-01-01T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v1.0.0', '2020-01-01T00:00:00')
        _commit(repo, 'c2', '2020-01-02T00:00:00')
        # A .dev prerelease and an other-project tag must both be ignored for lexical.
        _tag(repo, 'graphrag-lexical-graph/v1.1.0.dev1', '2020-01-02T00:00:00')
        _tag(repo, 'graphrag-byokg/v2.0.0', '2020-01-02T00:00:01')
        _commit(repo, 'c3', '2020-01-03T00:00:00')
        monkeypatch.chdir(repo)

        assert gc.previous_release_tag('lexical-graph', 'HEAD') == 'graphrag-lexical-graph/v1.0.0'
        assert gc.previous_release_tag('byokg', 'HEAD') == 'graphrag-byokg/v2.0.0'

    def test_picks_most_recent_release(self, tmp_path, monkeypatch):
        repo = tmp_path
        self._init(repo)
        _commit(repo, 'c1', '2020-01-01T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v1.0.0', '2020-01-01T00:00:00')
        _commit(repo, 'c2', '2020-01-02T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v1.2.0', '2020-01-02T00:00:00')
        _commit(repo, 'c3', '2020-01-03T00:00:00')
        monkeypatch.chdir(repo)

        assert gc.previous_release_tag('lexical-graph', 'HEAD') == 'graphrag-lexical-graph/v1.2.0'
        # 'both' considers either project's tags.
        assert gc.previous_release_tag('both', 'HEAD') == 'graphrag-lexical-graph/v1.2.0'

    def test_excludes_the_ref_being_released(self, tmp_path, monkeypatch):
        repo = tmp_path
        self._init(repo)
        _commit(repo, 'c1', '2020-01-01T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v1.0.0', '2020-01-01T00:00:00')
        _commit(repo, 'c2', '2020-01-02T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v2.0.0', '2020-01-02T00:00:00')
        monkeypatch.chdir(repo)

        # Releasing v2.0.0 should diff against the prior release, not itself.
        assert gc.previous_release_tag(
            'lexical-graph', 'graphrag-lexical-graph/v2.0.0'
        ) == 'graphrag-lexical-graph/v1.0.0'

    def test_none_when_no_release_tag(self, tmp_path, monkeypatch):
        repo = tmp_path
        self._init(repo)
        _commit(repo, 'c1', '2020-01-01T00:00:00')
        monkeypatch.chdir(repo)

        assert gc.previous_release_tag('lexical-graph', 'HEAD') is None

    def test_non_ancestor_release_is_still_the_baseline(self, tmp_path, monkeypatch):
        """A release tagged on a divergent branch (squash-downmerge) must still be
        chosen — not skipped in favour of an older ancestor tag."""
        repo = tmp_path
        _git(repo, 'init', '-q', '-b', 'main')
        _commit(repo, 'c1', '2020-01-01T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v1.0.0', '2020-01-01T00:00:00')
        # v1.1.0 is tagged on a release branch that never merges back linearly.
        _git(repo, 'checkout', '-q', '-b', 'release')
        _commit(repo, 'r1', '2020-01-02T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v1.1.0', '2020-01-02T00:00:00')
        _git(repo, 'checkout', '-q', 'main')
        _commit(repo, 'c2', '2020-01-03T00:00:00')  # HEAD not descended from v1.1.0
        monkeypatch.chdir(repo)

        # v1.1.0 is NOT an ancestor of HEAD, but it is the most recent release, so
        # it must be the baseline (not the older, ancestor v1.0.0).
        assert gc.previous_release_tag('lexical-graph', 'HEAD') == 'graphrag-lexical-graph/v1.1.0'

    def test_both_skips_same_commit_sibling_tag(self, tmp_path, monkeypatch):
        """For `both`, the sibling project's tag on the same release commit must
        not be chosen as the baseline (that would yield an empty changelog)."""
        repo = tmp_path
        _git(repo, 'init', '-q', '-b', 'main')
        _commit(repo, 'c1', '2020-01-01T00:00:00')
        _tag(repo, 'graphrag-lexical-graph/v1.0.0', '2020-01-01T00:00:00')
        _tag(repo, 'graphrag-byokg/v1.0.0', '2020-01-01T00:00:01')
        _commit(repo, 'c2', '2020-01-02T00:00:00')
        # The release commit carries both projects' tags (identical commit).
        _tag(repo, 'graphrag-lexical-graph/v1.1.0', '2020-01-02T00:00:00')
        _tag(repo, 'graphrag-byokg/v1.1.0', '2020-01-02T00:00:01')
        monkeypatch.chdir(repo)

        baseline = gc.previous_release_tag('both', 'graphrag-lexical-graph/v1.1.0')
        assert baseline in ('graphrag-lexical-graph/v1.0.0', 'graphrag-byokg/v1.0.0')
