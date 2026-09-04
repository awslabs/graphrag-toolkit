# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for generate_changelog.py (pure functions; no git required)."""

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
