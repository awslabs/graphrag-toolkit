#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Generate a release changelog grouped/filtered by project.

The toolkit is a monorepo that releases ``lexical-graph`` and ``byokg`` one at a
time, but the changelog historically listed every commit since the previous tag
regardless of project. This script classifies each commit in a git range into
``lexical-graph``, ``byokg``, or ``both`` by the top-level folders it changed,
then emits markdown release notes for the requested project.

Classification (by changed path):
    * ``lexical-graph/``, ``lexical-graph-contrib/``, ``examples/lexical-graph*``
      -> lexical-graph
    * ``byokg-rag/``, ``examples/byokg*`` -> byokg
    * anything else (docs-site, benchmarks, integration-tests, .github, root, ...)
      is "shared" and reported under ``both`` since it can affect either package

A commit that touches both packages is ``both``; a commit that touches only
shared paths is also ``both``. A single-project changelog includes that
project's commits plus the ``both`` commits (those changes ship in the release
too).

Range: pass ``--from``/``--to`` explicitly, or let the script pick the previous
released tag of the same project (``.dev`` prereleases are ignored as baselines)
as the start and ``--to`` (default ``HEAD``) as the end.

Examples:
    generate_changelog.py --project lexical-graph
    generate_changelog.py --project byokg --to graphrag-byokg/v3.19.1
    generate_changelog.py --project both --from graphrag-lexical-graph/v3.19.0
"""

import argparse
import subprocess
import sys
from typing import Dict, List, NamedTuple, Optional, Set

LEXICAL = 'lexical-graph'
BYOKG = 'byokg'
BOTH = 'both'
PROJECTS = (LEXICAL, BYOKG, BOTH)

# Top-level path prefixes that map a changed file to a package. Order matters
# only in that the lexical-graph-contrib prefix is listed explicitly (it is not
# under 'lexical-graph/'). str.startswith accepts a tuple of prefixes.
LEXICAL_PREFIXES = ('lexical-graph/', 'lexical-graph-contrib/', 'examples/lexical-graph')
BYOKG_PREFIXES = ('byokg-rag/', 'examples/byokg')

# Human-readable section headings used when rendering the 'both' report.
SECTION_TITLES = {
    LEXICAL: 'lexical-graph',
    BYOKG: 'byokg',
    BOTH: 'Shared / both projects',
}

# Field/record separators for the machine-readable git log format. Using control
# characters avoids collisions with anything in a commit subject.
_REC = '\x1e'   # record separator: one per commit
_FLD = '\x1f'   # field separator: between sha / short-sha / subject


class Commit(NamedTuple):
    sha: str
    short_sha: str
    subject: str
    files: List[str]


def bucket_for_path(path: str) -> str:
    """Map one changed file path to 'lexical', 'byokg', or 'shared'."""
    if path.startswith(LEXICAL_PREFIXES):
        return 'lexical'
    if path.startswith(BYOKG_PREFIXES):
        return 'byokg'
    return 'shared'


def classify_commit(files: List[str]) -> str:
    """Classify a commit by the packages its changed files touch."""
    buckets: Set[str] = {bucket_for_path(p) for p in files}
    has_lexical = 'lexical' in buckets
    has_byokg = 'byokg' in buckets
    if has_lexical and has_byokg:
        return BOTH
    if has_lexical:
        return LEXICAL
    if has_byokg:
        return BYOKG
    # Only shared paths (or no files): affects both packages.
    return BOTH


def is_relevant(label: str, project: str) -> bool:
    """Whether a commit with `label` belongs in `project`'s changelog."""
    if project == BOTH:
        return True
    if project == LEXICAL:
        return label in (LEXICAL, BOTH)
    if project == BYOKG:
        return label in (BYOKG, BOTH)
    raise ValueError(f'unknown project: {project}')


def parse_git_log(text: str) -> List[Commit]:
    """Parse the output of `git log --pretty=... --name-only` into commits."""
    commits: List[Commit] = []
    for record in text.split(_REC):
        record = record.strip('\n')
        if not record:
            continue
        header, _, rest = record.partition('\n')
        sha, short_sha, subject = header.split(_FLD)
        files = [line for line in rest.split('\n') if line.strip()]
        commits.append(Commit(sha=sha, short_sha=short_sha, subject=subject, files=files))
    return commits


def _git(*args: str) -> str:
    result = subprocess.run(
        ['git', *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _is_ancestor(ancestor: str, ref: str) -> bool:
    return subprocess.run(
        ['git', 'merge-base', '--is-ancestor', ancestor, ref],
        capture_output=True,
    ).returncode == 0


def previous_release_tag(project: str, to_ref: str) -> Optional[str]:
    """Find the previous released tag for `project` reachable from `to_ref`.

    Considers tags of the same project (or of either project for `both`),
    ignores `.dev` prereleases as baselines, and returns the most recent one
    that is an ancestor of `to_ref` and not `to_ref` itself. Returns None if
    there is no suitable baseline (then the whole history is used).
    """
    if project == BYOKG:
        patterns = ['graphrag-byokg/v*']
    elif project == LEXICAL:
        patterns = ['graphrag-lexical-graph/v*']
    else:
        patterns = ['graphrag-lexical-graph/v*', 'graphrag-byokg/v*']

    tags: List[str] = []
    for pattern in patterns:
        tags.extend(
            t for t in _git('tag', '--list', pattern, '--sort=-creatordate').splitlines() if t
        )
    # Newest first; drop the current ref and .dev prereleases (a final release
    # should diff against the previous real release, not its own prerelease).
    for tag in tags:
        if tag == to_ref or '.dev' in tag:
            continue
        if _is_ancestor(tag, to_ref):
            return tag
    return None


def read_commits(from_ref: Optional[str], to_ref: str) -> List[Commit]:
    range_spec = f'{from_ref}..{to_ref}' if from_ref else to_ref
    fmt = f'{_REC}%H{_FLD}%h{_FLD}%s'
    out = _git('log', '--no-merges', f'--pretty=format:{fmt}', '--name-only', range_spec)
    return parse_git_log(out)


def _render_entry(commit: Commit) -> str:
    # Squash-merge subjects already carry the "(#123)" PR ref that GitHub
    # auto-links in release notes; add the short sha only when no PR ref present.
    if '(#' in commit.subject:
        return f'- {commit.subject}'
    return f'- {commit.subject} ({commit.short_sha})'


def render(commits: List[Commit], project: str, baseline: Optional[str]) -> str:
    labelled = [(classify_commit(c.files), c) for c in commits]
    relevant = [(label, c) for label, c in labelled if is_relevant(label, project)]

    since = f' since {baseline}' if baseline else ''
    lines: List[str] = []

    if project == BOTH:
        lines.append(f'## Changelog by project{since}')
        for section in (LEXICAL, BYOKG, BOTH):
            entries = [c for label, c in relevant if label == section]
            lines.append('')
            lines.append(f'### {SECTION_TITLES[section]}')
            if entries:
                lines.extend(_render_entry(c) for c in entries)
            else:
                lines.append('_No changes._')
    else:
        lines.append(f"## What's changed in {project}{since}")
        lines.append('')
        if relevant:
            lines.extend(_render_entry(c) for _, c in relevant)
        else:
            lines.append('_No changes._')

    return '\n'.join(lines) + '\n'


def build_changelog(project: str, from_ref: Optional[str], to_ref: str) -> str:
    baseline = from_ref if from_ref else previous_release_tag(project, to_ref)
    commits = read_commits(baseline, to_ref)
    return render(commits, project, baseline)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--project', required=True, choices=PROJECTS,
                        help="which project's changelog to produce")
    parser.add_argument('--from', dest='from_ref', default=None,
                        help='baseline ref (default: previous released tag of the project)')
    parser.add_argument('--to', dest='to_ref', default='HEAD',
                        help='end ref (default: HEAD; in CI pass the release tag)')
    parser.add_argument('--output', default=None,
                        help='write to this file instead of stdout')
    args = parser.parse_args(argv)

    changelog = build_changelog(args.project, args.from_ref, args.to_ref)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as fh:
            fh.write(changelog)
    else:
        sys.stdout.write(changelog)
    return 0


if __name__ == '__main__':
    sys.exit(main())
