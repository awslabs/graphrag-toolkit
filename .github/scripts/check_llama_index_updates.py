#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Check PyPI for llama-index-* releases newer than our pinned floors.

Reads the requirements.txt files listed in the REQUIREMENTS_FILES env var
(whitespace-separated), extracts every ``llama-index*`` requirement and its
``>=`` floor, queries PyPI for the latest release of each, and writes a markdown
report of the packages that have a newer version available.

Outputs (written to $GITHUB_OUTPUT when running under Actions):
    has_updates : "true" if at least one package has a newer release, else "false"

The markdown report is written to $GITHUB_WORKSPACE/llama_index_report.md.

This is a best-effort nudge: a network or parse error for a single package is
logged and skipped rather than failing the job.
"""

import json
import os
import re
import sys
import urllib.request
import urllib.error

try:
    from packaging.version import Version, InvalidVersion
except ImportError:  # pragma: no cover - packaging ships with pip on the runner
    print("packaging not available; install it in the workflow step", file=sys.stderr)
    raise

# Matches e.g. "llama-index-llms-bedrock-converse>=0.15.0". Only >= floors are
# tracked because that is the pinning style used across the toolkit.
REQ_LINE = re.compile(r'^(llama-index[a-z0-9-]*)\s*>=\s*([0-9][0-9A-Za-z.\-]*)\s*$')

PYPI_URL = 'https://pypi.org/pypi/{pkg}/json'


def latest_version(pkg: str) -> str:
    """Return the latest (info.version) release of a package on PyPI."""
    with urllib.request.urlopen(PYPI_URL.format(pkg=pkg), timeout=30) as resp:
        data = json.load(resp)
    return data['info']['version']


def collect_requirements(files):
    """Map each llama-index package to its lowest pinned floor and the files it appears in."""
    found = {}
    for path in files:
        if not path or not os.path.exists(path):
            print(f'skipping missing requirements file: {path}', file=sys.stderr)
            continue
        with open(path, encoding='utf-8') as fh:
            for raw in fh:
                match = REQ_LINE.match(raw.strip())
                if not match:
                    continue
                pkg, floor = match.group(1), match.group(2)
                entry = found.setdefault(pkg, {'floor': floor, 'files': set()})
                entry['files'].add(path)
                # Keep the lowest floor seen across files (most conservative).
                try:
                    if Version(floor) < Version(entry['floor']):
                        entry['floor'] = floor
                except InvalidVersion:
                    pass
    return found


TEST_COMMANDS = """### Suggested tests to run before merging a bump

```bash
# lexical-graph unit tests
cd lexical-graph && PYTHONPATH=src python -m pytest tests/

# byokg-rag unit tests
cd byokg-rag && PYTHONPATH=src python -m pytest tests/
```

Pay particular attention to the Bedrock / LLM paths, which are most sensitive to
llama-index-llms-bedrock-converse and llama-index-llms-anthropic changes:

```bash
cd lexical-graph && PYTHONPATH=src python -m pytest \\
  tests/unit/indexing/utils/test_batch_inference_utils.py \\
  tests/unit/indexing/utils/test_batch_inference_aws.py \\
  tests/unit/utils/test_llm_cache.py \\
  tests/unit/utils/test_llm_cache_cross_region.py
```
"""


def build_report(updates):
    """Render the markdown issue body for the packages that have newer releases."""
    lines = [
        'The following LlamaIndex dependencies have newer releases on PyPI than '
        'the floors pinned in this repo. Dependabot opens the actual bump PRs; '
        'this issue tracks awareness and the tests to run.',
        '',
        '| Package | Pinned floor | Latest on PyPI | Requirements file(s) |',
        '| --- | --- | --- | --- |',
    ]
    for pkg in sorted(updates):
        info = updates[pkg]
        files = '<br>'.join(sorted(info['files']))
        lines.append(f"| `{pkg}` | {info['floor']} | **{info['latest']}** | {files} |")
    lines.extend(['', TEST_COMMANDS])
    return '\n'.join(lines)


def set_output(name: str, value: str) -> None:
    out = os.environ.get('GITHUB_OUTPUT')
    if not out:
        return
    with open(out, 'a', encoding='utf-8') as fh:
        fh.write(f'{name}={value}\n')


def main() -> int:
    files = os.environ.get('REQUIREMENTS_FILES', '').split()
    requirements = collect_requirements(files)
    if not requirements:
        print('no llama-index requirements found', file=sys.stderr)
        set_output('has_updates', 'false')
        return 0

    updates = {}
    for pkg, info in sorted(requirements.items()):
        try:
            latest = latest_version(pkg)
        except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError) as exc:
            print(f'could not fetch {pkg} from PyPI: {exc}', file=sys.stderr)
            continue
        try:
            is_newer = Version(latest) > Version(info['floor'])
        except InvalidVersion:
            print(f'could not compare versions for {pkg}: {info["floor"]} vs {latest}', file=sys.stderr)
            continue
        status = 'newer available' if is_newer else 'up to date'
        print(f'{pkg}: floor {info["floor"]} -> latest {latest} ({status})')
        if is_newer:
            updates[pkg] = {**info, 'latest': latest}

    if not updates:
        set_output('has_updates', 'false')
        print('all llama-index dependencies are up to date')
        return 0

    report = build_report(updates)
    report_path = os.path.join(os.environ.get('GITHUB_WORKSPACE', '.'), 'llama_index_report.md')
    with open(report_path, 'w', encoding='utf-8') as fh:
        fh.write(report)
    set_output('has_updates', 'true')
    print(f'{len(updates)} package(s) have newer releases; report written to {report_path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
