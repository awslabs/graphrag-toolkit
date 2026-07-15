# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify that installed dependencies are compatible.

These tests catch version mismatches (e.g. boto3/botocore) that arise when
transitive dependencies (like aiobotocore via s3fs) constrain one package
without updating its paired counterpart.
"""

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]

INTEG_REQUIREMENTS = PROJECT_ROOT / "integration-tests" / "requirements-integ-test.txt"

BYOKG_RAG_REQUIREMENTS = (
    PROJECT_ROOT / "byokg-rag" / "src" / "graphrag_toolkit"
    / "byokg_rag" / "requirements.txt"
)

LEXICAL_GRAPH_REQUIREMENTS = (
    PROJECT_ROOT / "lexical-graph" / "src" / "graphrag_toolkit"
    / "lexical_graph" / "requirements.txt"
)


def _filter_requirements(path):
    """Read a requirements file, stripping pip options like --only-binary."""
    lines = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped or stripped.startswith("#"):
            continue
        lines.append(stripped)
    return lines


@pytest.fixture(scope="module")
def resolved_versions():
    """Dry-run pip install to resolve final versions for all dependencies.

    Mirrors the single-pass install in run_test_suite.sh which resolves
    all three requirements files together.
    """
    if not INTEG_REQUIREMENTS.exists():
        pytest.skip(f"Integration requirements not found: {INTEG_REQUIREMENTS}")

    packages = _filter_requirements(INTEG_REQUIREMENTS)
    if BYOKG_RAG_REQUIREMENTS.exists():
        packages += _filter_requirements(BYOKG_RAG_REQUIREMENTS)
    if LEXICAL_GRAPH_REQUIREMENTS.exists():
        packages += _filter_requirements(LEXICAL_GRAPH_REQUIREMENTS)

    import tempfile
    import json

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("\n".join(packages))
        tmp_req = f.name

    tmp_report = tmp_req + ".json"

    try:
        result = subprocess.run(
            [
                sys.executable, "-m", "pip", "install",
                "--dry-run", "--quiet", "--ignore-installed",
                "--report", tmp_report,
                "-r", tmp_req,
            ],
            capture_output=True,
            text=True,
        )
    finally:
        Path(tmp_req).unlink(missing_ok=True)

    if result.returncode != 0:
        pytest.fail(
            f"pip dependency resolution failed — dependencies are incompatible:\n"
            f"{result.stderr}"
        )

    report = json.loads(Path(tmp_report).read_text())
    Path(tmp_report).unlink(missing_ok=True)

    versions = {}
    for item in report.get("install", []):
        metadata = item.get("metadata", {})
        name = metadata.get("name", "").lower()
        version = metadata.get("version", "")
        if name and version:
            versions[name] = version

    return versions


class TestIntegDependencyCompatibility:
    """Verify integration test dependencies resolve without conflicts."""

    def test_boto3_botocore_version_alignment(self, resolved_versions):
        """boto3 and botocore must be from the same release."""
        boto3_ver = resolved_versions.get("boto3")
        botocore_ver = resolved_versions.get("botocore")

        if boto3_ver is None or botocore_ver is None:
            pytest.skip("boto3/botocore not found in environment")

        assert boto3_ver == botocore_ver, (
            f"boto3=={boto3_ver} and botocore=={botocore_ver} are mismatched. "
            f"These must be from the same release to avoid ImportError on "
            f"cross-package symbol references."
        )
