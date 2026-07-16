# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Guards against silent drift between the Classic and NextGen Neptune DB + OpenSearch
Serverless CloudFormation templates.

The NextGen template started as a copy of the Classic one with a small, deliberate delta
(a CollectionGroup resource, three fields on the Collection resource). CloudFormation has
no composition primitive that would let them share the VPC/Neptune/SageMaker resources
without a nested-stack rewrite, so the duplication is accepted; this test enforces that
any future fix to the shared resources gets applied to both files.
"""

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATES_DIR = REPO_ROOT / "examples" / "lexical-graph" / "cloudformation-templates"
CLASSIC_TEMPLATE = TEMPLATES_DIR / "graphrag-toolkit-neptune-db-opensearch-serverless.json"
NEXTGEN_TEMPLATE = TEMPLATES_DIR / "graphrag-toolkit-neptune-db-opensearch-nextgen-serverless.json"

# The only resource NextGen adds outright.
INTENTIONAL_NEW_RESOURCES = {"OpenSearchServerlessCollectionGroup"}

# The only shared resource allowed to differ, and the only fields it may differ on.
INTENTIONAL_MODIFIED_RESOURCE = "OpenSearchServerless"
INTENTIONAL_MODIFIED_FIELDS = {"DependsOn", "CollectionGroupName", "StandbyReplicas"}

# Top-level sections that must be byte-identical between the two templates.
SHARED_TOP_LEVEL_SECTIONS = ("Parameters", "Rules", "Metadata", "Conditions", "Outputs")


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


# Loaded once at import time; both files are read-only fixtures for this whole module.
CLASSIC = _load(CLASSIC_TEMPLATE)
NEXTGEN = _load(NEXTGEN_TEMPLATE)


def test_shared_top_level_sections_are_identical():
    classic = CLASSIC
    nextgen = NEXTGEN

    for section in SHARED_TOP_LEVEL_SECTIONS:
        assert classic.get(section) == nextgen.get(section), (
            f"'{section}' has drifted between the Classic and NextGen templates. "
            f"If this is an intentional NextGen-specific change, add it to "
            f"SHARED_TOP_LEVEL_SECTIONS' exceptions in this test; otherwise the change "
            f"needs to be applied to both templates."
        )


def test_only_the_collection_group_resource_is_new_in_nextgen():
    classic = CLASSIC
    nextgen = NEXTGEN

    added = set(nextgen["Resources"]) - set(classic["Resources"])
    removed = set(classic["Resources"]) - set(nextgen["Resources"])

    assert added == INTENTIONAL_NEW_RESOURCES
    assert not removed, f"NextGen template is missing resources present in Classic: {removed}"


def test_shared_resources_are_identical_except_the_known_delta():
    classic = CLASSIC
    nextgen = NEXTGEN

    shared_resource_names = set(classic["Resources"]) & set(nextgen["Resources"])

    for name in shared_resource_names:
        classic_resource = classic["Resources"][name]
        nextgen_resource = nextgen["Resources"][name]

        if name != INTENTIONAL_MODIFIED_RESOURCE:
            assert classic_resource == nextgen_resource, (
                f"Resource '{name}' has drifted between the Classic and NextGen "
                f"templates. Apply the change to both, or if it's an intentional "
                f"NextGen-only difference, update this test."
            )
            continue

        classic_props = classic_resource.get("Properties", {})
        nextgen_props = nextgen_resource.get("Properties", {})

        changed_top_level_keys = {
            k for k in set(classic_resource) | set(nextgen_resource)
            if classic_resource.get(k) != nextgen_resource.get(k) and k != "Properties"
        }
        changed_property_keys = {
            k for k in set(classic_props) | set(nextgen_props)
            if classic_props.get(k) != nextgen_props.get(k)
        }

        unexpected = (changed_top_level_keys | changed_property_keys) - INTENTIONAL_MODIFIED_FIELDS
        assert not unexpected, (
            f"'{INTENTIONAL_MODIFIED_RESOURCE}' differs on unexpected field(s) {unexpected} "
            f"between the Classic and NextGen templates. Only {INTENTIONAL_MODIFIED_FIELDS} "
            f"are expected to differ."
        )
