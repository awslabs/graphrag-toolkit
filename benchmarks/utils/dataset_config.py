# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared dataset configuration for benchmark scripts.

QA_FILE_MAP defines, for each dataset key:
  - files: list of QA JSON filenames to load
  - parent (optional): the parent dataset whose directory is shared on disk/S3

Splits like 'pga_bio' declare a 'parent' so that S3 sync and file loading
resolve to the correct shared subdirectory.
"""

from typing import Dict, Any

QA_FILE_MAP: Dict[str, Dict[str, Any]] = {
    'cuad': {'files': ['qa.json']},
    'cuad-prototype': {'files': ['qa.json']},
    'pga': {'files': ['pga_bio.json', 'pga_stat.json']},
    'pga_bio': {'files': ['pga_bio.json'], 'parent': 'pga'},
    'pga_stat': {'files': ['pga_stat.json'], 'parent': 'pga'},
    'concurrentqa': {'files': ['qa.json']},
    'concurrentqa-prototype': {'files': ['qa.json']},
    'wikihow': {'files': ['qa.json']},
}


def get_data_subdir(dataset: str) -> str:
    """Resolve the filesystem subdirectory for a dataset.

    Splits (e.g. 'pga_bio') declare a 'parent' in QA_FILE_MAP and share
    their parent's data directory on disk and in S3.
    """
    entry = QA_FILE_MAP.get(dataset, {})
    return entry.get('parent', dataset)
