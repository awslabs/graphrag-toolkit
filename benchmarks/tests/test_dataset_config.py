# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from benchmarks.utils.dataset_config import QA_FILE_MAP, get_data_subdir


class TestGetDataSubdir:
    """Tests for get_data_subdir parent resolution logic."""

    def test_pga_bio_resolves_to_pga(self):
        assert get_data_subdir('pga_bio') == 'pga'

    def test_pga_stat_resolves_to_pga(self):
        assert get_data_subdir('pga_stat') == 'pga'

    def test_pga_parent_resolves_to_itself(self):
        assert get_data_subdir('pga') == 'pga'

    def test_cuad_resolves_to_itself(self):
        assert get_data_subdir('cuad') == 'cuad'

    def test_concurrentqa_resolves_to_itself(self):
        assert get_data_subdir('concurrentqa') == 'concurrentqa'

    def test_wikihow_resolves_to_itself(self):
        assert get_data_subdir('wikihow') == 'wikihow'

    def test_unknown_dataset_falls_back_to_itself(self):
        assert get_data_subdir('nonexistent') == 'nonexistent'

    def test_empty_string_falls_back_to_itself(self):
        assert get_data_subdir('') == ''


class TestQAFileMap:
    """Tests for QA_FILE_MAP structure integrity."""

    def test_all_entries_have_files_key(self):
        for key, entry in QA_FILE_MAP.items():
            assert 'files' in entry, f"Entry '{key}' is missing 'files' key"
            assert isinstance(entry['files'], list), f"Entry '{key}' files is not a list"
            assert len(entry['files']) > 0, f"Entry '{key}' has empty files list"

    def test_parent_entries_reference_valid_parent(self):
        for key, entry in QA_FILE_MAP.items():
            if 'parent' in entry:
                parent = entry['parent']
                assert parent in QA_FILE_MAP, (
                    f"Entry '{key}' references parent '{parent}' which is not in QA_FILE_MAP"
                )
                assert 'parent' not in QA_FILE_MAP[parent], (
                    f"Parent '{parent}' of '{key}' is itself a child — no chained parents"
                )
