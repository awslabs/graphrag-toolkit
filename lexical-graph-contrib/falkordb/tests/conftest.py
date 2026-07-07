# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


def pytest_configure(config):
    """Register the ``integration`` marker so it is known in every run context,
    whether the contrib package is tested standalone or alongside others."""
    config.addinivalue_line(
        'markers',
        'integration: needs a live FalkorDB; skipped automatically when unreachable',
    )
