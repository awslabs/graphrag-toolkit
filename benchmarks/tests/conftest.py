# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Benchmarks test configuration.

Mirrors lexical-graph/tests/conftest.py so HYPOTHESIS_PROFILE=ci reaches the
property-based tests under benchmarks/tests too. Without it the workflow's
env var is inert and the tests run at the default 100 examples with a 200 ms
deadline, which is where a slow runner turns them flaky.

Activate with:  HYPOTHESIS_PROFILE=ci pytest ...
"""

import os
from hypothesis import settings, HealthCheck

settings.register_profile(
    "default",
    max_examples=100,
)

settings.register_profile(
    "ci",
    max_examples=20,
    deadline=5000,  # 5 seconds per example
    suppress_health_check=[HealthCheck.too_slow],
)

# Load the profile selected by env var, falling back to "default"
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))
