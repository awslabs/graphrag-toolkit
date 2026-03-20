# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Top-level test configuration.

Registers and loads a Hypothesis 'ci' profile with reduced max_examples
and a per-example deadline so property-based tests complete in a
reasonable time on GitHub Actions runners.

Activate with:  HYPOTHESIS_PROFILE=ci pytest ...
"""

import os
from hypothesis import settings, HealthCheck

settings.register_profile(
    "ci",
    max_examples=20,
    deadline=5000,  # 5 seconds per example
    suppress_health_check=[HealthCheck.too_slow],
)

# Auto-load the ci profile when the env var is set
if os.environ.get("HYPOTHESIS_PROFILE"):
    settings.load_profile(os.environ["HYPOTHESIS_PROFILE"])
