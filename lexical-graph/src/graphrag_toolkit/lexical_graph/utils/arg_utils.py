# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Any

def coalesce(*items):
    return next((item for item in items if item is not None), None)