# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Any

def first_non_none(items:List[Any]):
    return next((item for item in items if item is not None), None)