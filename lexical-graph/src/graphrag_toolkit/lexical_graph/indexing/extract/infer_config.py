# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from dataclasses import dataclass
from typing import Optional

class OnExistingClassifications(Enum):
    MERGE_EXISTING = 1
    REPLACE_EXISTING = 2
    RETAIN_EXISTING = 3

@dataclass
class InferClassificationsConfig:
    num_samples:Optional[int]=5
    num_iterations:Optional[int]=1
    on_existing_classifications:Optional[OnExistingClassifications]=OnExistingClassifications.MERGE_EXISTING
    prompt_template:Optional[str]=None