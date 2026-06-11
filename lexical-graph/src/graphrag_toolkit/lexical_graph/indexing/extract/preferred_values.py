# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Union
from graphrag_toolkit.core.types import Node
from graphrag_toolkit.core.compat import BaseComponent, BaseNode

logger = logging.getLogger(__name__)

class PreferredValuesProvider(BaseComponent):

    def __call__(self, node:BaseNode) -> List[str]:
        pass

PREFERRED_VALUES_PROVIDER_TYPE = Union[List[str], PreferredValuesProvider]

class DefaultPreferredValues(PreferredValuesProvider):
    values:List[str]
    def __call__(self, node:BaseNode) -> List[str]:
        return self.values

def default_preferred_values(values:List[str]) -> PreferredValuesProvider:
    return DefaultPreferredValues(values=values)


