# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.lexical_graph.indexing.model import Fact, Entity

def string_complement_to_entity(fact:Fact) -> Fact:
    if isinstance(fact.complement, str):
        value = fact.complement
        fact.complement = Entity(value=value, classification=LOCAL_ENTITY_CLASSIFICATION)
    return fact