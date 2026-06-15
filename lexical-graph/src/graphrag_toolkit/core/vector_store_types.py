# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Vector store query types and metadata filters."""

from __future__ import annotations

from enum import Enum
from typing import Any, List, Optional, Sequence, Union

from pydantic import BaseModel, Field


class FilterOperator(str, Enum):
    """Vector store filter operator."""

    EQ = "=="
    GT = ">"
    LT = "<"
    NE = "!="
    GTE = ">="
    LTE = "<="
    IN = "in"
    NIN = "nin"
    ANY = "any"
    ALL = "all"
    TEXT_MATCH = "text_match"
    TEXT_MATCH_INSENSITIVE = "text_match_insensitive"
    CONTAINS = "contains"
    IS_EMPTY = "is_empty"


class FilterCondition(str, Enum):
    """Vector store filter conditions to combine different filters."""

    AND = "and"
    OR = "or"
    NOT = "not"


class MetadataFilter(BaseModel):
    """Comprehensive metadata filter for vector stores."""

    key: str
    value: Optional[Any] = None
    operator: FilterOperator = FilterOperator.EQ


class MetadataFilters(BaseModel):
    """Metadata filters for vector stores."""

    filters: List[Union[MetadataFilter, "MetadataFilters"]] = Field(default_factory=list)
    condition: Optional[FilterCondition] = FilterCondition.AND


class VectorStoreQueryMode(str, Enum):
    """Vector store query mode."""

    DEFAULT = "default"
    SPARSE = "sparse"
    HYBRID = "hybrid"
    TEXT_SEARCH = "text_search"
    SEMANTIC_HYBRID = "semantic_hybrid"
    MMR = "mmr"


class VectorStoreQueryResult(BaseModel):
    """Vector store query result."""

    nodes: Optional[Sequence] = None
    similarities: Optional[List[float]] = None
    ids: Optional[List[str]] = None
