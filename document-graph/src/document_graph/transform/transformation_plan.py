# Copyright (c) Evan Erwee. All rights reserved.

"""Transformation plan models for document graph processing.

This module defines the data models used to represent transformation plans
and steps for document graph processing pipelines. Transformation plans
describe a sequence of operations to be applied to documents or datasets
during processing, such as normalization, enrichment, and truncation.
"""

import logging
from typing import List, Literal, Optional, Union
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

class TransformationStep(BaseModel):
    """One step in a transformation plan.
    
    Represents a single operation in a document transformation pipeline,
    such as normalization, enrichment, or truncation.
    
    Attributes:
        type: The type of transformation step (transformers, normalizer, enricher, truncator)
        name: The registered name or class of the step
        args: Optional arguments for configuring the step
        
    Examples:
        >>> normalizer_step = TransformationStep(
        ...     type="normalizer",
        ...     name="text_normalizer",
        ...     args={"lowercase": True, "remove_punctuation": True}
        ... )
    """
    type: Literal["transformers", "normalizer", "enricher", "truncator"]
    name: str = Field(..., description="The registered name or class of the step")
    args: Optional[dict] = Field(default_factory=dict, description="Optional kwargs for the step")

class TransformationPlan(BaseModel):
    """A full transformation pipeline plan for a document or dataset.
    
    Represents a complete sequence of transformation operations to be applied
    to documents or datasets during processing. Each plan has a unique ID,
    an optional description, and an ordered list of transformation steps.
    
    Attributes:
        id: Unique identifier for the transformation plan
        description: Optional human-readable description of the plan
        steps: Ordered list of transformation steps to apply
        
    Examples:
        >>> plan = TransformationPlan(
        ...     id="doc-processing-plan",
        ...     description="Standard document processing pipeline",
        ...     steps=[
        ...         TransformationStep(type="normalizer", name="text_normalizer", 
        ...                           args={"lowercase": True}),
        ...         TransformationStep(type="enricher", name="entity_enricher", 
        ...                           args={"entities": ["person", "organization"]})
        ...     ]
        ... )
    """
    id: str
    description: Optional[str] = None
    steps: List[TransformationStep] = Field(..., description="Ordered list of transformation steps to apply")

    def get_steps_by_type(self, step_type: str) -> List[TransformationStep]:
        """Filter steps by their type.
        
        Args:
            step_type: The type of steps to filter for (e.g., "normalizer", "enricher")
            
        Returns:
            List of transformation steps matching the specified type
            
        Examples:
            >>> plan = TransformationPlan(
            ...     id="doc-processing-plan",
            ...     steps=[
            ...         TransformationStep(type="normalizer", name="text_normalizer"),
            ...         TransformationStep(type="enricher", name="entity_enricher")
            ...     ]
            ... )
            >>> normalizers = plan.get_steps_by_type("normalizer")
            >>> len(normalizers)
            1
            >>> normalizers[0].name
            'text_normalizer'
        """
        return [step for step in self.steps if step.type == step_type]
