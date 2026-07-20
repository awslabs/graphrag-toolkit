"""Extraction result — output of the extract stage."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd

@dataclass
class ExtractionResult:
    """Holds the output of an extraction stage including schema, nodes, and data."""

    document_id: str = ""
    extracted_schema: dict = field(default_factory=dict)
    nodes: list = field(default_factory=list)
    dataframe: Optional[pd.DataFrame] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
