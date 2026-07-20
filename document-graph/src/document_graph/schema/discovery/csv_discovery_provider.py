# Copyright (c) Evan Erwee. All rights reserved.
"""CSV discovery provider — discovers schema from CSV file structure."""

import logging
from typing import List, Dict, Any
import pandas as pd

from document_graph.schema.etl_schema_model import (
    ETLSchema, ExtractConfig, TransformConfig, LoadConfig,
    ChunkingConfig, MetadataMapping, EntityExtractionConfig,
    NormalizeConfig, NodeDefinition, RelationshipDefinition
)
from document_graph.schema.discovery.tabular_discovery_base import TabularSchemaDiscoveryProvider

logger = logging.getLogger(__name__)


class CSVSchemaDiscoveryProvider(TabularSchemaDiscoveryProvider):
    """
    Stable CSV schema discovery that ignores batching and reads only headers.

    Key design choices:
    - Remove batching args (skiprows/nrows) so schema isn't batch-dependent.
    - Read only header row (nrows=0) to discover columns quickly & deterministically.
    - If header=None, sample one row to count fields and synthesize column names.
    - Keep on_bad_lines='skip' resilient by forcing engine='python' when needed.
    """

    _BATCH_ARG_KEYS = {"skiprows", "nrows"}  # anything that could make schema batch-specific

    def _sanitize_read_args(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of args safe for schema discovery (no batching)."""
        clean = dict(args or {})
        # Pull out discovery flags
        clean.pop("log_on_extract", None)

        # Remove batch-affecting args
        for k in list(clean.keys()):
            if k in self._BATCH_ARG_KEYS:
                clean.pop(k, None)

        # Be resilient by default
        clean.setdefault("on_bad_lines", "skip")

        # Ensure pandas engine compatibility for on_bad_lines='skip'
        if str(clean.get("on_bad_lines")).lower() == "skip":
            # Pandas requires engine='python' for on_bad_lines='skip' unless already set
            engine = clean.get("engine", "python")
            if engine.lower() != "python":
                logger.warning(
                    "on_bad_lines='skip' requires engine='python' for reliable behavior during discovery; "
                    "overriding engine to 'python'."
                )
                clean["engine"] = "python"

        # Avoid dtype surprises during discovery
        # (Don't set dtype here; let pandas leave them as object when nrows=0)
        return clean

    def _discover_headers(self, read_args: Dict[str, Any]) -> List[str]:
        """
        Determine headers deterministically:
        - If header is provided (e.g., 0), read with nrows=0 and use df.columns.
        - If header=None, read a single row with header=None to count fields and synthesize names.
        """
        # Case 1: header row exists (header is 0 or an int/list)
        header_arg = read_args.get("header", 0)
        try:
            # Fast path: read only the header row structure
            discovery_args = dict(read_args)
            discovery_args["nrows"] = 0
            df = pd.read_csv(self.source, **discovery_args)
            headers = list(df.columns)

            # If user explicitly said header=None, columns will be RangeIndex([]) with nrows=0.
            # In that case, synthesize names from a one-row sample.
            if header_arg is None or len(headers) == 0:
                sample_args = dict(read_args)
                sample_args["header"] = None
                sample_args["nrows"] = 1
                sample_df = pd.read_csv(self.source, **sample_args)
                n_cols = sample_df.shape[1]
                headers = [f"column_{i}" for i in range(n_cols)]
        except Exception as e:
            raise ErrorHandler.csv_parse_error(str(self.source), e)

        if not headers:
            raise ValueError("Validation error")

        return headers

    def discover_schema(self) -> ETLSchema:
        """
        Discover and return an ETL schema from a CSV file, ignoring batching.
        """
        if not self.source.exists():
            raise FileNotFoundError(f"File not found")

        raw_args = dict(self.args or {})
        log_enabled = bool(raw_args.get("log_on_extract", False))

        # Build safe args for discovery
        read_args = self._sanitize_read_args(raw_args)

        # Optional log file
        log_path = self.source.with_suffix(self.source.suffix + ".log") if log_enabled else None

        try:
            headers: List[str] = self._discover_headers(read_args)
            logger.info(f"Discovered columns from CSV: {headers}")

            if log_path:
                with log_path.open("w", encoding="utf-8") as log_file:
                    log_file.write(f"CSV Discovery Log for {self.source.name}\n")
                    log_file.write(f"Discovered columns: {headers}\n")
                    # Show the sanitized args we actually used (without batching)
                    log_file.write(f"Applied pandas.read_csv args (sanitized): {read_args}\n")

        except Exception as e:
            if log_path:
                with log_path.open("a", encoding="utf-8") as log_file:
                    log_file.write("\nException encountered during CSV parsing:\n")
                    log_file.write(str(e))
            raise

        # Construct a stable ETL schema using discovered headers only
        return ETLSchema(
            schema_id=f"discovered-{self.source.stem}",
            description=f"Discovered ETL schema from CSV file: {self.source.name}",
            extract=ExtractConfig(source_type="file", file_type="csv"),
            transform=TransformConfig(
                chunking=ChunkingConfig(strategy="fixed_length", min_length=100),
                metadata_mapping=MetadataMapping(),
                entity_extraction=EntityExtractionConfig(method="ner"),
                normalize=NormalizeConfig()
            ),
            load=LoadConfig(
                document_node=NodeDefinition(type="Document", fields=headers),
                section_node=NodeDefinition(type="Row", fields=headers),
                relationships=[
                    RelationshipDefinition(type="contains", source="document_id", target="row_id")
                ]
            )
        )
