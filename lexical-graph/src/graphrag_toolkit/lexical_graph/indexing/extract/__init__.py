# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from .extraction_pipeline import ExtractionPipeline
from .batch_config import BatchConfig
from .llm_proposition_extractor import LLMPropositionExtractor
from .batch_llm_proposition_extractor_sync import BatchLLMPropositionExtractorSync
from .proposition_extractor import PropositionExtractor
from .batch_topic_extractor_sync import BatchTopicExtractorSync
from .topic_extractor import TopicExtractor
from .file_system_tap import FileSystemTap
from .infer_classifications import InferClassifications
from .infer_config import InferClassificationsConfig
from .preferred_values import PREFERRED_VALUES_PROVIDER_TYPE, PreferredValuesProvider, default_preferred_values
from .extraction_stage import ExtractionStage
from .extraction_schema import ExtractionSchema, EntityTypeConfig
from .pipeline_builder import PipelineBuilder
from .stages import LLMPropositionStage, LocalPropositionStage, LLMTopicExtractionStage, SchemaFilterStage, NERExtractionStage, EntityMergeStage, BatchLLMPropositionStage, BatchTopicExtractionStage

# Suggestion-mode surface for ontology-guided extraction (Requirements 5.1,
# 5.4, 14.3, 14.4, NFR-4). Importing these symbols does NOT pull in
# ``rdflib``: ``ontology_schema`` only imports ``rdflib`` lazily inside
# ``OntologySchema.from_turtle`` / ``from_turtle_string``. The strict-mode
# re-export for ``OntologyFilterStage`` lives below behind a conditional
# ``try/except ImportError`` wrapper so that deleting
# ``ontology_filter_stage.py`` leaves this package importable (Requirement
# 14.5, NFR-7).
from .ontology_schema import (
    DatatypeProperty,
    ObjectProperty,
    OntologyClass,
    OntologyLoadError,
    OntologySchema,
)

# Strict-mode ontology filter stage — conditional re-export (Requirement
# 14.4, 14.5, NFR-7). Same pattern as stages/__init__.py; keeps the
# top-level import surface intact when strict mode is excised.
try:
    from .stages.ontology_filter_stage import OntologyFilterStage
except ImportError:
    pass
