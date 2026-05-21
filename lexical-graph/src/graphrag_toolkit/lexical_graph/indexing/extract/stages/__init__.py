# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from .llm_proposition_stage import LLMPropositionStage
from .local_proposition_stage import LocalPropositionStage
from .llm_topic_extraction_stage import LLMTopicExtractionStage
from .schema_filter_stage import SchemaFilterStage
from .ner_extraction_stage import NERExtractionStage
from .entity_merge_stage import EntityMergeStage
from .batch_stages import BatchLLMPropositionStage, BatchTopicExtractionStage

# Strict-mode ontology filter stage — conditional re-export (Requirement
# 14.4, 14.5, NFR-7). The strict-mode module is a separable unit; deleting
# ``ontology_filter_stage.py`` must not raise ImportError when this package
# is imported. The try/except wrapper here turns a missing strict-mode
# module into a no-op: importers who use suggestion mode only get the
# existing exports; importers who add strict mode get ``OntologyFilterStage``
# as a re-export.
try:
    from .ontology_filter_stage import OntologyFilterStage
except ImportError:
    pass  # Strict-mode module absent — suggestion-mode code still works
