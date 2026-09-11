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

# Ontology configuration, re-exported so `ExtractionConfig(ontology=...)` and its
# argument come from the same place a user already imports the extractors from.
#
# This pulls rdflib into every process that imports the extract package, workers
# included, which the split between `Ontology` (owns the rdflib graph, parent
# process) and `OntologyIndex` (plain data, crosses the spawn boundary) was set
# up to avoid. Measured before accepting it: `import rdflib` is ~88ms against
# this package's own ~2.9s import, so ~3%, once per worker. Not worth a lazy
# module `__getattr__` and the tooling breakage that comes with one. The split
# still earns its keep for a different reason - it is what keeps `OntologyIndex`
# picklable - and extractors still take a rendered string, not an ontology.
from .ontology import Ontology, OntologyConfig, OntologyLoadError, OntologyType, TypedProperties, to_ontology_config

# The one ontology component that goes into the pipeline. Exported here because
# `_configure_extraction_pipeline` appends it alongside the extractors above, and
# a user reading `extraction_components` should be able to import the type they
# find there from the same place.
from .ontology import OntologyFilter
