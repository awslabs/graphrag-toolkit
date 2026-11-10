# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Importing this package imports rdflib, via ontology.py. Modules that run
# inside extraction workers - anything pickled across the spawn boundary -
# should import from .ontology_index directly instead, which is plain data and
# rdflib-free.

from .datatype_utils import coerce_literal, validate_literal_against_xsd
from .naming import camel_to_upper_snake, resolution_key, title_case_with_spaces
from .ontology import Ontology, OntologyLoadError
from .ontology_config import (
    ONTOLOGY_AUTHORITY_LEVELS,
    OntologyConfig,
    OntologySource,
    OntologyType,
    ResolvedDimensions,
    OntologyAuthority,
    TypedProperties,
    VocabularyFormat,
    to_ontology_config,
)
from .ontology_filter import FilterCounters, OntologyFilter, authored_name
from .ontology_index import (
    OWL_THING,
    XSD_NAMESPACE,
    DatatypeProperty,
    ObjectProperty,
    OntologyClass,
    OntologyIndex,
)
from .prompt_constraint import (
    PROMPT_CONSTRAINT_LEVELS,
    PROSE_VOCABULARY,
    TURTLE_VOCABULARY,
    VOCABULARY_FORMATS,
    rendered_class_names,
)
