# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

TOPICS_KEY = 'aws::graph::topics'
PROPOSITIONS_KEY= 'aws::graph::propositions'
SOURCE_DOC_KEY = 'aws::graph::source_doc'

LOCAL_ENTITY_CLASSIFICATION = '__Local_Entity__'
DEFAULT_TOPIC = 'context'
DEFAULT_CLASSIFICATION = 'unknown'
DEFAULT_ENTITY_CLASSIFICATIONS = [
    'Company',
    'Location',
    'Event',
    'Sports Team',
    'Person',
    'Role',
    'Product',
    'Service',
    'Creative Work',
    'Software',
    'Financial Instrument'
]

# Properties the graph model already owns on `__Entity__`, and which a typed
# attribute property therefore must not be allowed to key.
# `value` is the entity's identity string, `search_str` is what lookup matches
# on, and `class` is its classification - overwriting any of them would not add
# a queryable attribute, it would corrupt the node.
#
# Declared here, with the other graph-model names, rather than in
# `ontology_config.py` or in `entity_graph_builder.py`: both sides need the same
# list (validation rejects the ontology, the builder skips the write as defence
# in depth) and neither of those modules should import the other.
RESERVED_ENTITY_PROPERTIES = ('value', 'search_str', 'class')

# Reserved in addition to the above once complement placement is active, since
# that is the placement which writes them. They are not
# reserved otherwise: nothing writes them, so nothing can be clobbered.
COMPLEMENT_ENTITY_PROPERTIES = ('typed_value', 'datatype')

# Where a coerced attribute value is stored, if anywhere. `OntologyConfig` owns
# the setting and the `TypedProperties` type alias; the accepted values live here
# because the build pipeline and the builders test against them and this module
# is rdflib-free, which the ontology package is not.
TYPED_PROPERTY_PLACEMENTS = ('off', 'subject', 'complement', 'both')

# The two placements, as membership tests rather than as string comparisons
# repeated at each call site. `'both'` is in each of them, which is the whole
# reason these exist rather than `== 'subject'`.
SUBJECT_PLACEMENTS = ('subject', 'both')
COMPLEMENT_PLACEMENTS = ('complement', 'both')

# What `typed_properties` resolves to when nothing asked for anything.
TYPED_PROPERTIES_OFF = 'off'





