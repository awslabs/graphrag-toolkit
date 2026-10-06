# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

TOPICS_KEY = 'aws::graph::topics'
PROPOSITIONS_KEY= 'aws::graph::propositions'
SOURCE_DOC_KEY = 'aws::graph::source_doc'

# Property on a __Source__ node holding the source hash: the untruncated digests the
# source id is a prefix of. Two documents sharing an id but not this value are two
# different documents.
SOURCE_HASH_PROPERTY = 'sourceHash'
SOURCE_HASH_PARAM = '__source_hash__'

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





