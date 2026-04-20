# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
from typing import Dict, List, Any, Tuple
from graphrag_toolkit.lexical_graph.versioning import VERSIONING_METADATA_KEYS

def remove_collection_items_from_metadata(metadata:Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    clean_metadata = {}
    invalid_items = {}
    for k,v in metadata.items():
        if isinstance(v, list):
            invalid_items[k] = v
        elif isinstance(v, dict):
            invalid_items[k] = v
        elif isinstance(v, set):
            invalid_items[k] = v
        else:
            clean_metadata[k] = v
    return (clean_metadata, invalid_items)

def get_properties_str(properties, default):
    if properties:
        return ';'.join(sorted([f'{k}:{v}' for k,v in properties.items()]))
    else:
        return default
        
def last_accessed_date(*args):
    return {
        'last_accessed_date': datetime.datetime.now().strftime("%Y-%m-%d")
    }