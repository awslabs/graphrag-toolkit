# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

INDEX_KEY = 'aws::graph::index'
ALL_EMBEDDING_INDEXES = ['chunk', 'statement', 'topic']
DEFAULT_EMBEDDING_INDEXES = ['chunk', 'statement']
LEXICAL_GRAPH_LABELS = [
    '__Source__',
    '__Chunk__',
    '__Topic__',
    '__Statement__',
    '__Fact__',
    '__Entity__',
    '__SYS_SV__EntityClassification__',
    '__SYS_SV__StatementTopic__',
    '__SYS_Class__',
    '__SYS_Config__'
]

