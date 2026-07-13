# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum


class GraphOperation(str, Enum):
    """Backend-neutral lexical-graph operations.

    Query-language-specific stores use these identifiers to select their native
    implementation.
    """

    UPSERT_SOURCE = 'upsert_source'
    UPSERT_CHUNK = 'upsert_chunk'
    LINK_CHUNK_SOURCE = 'link_chunk_source'
    LINK_CHUNKS = 'link_chunks'
    UPSERT_TOPIC = 'upsert_topic'
    UPSERT_STATEMENT = 'upsert_statement'
    LINK_STATEMENT_CHUNK = 'link_statement_chunk'
    LINK_STATEMENT_TOPIC = 'link_statement_topic'
    LINK_STATEMENTS = 'link_statements'
    UPSERT_FACT = 'upsert_fact'
    LINK_FACT_ENTITY = 'link_fact_entity'
    UPSERT_ENTITY = 'upsert_entity'
    LINK_ENTITIES = 'link_entities'
    ADD_ENTITY_TYPE = 'add_entity_type'
    UPDATE_GRAPH_SUMMARY = 'update_graph_summary'
    FIND_COMPLEMENTS = 'find_complements'
    FIND_SUBJECTS = 'find_subjects'
    COPY_COMPLEMENT_RELATIONSHIPS = 'copy_complement_relationships'
    DELETE_COMPLEMENT = 'delete_complement'
    GET_STATEMENTS = 'get_statements'
    GET_FACTS = 'get_facts'
    GET_CHUNKS = 'get_chunks'
    GET_TOPIC = 'get_topic'
    SEARCH_BY_CHUNK = 'search_by_chunk'
    SEARCH_BY_TOPIC = 'search_by_topic'
    FIND_ENTITIES_BY_KEYWORD = 'find_entities_by_keyword'
    FIND_ENTITIES_BY_CHUNKS = 'find_entities_by_chunks'
    FIND_ENTITIES_BY_TOPICS = 'find_entities_by_topics'
    FIND_ENTITY_NEIGHBORS = 'find_entity_neighbors'
    SCORE_ENTITIES = 'score_entities'
    SEARCH_BY_ENTITY = 'search_by_entity'
    SEARCH_BY_ENTITIES = 'search_by_entities'
