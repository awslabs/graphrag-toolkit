# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import logging
from typing import Tuple, List

from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_TOPIC, LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection, Topic, Fact, Entity, Relation, Statement

logger = logging.getLogger(__name__)

def format_text(text):
        if isinstance(text, list):
            return '\n'.join(s for s in text)
        else:
            return text

def format_list(values:List[str]):
    return '\n'.join(values)

def clean(s):
    return remove_parenthetical_content(format_value(s))
    
def format_value(s):
    return s.replace('_', ' ') if s else ''

def format_classification(s):
    return s.title() if s else ''
    
def strip_full_stop(s):
    return s[:-1] if s and s.endswith('.') else s
    
def remove_parenthetical_content(s):
    return re.sub(r'\(.*\)', '', s).replace('  ', ' ').strip()

def parse_extracted_topics(raw_text:str) -> Tuple[TopicCollection, List[str]]:
    
    garbage = []
    current_state = None

    topics = TopicCollection(topics=[])

    current_topic = Topic(value=DEFAULT_TOPIC, facts=[], details=[])
    current_statement:Statement = None
    current_entities = {}

    for line in raw_text.split('\n'):
            
        if not line:
            continue
            
        line = line.strip()

        if line.startswith('topic:'):

            if current_statement and (current_statement.details or current_statement.facts):
                current_topic.statements.append(current_statement)
                    
            if current_entities:
                current_topic.entities = list(current_entities.values())

            if current_topic.entities or current_topic.statements:
                topics.topics.append(current_topic)

            current_state = None
            current_statement = None
            current_entities = {}

            topic_str = format_value(''.join(line.split(':')[1:]).strip())
            topic_str = strip_full_stop(topic_str)

            current_topic = Topic(value=topic_str, facts=[], details=[])
                
            continue
            
        if line.startswith('proposition:'):

            if current_statement and (current_statement.details or current_statement.facts):
                current_topic.statements.append(current_statement)

            statement_str = format_value(''.join(line.split(':')[1:]).strip())
            current_statement = Statement(value=statement_str, facts=[], details=[])

            current_state = 'relationship-extraction'
                
            continue

        elif line.startswith('entities:'):
            current_state = 'entity-extraction'
            continue

        elif line.startswith('entity-') and line.endswith('relationships:'):    
            current_state = 'relationship-extraction'
            continue

        elif current_state and current_state == 'entity-extraction':
            parts = line.split('|')
            if len(parts) == 2:
                entity_raw_value = parts[0]
                entity_clean_value = clean(entity_raw_value)
                entity = Entity(value=entity_clean_value, classification=format_classification(parts[1]))
                if entity_clean_value not in current_entities:
                    current_entities[entity_clean_value] = entity
            else:
                garbage.append(f'UNPARSEABLE ENTITY: {line}')

        elif current_state and current_state == 'relationship-extraction':
            
            parts = line.split('|')
            fact = None
            
            if len(parts) == 3:
                s, p, o = parts
                if s and p and o:
                    s_entity = current_entities.get(clean(s), None)
                    o_entity = current_entities.get(clean(o), None)
                    if s_entity and o_entity:
                        fact = Fact(
                            subject=s_entity,
                            predicate=Relation(value=format_value(p)),
                            object=o_entity
                        )
                    elif s_entity:
                        fact = Fact(
                            subject=s_entity,
                            predicate=Relation(value=format_value(p)),
                            complement=Entity(value=format_value(o), classification=LOCAL_ENTITY_CLASSIFICATION)
                        )
                    else:
                        fact = Fact(
                            subject=Entity(value=format_value(s), classification=LOCAL_ENTITY_CLASSIFICATION),
                            predicate=Relation(value=format_value(p)),
                            complement=Entity(value=format_value(o), classification=LOCAL_ENTITY_CLASSIFICATION)
                        )
                        if current_statement:
                            current_statement.details.append(' '.join([format_value(part) for part in parts]))
                        
                    if current_statement:
                        current_statement.facts.append(fact)

    
            if not fact:
                details = None
                if parts and current_statement:
                    details = ' '.join([format_value(part) for part in parts])
                    if details:
                        current_statement.details.append(details)
                if not details:
                    garbage.append(f'UNPARSEABLE STATEMENT: {line}')

        else:
            garbage.append(f'UNPARSEABLE: {line}')

    if current_topic:
        if current_statement and (current_statement.details or current_statement.facts):
            current_topic.statements.append(current_statement)
                    
        if current_entities:
            current_topic.entities = list(current_entities.values())

        if current_topic.entities or current_topic.statements:
            topics.topics.append(current_topic)
            
    if logger.isEnabledFor(logging.DEBUG):
        s = f"""====================================
raw_text: {raw_text}
------------------------------------
topics: {topics}
------------------------------------
garbage: {garbage}
"""
        logger.debug(s)

    return (topics, garbage)