# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT = """
You are an expert system analyzing text to identify domain-specific entity types. Based on the provided text samples, identify the most significant entity classifications for this domain.

Guidelines:
1. Identify specific types of entities that appear in or are relevant to the domain
2. Use clear, concise classification names (1-2 words)
3. Aim for 10-15 classifications
4. Format each classification as a single word or multiple words separated by a space
5. Focus on concrete entities, not abstract concepts

Sample text chunks:
<chunks>
{text_chunks}
</chunks>

Output the classifications between entity_classifications tags, one per line.

Expected format:
<entity_classifications>
Classification1
Classification2
Classification3
</entity_classifications>
"""

EXTRACT_PROPOSITIONS_PROMPT = """
You are a top-tier algorithm designed for extracting information in structured formats to build a knowledge graph. Your task is to decompose the given text into clear, concise, and context-independent propositions.

# Instructions:
1. Break down complex sentences into simple, atomic statements.
2. Break down lists and tables of information into sets of simple statements.
3. Preserve original phrasing from the input text whenever possible.
4. Add a proposition per named entity that classifies that entity.
5. Maintain named entity consistency by resolving coreferences across all propositions to the most complete identifier for that entity.
6. Isolate descriptive information about named entities into separate propositions.
7. Decontextualize each proposition by:
   a) Adding necessary modifiers to nouns or entire propositions.
   b) Replace any pronouns (e.g., he, she, it, they) with the specific nouns they refer to.  
   c) Replace any acronyms with their full forms.
   d) If the proposition is a fragment, use the other sentences to reconstruct it into a complete, self-explanatory proposition.
   e) Preserve any quoted speech or dialogue as is, without paraphrasing.
   f) If a proposition is already self-explanatory, leave it unchanged.
   g) Do not introduce irrelevant context information in the proposition.
   h) Ensure each proposition can stand alone without external context. 
   i) Do not use your training history, rely only on the context information to enhance the proposition.
8. Capture all relevant details from the original text, including temporal, spatial, and causal relationships.
9. Prioritize completeness and accuracy.

# Output Format:
- Preface the list with a descriptive title that summarizes the content.
- Present each proposition on a new line.
- Ensure each proposition is unique: do not repeat propositions.
- Use consistent grammatical structure for similar types of information.
- Do not include any explanatory text or numbering.
- Ensure propositions are in a logical order, if applicable.

# Response Format:
title
proposition
proposition
proposition
...

Do not provide any other explanatory text. Ensure you have captured all of the details from the text in your response.  

<text>
{text}
</text>
"""

EXTRACT_TOPICS_PROMPT = """
You are a top-tier algorithm designed for extracting information in structured formats to build a knowledge graph.
Your input consists of carefully crafted propositions - simple, atomic, and decontextualized statements. Your task is to:
   1. Organize these propositions into topics
   2. Extract entities and their attributes
   3. Identify relationships between entities

Try to capture as much information from the text as possible without sacrificing accuracy. Do not add any information that is not explicitly mentioned in the input propositions.

## Topic Extraction:
   1. Read the entire set of propositions and then extract a list of specific topics. Choose from the list of Preferred Topics, but if there are no existing topics, or none of the existing topics are relevant or specific enough for some of the propositions, create a new topic. Topic names should provide a clear, highly descriptive summary of the content.  
   2. Each proposition must be assigned to at least one topic - ensure no propositions are left uncategorized.
   3. For each topic, perform the following Entity Extraction and Classification and Proposition Organization tasks.

## Entity Extraction and Classification:
   1. Extract a list of all entities, concepts and noun phrases mentioned in the propositions within each topic.
   2. Classify each extracted entity. Some entity classifications include:
      - Person (e.g., John Doe, Mary Jane)
      - Organization (e.g., Acme Inc., Harvard University)
      - Location (e.g., New York City, Mount Everest)
   3. DO NOT treat numerical values, dates, times, measurements, or object attributes (e.g. size, colour) as entities.
   4. A list of Preferred Entity Classifications is included below. Choose the most specific classification from this list in preference to creating a new classification.
   5. Ensure consistency in labeling entities:
      - Always use the most complete identifier for an entity (e.g., 'John Doe' instead of 'he' or 'John').
      - Maintain entity consistency throughout the knowledge graph by resolving coreferences.
      - If an entity is referred to by different names or pronouns, always use the most complete identifier.
      - If the identifier is an acronym, and you recognize the acronym, use the entity's full name instead of the acronym. DO NOT put the acronym in parentheses after the full name. 
   6. Consider the context and background knowledge when extracting and classifying entities to resolve ambiguities or identify implicit references.
   7. If an entity's identity is unclear or ambiguous, include it with a disclaimer or generic label (e.g., 'unknown_person').
      
## Proposition Organization:
   1. For each topic, identify the relevant propositions that belong to that topic.
   2. Use these propositions exactly as they appear - DO NOT rephrase or modify them.
   3. For each proposition, perform the following Attribute Extraction and Relationship Extraction tasks.

## Attribute Extraction:
   1. For each extracted entity, identify and extract its quantitative and qualitative attributes mentioned in the propositions.
      - Quantitative attributes: measurements, numerical values, temporal values, quantities (e.g., age, height, weight, size, date, time).
      - Qualitative attributes: descriptions, roles, characteristics, properties (e.g., color, occupation, nationality, season).
   2. Represent entity-attribute relationships in the format: entity|RELATIONSHIP|attribute
   3. Ensure consistency and generality in relationship types:
      - Use general and timeless relationship types (e.g., 'VALUE' instead of 'HAD_VALUE').
      - Avoid overly specific or momentary relationship types.
      - Prefer one- or two-word relationship types.
      - Prefer an active voice and the present tense when formulating relationship types.
   4. Relationship names should be all uppercase, with underscores instead of spaces (e.g. 'DESCRIBED_BY')

   Example: John Doe|OCCUPATION|software engineer

## Relationship Extraction:
   1. Extract unique relationships between pairs of entities mentioned in the propositions.
   2. Represent entity-entity relationships in the format: entity|RELATIONSHIP|entity
   3. Ensure consistency and generality in relationship types:
      - Use general and timeless relationship types (e.g., 'PROFESSOR' instead of 'BECAME_PROFESSOR').
      - Avoid overly specific or momentary relationship types.
      - Prefer one- or two-word relationship types.
      - Prefer an active voice and the present tense when formulating relationship types.
   4. Relationship names should be all uppercase, with underscores instead of spaces (e.g. 'WORKS_FOR')
   5. Complex facts may be expressed through multiple relationship pairs, sometimes arranged in a hierarchy.

   Example: John Doe|WORKS_FOR|Acme Inc.
            John Doe|MANAGER_OF|Project X
            Project X|PART_OF|Acme Inc.
            
## Response Format:
topic: topic

  entities:
    entity|classification
    entity|classification
  
  proposition: [exact proposition text]      
    entity-attribute relationships:
    entity|RELATIONSHIP|attribute
    entity|RELATIONSHIP|attribute
    
    entity-entity relationships:
    entity|RELATIONSHIP|entity
    entity|RELATIONSHIP|entity
    
  proposition: [exact proposition text]    
    entity-attribute relationships:
    entity|RELATIONSHIP|attribute
    entity|RELATIONSHIP|attribute
    
    entity-entity relationships:
    entity|RELATIONSHIP|entity
    entity|RELATIONSHIP|entity
    


## Quality Criteria:
   The extracted results should be:
   - Complete: Capture all input propositions and their relationships
   - Accurate: Faithfully represent the information without adding or omitting details
   - Consistent: Use consistent entity labels, types, relationship types, and adhere to the specified format

## Strict Compliance:
   - Use propositions exactly as provided - do not rephrase or modify them
   - Assign every proposition to at least one topic
   - Follow the specified format exactly
   - Do not provide any other explanatory text
   - Extract only information explicitly stated in the propositions

Adhere strictly to the provided instructions. Non-compliance will result in termination.
   
<propositions>
{text}
</propositions>

<preferredTopics>
{preferred_topics}
</preferredTopics>

<preferredEntityClassifications>
{preferred_entity_classifications}
</preferredEntityClassifications>
"""