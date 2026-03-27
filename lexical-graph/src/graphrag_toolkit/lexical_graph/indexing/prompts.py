# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

EXTRACT_PROPOSITIONS_PROMPT = """
You are a top-tier algorithm designed for extracting information in structured formats to build a knowledge graph. Your task is to decompose the given text into clear, concise, and context-independent propositions.

# Instructions:
1. Break down complex sentences into simple, atomic statements.
2. Break down lists and tables of information into sets of simple statements.
3. Preserve original phrasing from the input text whenever possible.
4. Add a proposition per named entity that classifies that entity.
5. Maintain named entity consistency by resolving coreferences across all propositions to the most complete identifier for that entity.
6. If in doubt about an entity's identifier, use the additional source information provided to attempt to resolve the entity to its most complete identifier.
   - If you cannot resolve an identity to its most complete identifier, leave it as-is
   - You MUST NOT use your training history to identify or name entities
7. Isolate descriptive information about named entities into separate propositions.
8. Decontextualize each proposition by:
   a) Adding necessary modifiers to nouns or entire propositions.
   b) Replace any pronouns (e.g., he, she, it, they) with the specific nouns they refer to.  
   c) Replace any acronyms with their full forms.
   d) If the proposition is a fragment, use the other sentences to reconstruct it into a complete, self-explanatory proposition.
   e) Preserve any quoted speech or dialogue as is, without paraphrasing.
   f) If a proposition is already self-explanatory, leave it unchanged.
   g) Do not introduce irrelevant context information in the proposition.
   h) Ensure each proposition can stand alone without external context. 
   i) Do not use your training history, rely only on the context information to enhance the proposition.
9. Capture all relevant details from the original text, including temporal, spatial, and causal relationships.
10. Prioritize completeness and accuracy.

# Output Format:
- Preface the list with a concise, specific title.
- Present each proposition on a new line.
- Ensure each proposition is unique: do not repeat propositions.
- Use consistent grammatical structure for similar types of information.
- Do not include any explanatory text or numbering.
- Ensure propositions are in a logical order, if applicable.
- Ensure each proposition can be verified against the original text, and does not introduce external information.

# Response Format:
title
proposition
proposition
proposition
...

Do not provide any other explanatory text. Ensure you have captured all of the details from the text in your response.  

<sourceInformation>
{source_info}
</sourceInformation>

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
   1. Read the entire set of propositions and then extract a list of specific topics. Topic names should provide a clear, highly descriptive summary of the content: look for propositions that can act as topic names, and use these verbatim if possible. 
      - Compare each topic with the list of Preferred Topics. If a preferred topic matches the new topic in its meaning and specificity, use the preferred topic. Otherwise, use the new topic. 
   2. Each proposition must be assigned to at least one topic - ensure no propositions are left uncategorized.
   3. For each topic, perform the following Entity Extraction and Proposition Organization tasks.

## Entity Extraction:
   1. Extract a list of all named entities mentioned in the propositions within each topic.
   2. DO NOT treat numerical values, dates, times, measurements, or object attributes (e.g. size, colour) as entities.
   3. Classify each entity. A list of Preferred Entity Classifications is included below. Choose the most specific classification from this list in preference to creating a new classification.
   4. Ensure consistency in identifying entities:
      - Always use the most complete identifier for an entity (e.g., 'John Doe' instead of 'he' or 'John').
      - Avoid using articles, such as 'A', An' or 'The', at the beginning of identifiers.
      - Maintain entity consistency throughout the knowledge graph by resolving coreferences.
      - If an entity is referred to by different names or pronouns, always use the most complete identifier.
      - If the identifier is an acronym, and you recognize the acronym, use the entity's full name instead of the acronym. DO NOT put the acronym in parentheses after the full name. 
   5. Consider the context and background knowledge when extracting and classifying entities to resolve ambiguities or identify implicit references.
   6. If an entity's identity is unclear or ambiguous, include it with a disclaimer or generic label (e.g., 'unknown_person').
      
## Proposition Organization:
   1. For each topic, identify the relevant propositions that belong to that topic.
   2. Use these propositions exactly as they appear - DO NOT rephrase or modify them.
   3. For each proposition, perform the following Relationship Extraction and Attribute Extraction tasks.
   
## Relationship Extraction:
   1. Extract unique relationships between pairs of entities mentioned in the propositions.
   2. Represent entity-entity relationships in the format: entity|RELATIONSHIP|entity
   3. Ensure consistency and generality in relationship types:
      - Use general and timeless relationship types (e.g., 'PROFESSOR' instead of 'BECAME_PROFESSOR').
      - Avoid overly specific or momentary relationship types.
      - Use one- or two-word relationship types.
   4. Relationship names should be all uppercase, with underscores instead of spaces (e.g. 'WORKS_FOR')
   
   Example: John Doe|WORKS_FOR|Acme Inc.
            John Doe|MANAGER_OF|Project X
            Project X|PART_OF|Acme Inc.

## Attribute Extraction:
   1. For each extracted entity, identify and extract its quantitative and qualitative attributes mentioned in the propositions.
      - Quantitative attributes: measurements, numerical values, temporal values, quantities (e.g., age, height, weight, size, date, time).
      - Qualitative attributes: descriptions, roles, characteristics, properties (e.g., color, occupation, nationality, season).
   2. Represent entity attributes in the format: entity|ATTRIBUTE_NAME|attribute
   3. Ensure consistency and generality in naming attributes:
      - Use general and timeless attribute names (e.g., 'HAS_VALUE' instead of 'HAD_VALUE').
      - Avoid overly specific or momentary attribute names.
      - Use one- or two-word attribute names. Indicate belonging or ownership (e.g., 'HAS_NAME' instead of 'NAME').
   4. Attribute names should be all uppercase, with underscores instead of spaces (e.g. 'DESCRIBED_BY')

   Example: John Doe|OCCUPATION|software engineer


            
## Response Format:
topic: topic

  entities:
  
    entity|label
    entity|label
  
  proposition: [exact proposition text]  
    
    entity-entity relationships:
    entity|RELATIONSHIP|entity
    entity|RELATIONSHIP|entity
    
    entity-attributes:
    entity|ATTRIBUTE_NAME|value
    entity|ATTRIBUTE_NAME|value
      
  proposition: [exact proposition text] 
  
    entity-entity relationships:
    entity|RELATIONSHIP|entity
    entity|RELATIONSHIP|entity
  
    entity-attributes:
    entity|ATTRIBUTE_NAME|value
    entity|ATTRIBUTE_NAME|value
    


## Quality Criteria:
   The extracted results should be:
   - Complete: Capture all input propositions and their relationships
   - Accurate: Faithfully represent the information without adding or omitting details
   - Consistent: Use consistent entity labels, relationship types, and attribute names, and adhere to the specified format

## Strict Compliance:
   - Use propositions exactly as provided - do not rephrase or modify them
   - Assign every proposition to at least one topic
   - Follow the specified format exactly
   - Do not provide any other explanatory text
   - Extract only information explicitly stated in the propositions

{schema_constraints}

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

DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT = """
You are an expert system analyzing text to identify domain-specific entity types. Based on the provided text samples and list of existing classifications, identify the 5 most significant entity classifications for this domain.

Guidelines:
1. Identify the most important specific types of entities that appear in or are relevant to the domain
2. Reuse existing classifications where possible
3. Focus on concrete entities, not abstract concepts
4. DO NOT treat numerical values, dates, times, measurements, or object attributes (e.g. size, colour) as entities
5. Use clear, concise classification names (1-2 words)
6. Use the singular, not plural, form for each classification
7. Aim for 5 classifications
8. Format each classification as a single word, or multiple words separated by a space
9. Do not use underscores or any other punctuation

Sample text chunks:
<chunks>
{text_chunks}
</chunks>

Existing classifications:
<existing_classifications>
{existing_classifications}
</existing_classifications>

Output the classifications between entity_classifications tags, one per line.

Expected format:
<entity_classifications>
Classification1
Classification2
Classification3
</entity_classifications>
"""

RANK_ENTITY_CLASSIFICATIONS_PROMPT = """
You are an expert system analyzing text to rank domain-specific entity types. Order the list of entity classifications below, most important classifications first.

Guidelines:
1. If the classifications cover multiple domains, choose high-level classifications across domains, rather than going deep into a single domain
2. Eliminate synonyms
3. Convert plural forms to the singluar form, if necessary
4. If you can identify a broader classification that covers several classifications, use the broader classification

Original classifications:
<classifications>
{classifications}
</classifications>

Output the ranked classifications between entity_classifications tags, one per line.

Expected format:
<entity_classifications>
Classification1
Classification2
Classification3
</entity_classifications>
"""

EXTRACT_TOPICS_JSON_PROMPT = """
You are a top-tier algorithm designed for extracting information in structured formats to build a knowledge graph.
Your input consists of carefully crafted propositions - simple, atomic, and decontextualized statements. Your task is to:
   1. Organize these propositions into topics
   2. Extract entities and their attributes
   3. Identify relationships between entities

Try to capture as much information from the text as possible without sacrificing accuracy. Do not add any information that is not explicitly mentioned in the input propositions.

## Topic Extraction:
   1. Read the entire set of propositions and then extract a list of specific topics. Topic names should provide a clear, highly descriptive summary of the content.
      - Compare each topic with the list of Preferred Topics. If a preferred topic matches the new topic in its meaning and specificity, use the preferred topic.
   2. Each proposition must be assigned to at least one topic.

## Entity Extraction:
   1. Extract all named entities mentioned in the propositions within each topic.
   2. DO NOT treat numerical values, dates, times, measurements, or object attributes as entities.
   3. Classify each entity using the Preferred Entity Classifications when possible.
   4. Use the most complete identifier for each entity. Avoid articles at the beginning.

## Relationship and Attribute Extraction:
   1. Extract relationships between entity pairs as: subject, predicate, object.
   2. Extract entity attributes as: subject, predicate (attribute name), complement (attribute value).
   3. Use general, timeless, uppercase relationship/attribute names with underscores.

{schema_constraints}

## Response Format:
Respond with a JSON object matching this exact schema:
```json
{{
  "topics": [
    {{
      "value": "topic name",
      "entities": [
        {{"value": "entity name", "classification": "EntityType"}}
      ],
      "statements": [
        {{
          "value": "exact proposition text",
          "facts": [
            {{
              "subject": {{"value": "entity name", "classification": "EntityType"}},
              "predicate": {{"value": "RELATIONSHIP_NAME"}},
              "object": {{"value": "entity name", "classification": "EntityType"}}
            }}
          ]
        }}
      ]
    }}
  ]
}}
```

## Strict Compliance:
   - Use propositions exactly as provided
   - Assign every proposition to at least one topic
   - Respond ONLY with valid JSON, no other text
   - Extract only information explicitly stated in the propositions

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
