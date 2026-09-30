# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class EntityTypeConfig:
    """Configuration for an entity type in the extraction schema.

    Attributes:
        description: Human-readable description of this entity type.
        attributes: Expected attribute names for this entity type.
        aliases: Alternative names that should map to this entity type.
    """
    description: Optional[str] = None
    attributes: List[str] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)


@dataclass
class ExtractionSchema:
    """Schema defining allowed entity types, relationship types, and constraints.

    When provided to the extraction pipeline, the schema:
    - Feeds entity type names into the extraction prompt as preferred classifications
    - Configures NER stage labels (when using NERExtractionStage)
    - Filters non-matching entities/relationships when strict=True

    Attributes:
        entity_types: Mapping of entity type name to its configuration.
        relationship_types: Allowed relationship type names.
        strict: When True, filter out entities and relationships not in the schema.
    """
    entity_types: Dict[str, EntityTypeConfig] = field(default_factory=dict)
    relationship_types: List[str] = field(default_factory=list)
    strict: bool = False

    def entity_type_names(self) -> List[str]:
        """Return sorted list of entity type names."""
        return sorted(self.entity_types.keys())

    def format_as_prompt_constraint(self) -> str:
        """Format schema as a text block for injection into extraction prompts."""
        lines = []
        if self.entity_types:
            lines.append('Entity types:')
            for name, config in sorted(self.entity_types.items()):
                desc = f' - {config.description}' if config.description else ''
                lines.append(f'  {name}{desc}')
                if config.attributes:
                    lines.append(f'    Attributes: {", ".join(config.attributes)}')
        if self.relationship_types:
            lines.append('Relationship types:')
            for rt in sorted(self.relationship_types):
                lines.append(f'  {rt}')
        if self.strict:
            lines.append('STRICT MODE: Only extract entities and relationships matching the types listed above.')
        return '\n'.join(lines)
