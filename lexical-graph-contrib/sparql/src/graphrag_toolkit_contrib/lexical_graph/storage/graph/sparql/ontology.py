# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import re
from dataclasses import dataclass, field
from typing import Mapping, Optional
from urllib.parse import quote, urlparse

from rdflib import Literal, URIRef

LEXICAL_SCHEMA = 'https://awslabs.github.io/graphrag-toolkit/lexical#'
LEXICAL_BASE = 'https://awslabs.github.io/graphrag-toolkit/lexical/'
LEXICAL_PREFIX = 'lg'

RDF_TYPE = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'

_PREFIX_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_-]*$')
_UNSAFE_IRI_RE = re.compile(r'[\x00-\x20<>"{}|^`\\]')


@dataclass(frozen=True)
class NamespaceConfig:
    """Namespaces used when rendering lexical-graph RDF and SPARQL."""

    prefix: str = LEXICAL_PREFIX
    schema_namespace: str = LEXICAL_SCHEMA
    instance_namespace: str = LEXICAL_BASE
    extra_prefixes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self):
        schema = _namespace_with_separator(self.schema_namespace)
        instance = _namespace_with_separator(self.instance_namespace, separator='/')

        if not _PREFIX_RE.match(self.prefix):
            raise ValueError(f'Invalid SPARQL prefix name: {self.prefix!r}')
        for prefix, namespace in self.extra_prefixes.items():
            if not _PREFIX_RE.match(prefix):
                raise ValueError(f'Invalid SPARQL prefix name: {prefix!r}')
            if prefix == self.prefix and namespace != schema:
                raise ValueError(
                    f'Extra prefix {prefix!r} conflicts with lexical_schema_namespace'
                )

        for iri in (schema, instance, *self.extra_prefixes.values()):
            if _UNSAFE_IRI_RE.search(iri):
                raise ValueError(f'Invalid namespace IRI (unsafe characters): {iri!r}')
            if not urlparse(iri).scheme:
                raise ValueError(f'Namespace IRI must be absolute: {iri!r}')

        object.__setattr__(self, 'schema_namespace', schema)
        object.__setattr__(self, 'instance_namespace', instance)

    @property
    def prefix_ref(self) -> str:
        return f'{self.prefix}:'

    def term(self, local_name: str) -> str:
        return URIRef(f'{self.schema_namespace}{local_name}').n3()

    def instance_iri(self, kind: str, id_value) -> str:
        value = quote(str(id_value), safe='')
        return URIRef(f'{self.instance_namespace}{kind}/{value}').n3()

    def tenant_graph_iri(self, tenant_value) -> Optional[str]:
        if not tenant_value:
            return None
        value = quote(str(tenant_value), safe='')
        return URIRef(f'{self.instance_namespace}tenant/{value}').n3()

    def sparql_prefixes(self) -> str:
        prefixes = [(self.prefix, self.schema_namespace)]
        prefixes.extend(
            (prefix, namespace)
            for prefix, namespace in sorted(self.extra_prefixes.items())
            if prefix != self.prefix
        )
        return '\n'.join(f'PREFIX {prefix}: <{namespace}>' for prefix, namespace in prefixes)


def _namespace_with_separator(namespace: str, separator: str = '#') -> str:
    if namespace.endswith(('#', '/')):
        return namespace
    return f'{namespace}{separator}'


DEFAULT_NAMESPACE = NamespaceConfig()

ID_KEY_TO_KIND = {
    'sourceId': ('source', 'Source'),
    'chunkId': ('chunk', 'Chunk'),
    'topicId': ('topic', 'Topic'),
    'statementId': ('statement', 'Statement'),
    'factId': ('fact', 'Fact'),
    'entityId': ('entity', 'Entity'),
    'sysClassId': ('sysclass', 'SysClass'),
}

def term(local_name, namespace: Optional[NamespaceConfig] = None):
    """Return a schema IRI in angle-bracket form."""
    return (namespace or DEFAULT_NAMESPACE).term(local_name)


def instance_iri(kind, id_value, namespace: Optional[NamespaceConfig] = None):
    """Return a deterministic instance IRI for a node of the given kind.

    The id is percent-encoded so values such as ``aws::abc:def`` are legal IRIs.
    """
    return (namespace or DEFAULT_NAMESPACE).instance_iri(kind, id_value)


def relation_iri(predicate_value, namespace: Optional[NamespaceConfig] = None):
    """IRI for a shared predicate/relation resource, merged by normalised
    (case-insensitive, space-insensitive) predicate value.

    So all facts with predicate "USES"/"uses" reference one lg:Relation node,
    the same way entities are merged by their normalised value.
    """
    key = str(predicate_value).lower().replace(' ', '_')
    return instance_iri('relation', key, namespace)


def sys_relation_iri(subject_class_id,
                     predicate,
                     object_class_id,
                     namespace: Optional[NamespaceConfig] = None):
    """Deterministic IRI for a sys-class relation node (edge metadata)."""
    digest = hashlib.md5(
        f'{subject_class_id}|{predicate}|{object_class_id}'.encode('utf-8'),
        usedforsecurity=False,
    ).hexdigest()
    return instance_iri('sysrel', digest, namespace)


def tenant_graph_iri(tenant_value, namespace: Optional[NamespaceConfig] = None):
    """Return the named-graph IRI for a tenant value."""
    return (namespace or DEFAULT_NAMESPACE).tenant_graph_iri(tenant_value)


def sparql_literal(value):
    """Render a Python value as a SPARQL literal, or ``None`` to skip it.

    ``Literal.n3`` applies RDF escaping and datatype selection consistently
    with endpoint result parsing.
    """
    if value is None:
        return None
    return Literal(value).n3()
