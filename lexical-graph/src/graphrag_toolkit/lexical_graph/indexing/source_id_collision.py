# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, Iterable, List, Optional, Tuple

from llama_index.core.schema import NodeRelationship

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.model import SourceType, SourceDocument
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce

logger = logging.getLogger(__name__)

# Property on the __Source__ node holding the document's hash. The value is the
# hash llama_index computes over a document's text and metadata, which every
# chunk carries on its SOURCE relationship.
DOCUMENT_HASH_PROPERTY = 'documentHash'


class SourceIdCollisionError(ValueError):
    """Raised when two different documents resolve to one source id."""


def _name(metadata:Optional[Dict]) -> str:
    return str((metadata or {}).get('file_path') or 'a document')


class SourceIdCollisionGuard:
    """
    Stops a build when two different documents claim one source id.

    A document is identified by the hash on its chunks' SOURCE relationship. A
    collision is one source id seen with two hashes: within one document (a
    storage prefix that merged two documents), within one run, or against the
    hash already recorded on the graph's __Source__ node. A source written before
    the hash was recorded carries none and is accepted; the build records it.

    Graph lookups run once per ``lookup_batch_size`` documents, so documents are
    yielded in groups of that size.
    """

    def __init__(self, graph_store:GraphStore, tenant_id:TenantId, lookup_batch_size:Optional[int]=None):
        self.graph_store = graph_store
        self.tenant_id = tenant_id
        self.lookup_batch_size = coalesce(lookup_batch_size, GraphRAGConfig.build_batch_size)
        self._seen:Dict[str, Tuple[str, Optional[Dict]]] = {}
        self._checked_against_graph:set = set()

    @staticmethod
    def _sources(item:SourceType) -> List[Tuple[str, Optional[str], Optional[Dict]]]:
        """(source id, hash, source metadata) for each chunk in the input."""
        nodes = item.nodes if isinstance(item, SourceDocument) else [item]
        found = []
        for node in nodes:
            source = node.relationships.get(NodeRelationship.SOURCE)
            if source:
                found.append((source.node_id, source.hash, source.metadata))
        return found

    def _claim(self, item:SourceType) -> Optional[str]:
        """Records the document's claim on its source id. Returns the id when the
        graph still has to be consulted for it."""
        sources = self._sources(item)
        if not sources:
            return None

        source_id = sources[0][0]
        hashes = {h for _, h, _ in sources if h}
        if len(hashes) > 1:
            raise SourceIdCollisionError(
                f'Two different documents share source id {source_id}: chunks in one '
                f'document carry hashes {" and ".join(sorted(hashes))}. The documents '
                f'were merged in storage before the build.'
            )
        if not hashes:
            logger.debug(f'Source carries no document hash, so it is not checked for a collision [source_id: {source_id}]')
            return None

        document_hash = hashes.pop()
        metadata = sources[0][2]
        earlier = self._seen.get(source_id)
        if earlier and earlier[0] != document_hash:
            raise SourceIdCollisionError(
                f'Two different documents share source id {source_id}: '
                f'{_name(earlier[1])} (hash {earlier[0]}) and {_name(metadata)} '
                f'(hash {document_hash}).'
            )
        self._seen[source_id] = (document_hash, metadata)

        return None if source_id in self._checked_against_graph else source_id

    def _check_graph(self, source_ids:List[str]) -> None:
        if not source_ids:
            return
        rows = self.graph_store.execute_query(
            f'MATCH (s:`__Source__`) WHERE {self.graph_store.node_id("s.sourceId")} IN $sourceIds '
            f'RETURN {self.graph_store.node_id("s.sourceId")} AS sourceId, '
            f's.{DOCUMENT_HASH_PROPERTY} AS {DOCUMENT_HASH_PROPERTY}, s.file_path AS filePath',
            {'sourceIds': source_ids},
        )
        for row in rows:
            stored = row.get(DOCUMENT_HASH_PROPERTY)
            incoming = self._seen.get(row['sourceId'])
            if stored and incoming and stored != incoming[0]:
                raise SourceIdCollisionError(
                    f'Two different documents share source id {row["sourceId"]}: '
                    f'{_name(incoming[1])} (hash {incoming[0]}) and '
                    f'{_name({"file_path": row.get("filePath")})} already in the graph (hash {stored}).'
                )
        self._checked_against_graph.update(source_ids)

    def _check(self, inputs:Iterable[SourceType]):
        pending:List[SourceType] = []
        to_look_up:List[str] = []

        def flush():
            self._check_graph(list(to_look_up))
            to_look_up.clear()
            yield from list(pending)
            pending.clear()

        for item in inputs:
            source_id = self._claim(item)
            if source_id:
                to_look_up.append(source_id)
            pending.append(item)
            if len(pending) >= self.lookup_batch_size:
                yield from flush()

        yield from flush()

    def __call__(self, inputs:Iterable[SourceType]):
        """
        Yields the inputs unchanged. A sized input comes back as a list so the
        build pipeline can still report batch totals.
        """
        checked = self._check(inputs)
        return list(checked) if hasattr(inputs, '__len__') else checked
